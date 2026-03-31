from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta
from app.routes.agent_bonuses import get_sales_multiplier

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/all-ftcs", response_class=HTMLResponse)
async def all_ftcs_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance", status_code=302)
    return templates.TemplateResponse("all_ftcs.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/all-ftcs")
async def all_ftcs_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if user.get("role") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    _ck = f"all_ftcs_v3:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
        datetime.strptime(date_from, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    sql = """
        WITH
        ftc_accs AS (
            SELECT a.accountid,
                   COALESCE(td.original_deposit_owner, a.assigned_to) AS agent_id,
                   1 AS is_ftc
            FROM accounts a
            LEFT JOIN (
                SELECT DISTINCT ON (vtigeraccountid)
                       vtigeraccountid, original_deposit_owner
                FROM transactions
                WHERE ftd = 1
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
                ORDER BY vtigeraccountid, confirmation_time ASC
            ) td ON td.vtigeraccountid = a.accountid
            WHERE a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND a.is_test_account = 0
              AND a.accountid IS NOT NULL
        ),
        ftd100_accs AS (
            SELECT f.accountid,
                   f.original_deposit_owner                                     AS agent_id,
                   1                                                             AS is_ftd100,
                   CASE WHEN f.ftd_100_amount >= 240 THEN 'full' ELSE 'half' END AS ftd100_type,
                   CASE WHEN f.ftd_100_amount < 500  THEN 0
                        WHEN f.ftd_100_amount < 1000 THEN 10
                        WHEN f.ftd_100_amount < 5000 THEN 20
                        ELSE 50 END::float                                      AS ftd_amount_bonus_raw
            FROM ftd100_clients f
            WHERE f.ftd_100_date >= %(date_from)s
              AND f.ftd_100_date <  %(date_to_excl)s
              AND f.original_deposit_owner IS NOT NULL
        ),
        combined AS (
            SELECT COALESCE(f.accountid, t.accountid)  AS accountid,
                   COALESCE(f.agent_id,  t.agent_id)   AS agent_id,
                   COALESCE(t.is_ftc,    0)             AS is_ftc,
                   COALESCE(f.is_ftd100, 0)             AS is_ftd100,
                   f.ftd100_type,
                   COALESCE(f.ftd_amount_bonus_raw, 0)  AS ftd_amount_bonus_raw
            FROM ftd100_accs f
            FULL OUTER JOIN ftc_accs t ON t.accountid = f.accountid
        ),
        ftc_txns AS (
            SELECT t.vtigeraccountid AS accountid,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled')
                            THEN t.usdamount ELSE 0 END)::float AS ftc_deposit,
                   SUM(CASE WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')
                            THEN t.usdamount ELSE 0 END)::float AS ftc_wd
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND (a.client_qualification_date >= t.confirmation_time::date OR t.ftd = 1)
              AND a.is_test_account = 0
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            GROUP BY t.vtigeraccountid
        ),
        agent_totals AS (
            SELECT bon.agent_id,
                   SUM(bon.ftd100_count)::int       AS ftd100_total,
                   COALESCE(tgt.target_ftc, 0)::int AS target_ftc
            FROM (
                SELECT agent_id, SUM(ftd100_count) AS ftd100_count
                FROM mv_sales_bonuses
                WHERE ftd_100_date >= %(date_from)s AND ftd_100_date < %(date_to_excl)s
                GROUP BY agent_id
            ) bon
            LEFT JOIN (
                SELECT agent_id::bigint, SUM(ftc)::int AS target_ftc
                FROM targets
                WHERE date >= %(date_from)s AND date < %(date_to_excl)s
                GROUP BY agent_id
            ) tgt ON tgt.agent_id = bon.agent_id
            GROUP BY bon.agent_id, tgt.target_ftc
        )
        SELECT
            COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
            COALESCE(u.desk, '')                          AS desk,
            COALESCE(u.office_name, '')                   AS office_name,
            COALESCE(u.position, '')                      AS position,
            c.accountid,
            COALESCE(ft.ftc_deposit, 0)::float            AS ftc_deposit,
            COALESCE(ft.ftc_wd, 0)::float                 AS ftc_wd,
            COALESCE(c.ftd100_type, '')                   AS ftd100_type,
            c.ftd_amount_bonus_raw::float                 AS ftd_amount_bonus_raw,
            COALESCE(c.is_ftd100, 0)::int                 AS is_ftd100,
            COALESCE(at.ftd100_total, 0)::int             AS ftd100_total,
            COALESCE(at.target_ftc, 0)::int               AS target_ftc
        FROM combined c
        LEFT JOIN crm_users u ON u.id = c.agent_id
        LEFT JOIN ftc_txns ft ON ft.accountid = c.accountid
        LEFT JOIN agent_totals at ON at.agent_id = c.agent_id
        WHERE c.is_ftc = 1
          AND (u.id IS NULL
               OR (TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                   AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'))
        ORDER BY COALESCE(u.agent_name, u.full_name, 'N/A'), c.accountid
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            rows = cur.fetchall()

        data = []
        for r in rows:
            agent_name           = r[0]
            desk                 = r[1]
            office_name          = r[2]
            position             = r[3]
            accountid            = r[4]
            ftc_deposit          = round(float(r[5]), 2)
            ftc_wd               = round(float(r[6]), 2)
            ftd100_type          = r[7]
            ftd_amount_bonus_raw = round(float(r[8]), 2)
            is_ftd100            = int(r[9])
            ftd100_total         = int(r[10])
            target_ftc           = int(r[11])

            qualify    = target_ftc > 0 and ftd100_total >= 0.50 * target_ftc
            multiplier = get_sales_multiplier(ftd100_total)

            if is_ftd100 and qualify:
                basic_bonus      = multiplier if ftd100_type == 'full' else round(multiplier / 2, 2)
                ftd_amount_bonus = ftd_amount_bonus_raw
            else:
                basic_bonus      = 0
                ftd_amount_bonus = 0

            data.append({
                "agent_name":       agent_name,
                "desk":             desk,
                "office_name":      office_name,
                "position":         position,
                "accountid":        accountid,
                "ftc":              1,
                "ftc_deposit":      ftc_deposit,
                "ftc_wd":           ftc_wd,
                "basic_bonus":      basic_bonus,
                "ftd_amount_bonus": ftd_amount_bonus,
            })

        _result = {"rows": data}
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
