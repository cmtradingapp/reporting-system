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

    _ck = f"all_ftcs_v9:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    _TARGETS_CUTOFF = datetime.strptime("2026-04-01", "%Y-%m-%d").date()

    sql = """
        WITH
        ftc_accounts AS (
            -- All accounts that qualified as FTCs in the date range
            SELECT a.accountid, a.assigned_to, a.client_qualification_date
            FROM accounts a
            WHERE a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND a.is_test_account = 0
              AND a.accountid IS NOT NULL
        ),
        ftd_info AS (
            -- The agent who gets FTC=1 credit (original_deposit_owner of the FTD transaction)
            SELECT fa.accountid,
                   COALESCE(td.ftd_agent_id, fa.assigned_to) AS ftd_agent_id
            FROM ftc_accounts fa
            LEFT JOIN (
                SELECT DISTINCT ON (t.vtigeraccountid)
                       t.vtigeraccountid                                         AS accountid,
                       COALESCE(t.original_deposit_owner, a2.assigned_to)        AS ftd_agent_id
                FROM transactions t
                JOIN accounts a2 ON a2.accountid = t.vtigeraccountid
                WHERE t.ftd = 1
                  AND t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                ORDER BY t.vtigeraccountid, t.confirmation_time ASC
            ) td ON td.accountid = fa.accountid
        ),
        ftd100_accs AS (
            SELECT f.accountid,
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
        per_agent_deps AS (
            -- Sum deposits/WDs per (account, agent) using original_deposit_owner on each transaction.
            -- NULL original_deposit_owner rows are excluded here; the current assigned_to gets
            -- a $0 row via the UNION in all_account_agents if they have no transactions.
            SELECT t.vtigeraccountid                                              AS accountid,
                   t.original_deposit_owner                                       AS agent_id,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled')
                            THEN t.usdamount ELSE 0 END)::float                  AS ftc_deposit,
                   SUM(CASE WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')
                            THEN t.usdamount ELSE 0 END)::float                  AS ftc_wd
            FROM transactions t
            JOIN ftc_accounts fa ON fa.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.original_deposit_owner IS NOT NULL
              AND (t.confirmation_time::date <= fa.client_qualification_date OR t.ftd = 1)
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            GROUP BY t.vtigeraccountid, t.original_deposit_owner
        ),
        all_account_agents AS (
            -- One row per (account, agent): from transactions + current assigned_to with $0 if missing
            SELECT accountid, agent_id, ftc_deposit, ftc_wd
            FROM per_agent_deps

            UNION

            SELECT fa.accountid, fa.assigned_to AS agent_id, 0::float, 0::float
            FROM ftc_accounts fa
            LEFT JOIN per_agent_deps pad
                   ON pad.accountid = fa.accountid AND pad.agent_id = fa.assigned_to
            WHERE fa.assigned_to IS NOT NULL
              AND pad.agent_id IS NULL
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
            LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = bon.agent_id
            GROUP BY bon.agent_id, tgt.target_ftc
        )
        SELECT
            COALESCE(u.agent_name, u.full_name, 'N/A')                           AS agent_name,
            COALESCE(u.desk, '')                                                  AS desk,
            COALESCE(u.office_name, '')                                           AS office_name,
            COALESCE(u.position, '')                                              AS position,
            aaa.accountid,
            aaa.ftc_deposit::float                                                AS ftc_deposit,
            aaa.ftc_wd::float                                                     AS ftc_wd,
            CASE WHEN aaa.agent_id = fi.ftd_agent_id
                 THEN COALESCE(f.ftd100_type, '') ELSE '' END                     AS ftd100_type,
            CASE WHEN aaa.agent_id = fi.ftd_agent_id
                 THEN COALESCE(f.ftd_amount_bonus_raw, 0) ELSE 0
                 END::float                                                       AS ftd_amount_bonus_raw,
            CASE WHEN aaa.agent_id = fi.ftd_agent_id
                 THEN COALESCE(f.is_ftd100, 0) ELSE 0 END::int                   AS is_ftd100,
            COALESCE(at.ftd100_total, 0)::int                                    AS ftd100_total,
            COALESCE(at.target_ftc, 0)::int                                      AS target_ftc,
            CASE WHEN aaa.agent_id = fi.ftd_agent_id THEN 1 ELSE 0 END::int      AS is_ftc
        FROM all_account_agents aaa
        JOIN ftd_info fi ON fi.accountid = aaa.accountid
        LEFT JOIN ftd100_accs f ON f.accountid = aaa.accountid
        LEFT JOIN crm_users u ON u.id = aaa.agent_id
        LEFT JOIN agent_totals at ON at.agent_id = aaa.agent_id
        WHERE (u.id IS NULL
               OR (TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                   AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'))
        ORDER BY COALESCE(u.agent_name, u.full_name, 'N/A'), aaa.accountid
    """

    if dt_from >= _TARGETS_CUTOFF:
        _tgt_subq = """SELECT crm_user_id AS agent_id, monthly_ftd100_target AS target_ftc
            FROM agent_targets_history
            WHERE report_month = DATE_TRUNC('month', %(date_from)s::date)
              AND crm_user_id IS NOT NULL"""
    else:
        _tgt_subq = """SELECT agent_id::int AS agent_id, ftc::int AS target_ftc
            FROM targets
            WHERE date = DATE_TRUNC('month', %(date_from)s::date)"""
    sql = sql.replace('{tgt_subq}', _tgt_subq)

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
            is_ftc               = int(r[12])

            qualify    = target_ftc > 0 and ftd100_total >= 0.50 * target_ftc
            multiplier = get_sales_multiplier(ftd100_total)

            if is_ftd100 and qualify and is_ftc == 1:
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
                "ftc":              is_ftc,
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
