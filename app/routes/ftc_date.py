from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import date, datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/ftc-date", response_class=HTMLResponse)
async def ftc_date_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("ftc_date.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/ftc-date/options")
async def ftc_date_options(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    sql = """
        SELECT DISTINCT u.id, u.agent_name, u.office_name, u.department
        FROM crm_users u
        WHERE u.id IN (SELECT DISTINCT assigned_to FROM accounts WHERE assigned_to IS NOT NULL)
          AND u.agent_name IS NOT NULL
        ORDER BY u.agent_name
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        agents = [{"id": r[0], "name": r[1]} for r in rows]
        offices = sorted(set(r[2] for r in rows if r[2]))
        teams = sorted(set(r[3] for r in rows if r[3]))
        return JSONResponse(content={"agents": agents, "offices": offices, "teams": teams})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/ftc-date")
async def ftc_date_api(
    request: Request,
    end_date: str = None,
    agent_id: int = None,
    office: str = None,
    team: str = None,
    groups: str = None,
    classification: str = None,
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    if not end_date:
        end_date = datetime.now(_TZ).date().strftime("%Y-%m-%d")
    _ck = f"ftc_v2:{end_date}:{agent_id}:{office}:{team}:{groups}:{classification}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    params = {"end_date": end_date}
    agent_clause = office_clause = team_clause = classification_clause = ""
    if agent_id:
        agent_clause = "AND u.id = %(agent_id)s"
        params["agent_id"] = agent_id
    if office:
        office_clause = "AND u.office_name = %(office)s"
        params["office"] = office
    if team:
        team_clause = "AND u.department = %(team)s"
        params["team"] = team
    if classification == "Low Quality":
        classification_clause = "AND a.classification_int BETWEEN 1 AND 5"
    elif classification == "High Quality":
        classification_clause = "AND a.classification_int BETWEEN 6 AND 10"
    elif classification == "No segmentation":
        classification_clause = "AND (a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)"

    sql = """
        WITH ftc_groups AS (
            SELECT
                a.accountid,
                a.client_qualification_date::date AS qual_date,
                (%(end_date)s::date - a.client_qualification_date::date) AS days_diff
            FROM accounts a
            LEFT JOIN crm_users u ON u.id = a.assigned_to
            LEFT JOIN client_classification cc ON cc.accountid = a.accountid::BIGINT
            WHERE a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date::date >= '2024-01-01'
              AND a.client_qualification_date::date <= %(end_date)s::date
              AND a.is_test_account = 0
              {agent_clause}
              {office_clause}
              {team_clause}
              {classification_clause}
        ),
        tx_per_account AS (
            SELECT
                t.vtigeraccountid AS accountid,
                SUM(CASE WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN t.usdamount ELSE 0 END) AS deposit_usd,
                SUM(CASE WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN t.usdamount ELSE 0 END) AS withdrawal_usd
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
              AND COALESCE(t.confirmation_time, t.created_time)::date >= '2024-01-01'
            GROUP BY t.vtigeraccountid
        ),
        rdp AS (
            SELECT DISTINCT t.vtigeraccountid AS accountid
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactiontype = 'Deposit'
              AND t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND a.client_qualification_date IS NOT NULL
              AND COALESCE(t.confirmation_time, t.created_time)::date > a.client_qualification_date::date
              AND COALESCE(t.confirmation_time, t.created_time)::date <= %(end_date)s::date
              AND a.is_test_account = 0
        ),
        withdrawalers AS (
            SELECT DISTINCT t.vtigeraccountid AS accountid
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactiontype = 'Withdrawal'
              AND t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND COALESCE(t.confirmation_time, t.created_time)::date <= %(end_date)s::date
              AND a.is_test_account = 0
        ),
        traders AS (
            SELECT DISTINCT ta.vtigeraccountid AS accountid
            FROM dealio_trades_mt4 d
            JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE d.notional_value > 0
              AND ta.vtigeraccountid IS NOT NULL
              AND ta.vtigeraccountid::text != ''
              AND d.open_time::date <= %(end_date)s::date
              AND a.is_test_account = 0
        ),
        grouped AS (
            SELECT
                CASE
                    WHEN fg.days_diff BETWEEN 0   AND 7   THEN 1
                    WHEN fg.days_diff BETWEEN 8   AND 14  THEN 2
                    WHEN fg.days_diff BETWEEN 15  AND 30  THEN 3
                    WHEN fg.days_diff BETWEEN 31  AND 60  THEN 4
                    WHEN fg.days_diff BETWEEN 61  AND 90  THEN 5
                    WHEN fg.days_diff BETWEEN 91  AND 120 THEN 6
                    WHEN fg.days_diff > 120               THEN 7
                END AS group_order,
                CASE
                    WHEN fg.days_diff BETWEEN 0   AND 7   THEN '0 - 7 days'
                    WHEN fg.days_diff BETWEEN 8   AND 14  THEN '8 - 14 days'
                    WHEN fg.days_diff BETWEEN 15  AND 30  THEN '15 - 30 days'
                    WHEN fg.days_diff BETWEEN 31  AND 60  THEN '31 - 60 days'
                    WHEN fg.days_diff BETWEEN 61  AND 90  THEN '61 - 90 days'
                    WHEN fg.days_diff BETWEEN 91  AND 120 THEN '91 - 120 days'
                    WHEN fg.days_diff > 120               THEN '120+ days'
                END AS day_group,
                fg.accountid,
                COALESCE(tx.deposit_usd,    0) AS deposit_usd,
                COALESCE(tx.withdrawal_usd, 0) AS withdrawal_usd,
                CASE WHEN rdp.accountid IS NOT NULL THEN 1 ELSE 0 END AS is_rdp,
                CASE WHEN wd.accountid  IS NOT NULL THEN 1 ELSE 0 END AS is_withdrawaler,
                CASE WHEN tr.accountid  IS NOT NULL THEN 1 ELSE 0 END AS is_trader
            FROM ftc_groups fg
            LEFT JOIN tx_per_account tx ON tx.accountid = fg.accountid
            LEFT JOIN rdp              ON rdp.accountid = fg.accountid
            LEFT JOIN withdrawalers wd ON wd.accountid  = fg.accountid
            LEFT JOIN traders tr       ON tr.accountid  = fg.accountid
        )
        SELECT
            group_order,
            day_group,
            COUNT(DISTINCT accountid)        AS ftc_count,
            SUM(is_rdp)                      AS rdp_count,
            COALESCE(SUM(deposit_usd),    0) AS deposit_usd,
            COALESCE(SUM(withdrawal_usd), 0) AS withdrawal_usd,
            SUM(is_withdrawaler)             AS wd_count,
            SUM(is_trader)                   AS trader_count
        FROM grouped
        GROUP BY group_order, day_group
        ORDER BY group_order
    """.format(
        agent_clause=agent_clause,
        office_clause=office_clause,
        team_clause=team_clause,
        classification_clause=classification_clause,
    )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        data = []
        for r in rows:
            group_order, day_group, ftc, rdp_cnt, dep, wd, wdcount, trader_count = r
            ftc = int(ftc or 0)
            rdp_cnt = int(rdp_cnt or 0)
            dep = float(dep or 0)
            wd = float(wd or 0)
            wdcount = int(wdcount or 0)
            traders = int(trader_count or 0)
            net_dep = dep - wd
            data.append({
                "day_group":      day_group,
                "ftc":            ftc,
                "rdp":            rdp_cnt,
                "deposit":        round(dep),
                "withdrawal":     round(wd),
                "net_deposit":    round(net_dep),
                "ltv":            round(net_dep / ftc, 2) if ftc > 0 else 0,
                "pct_std":        round(rdp_cnt / ftc * 100) if ftc > 0 else 0,
                "wd_count":       wdcount,
                "pct_wd_clients": round(wdcount / ftc * 100) if ftc > 0 else 0,
                "pct_wd_usd":     round(wd / dep * 100) if dep > 0 else 0,
                "traders":        traders,
                "traders_pct":    round(traders / ftc * 100) if ftc > 0 else 0,
            })

        if groups:
            allowed = set(groups.split(','))
            data = [r for r in data if r['day_group'] in allowed]

        total_ftc = total_rdp = total_dep = total_wd = total_wdcount = total_traders = 0
        for r in data:
            total_ftc     += r['ftc']
            total_rdp     += r['rdp']
            total_dep     += r['deposit']
            total_wd      += r['withdrawal']
            total_wdcount += r['wd_count']
            total_traders += r['traders']

        total_net = total_dep - total_wd
        grand = {
            "day_group":      "Grand Total",
            "ftc":            total_ftc,
            "rdp":            total_rdp,
            "deposit":        round(total_dep),
            "withdrawal":     round(total_wd),
            "net_deposit":    round(total_net),
            "ltv":            round(total_net / total_ftc, 2) if total_ftc > 0 else 0,
            "pct_std":        round(total_rdp / total_ftc * 100) if total_ftc > 0 else 0,
            "wd_count":       total_wdcount,
            "pct_wd_clients": round(total_wdcount / total_ftc * 100) if total_ftc > 0 else 0,
            "pct_wd_usd":     round(total_wd / total_dep * 100) if total_dep > 0 else 0,
            "traders":        total_traders,
            "traders_pct":    round(total_traders / total_ftc * 100) if total_ftc > 0 else 0,
        }

        _result = {"rows": data, "grand_total": grand, "end_date": end_date}
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
