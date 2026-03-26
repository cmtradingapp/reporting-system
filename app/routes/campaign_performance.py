from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/campaign-performance", response_class=HTMLResponse)
async def campaign_performance_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("campaign_performance.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/campaign-performance")
async def campaign_performance_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    _ck = f"camp_perf_v1:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Leads + Live Accounts (always today / MTD from mv_account_stats)
            cur.execute("""
                SELECT new_leads_today, new_leads_month, new_live_today, new_live_month
                FROM mv_account_stats LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                leads_today, leads_mtd, live_today, live_mtd = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                )
            else:
                leads_today = leads_mtd = live_today = live_mtd = 0

            # Deposits, Withdrawals, Net, FTD daily + MTD (tx_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(deposit_usd),    0)                                                AS deposits,
                    COALESCE(SUM(withdrawal_usd), 0)                                                AS withdrawals,
                    COALESCE(SUM(net_usd),        0)                                                AS net_deposits,
                    COALESCE(SUM(ftd_count),      0)                                                AS ftd_mtd,
                    COALESCE(SUM(CASE WHEN tx_date = %(date_to)s THEN ftd_count ELSE 0 END), 0)     AS ftd_daily
                FROM mv_daily_kpis
                WHERE tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
            row = cur.fetchone()
            if row:
                deposits_total    = float(row[0] or 0)
                withdrawals_total = float(row[1] or 0)
                net_total         = float(row[2] or 0)
                ftd_mtd           = int(row[3] or 0)
                ftd_daily         = int(row[4] or 0)
            else:
                deposits_total = withdrawals_total = net_total = ftd_mtd = ftd_daily = 0

            # FTC daily + MTD (qual_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(ftc_count), 0)                                                     AS ftc_mtd,
                    COALESCE(SUM(CASE WHEN qual_date = %(date_to)s THEN ftc_count ELSE 0 END), 0)   AS ftc_daily
                FROM mv_daily_kpis
                WHERE qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
            row = cur.fetchone()
            ftc_mtd   = int(row[0] or 0) if row else 0
            ftc_daily = int(row[1] or 0) if row else 0

            # Number of Traders — distinct non-test accounts with approved transactions
            cur.execute("""
                SELECT COUNT(DISTINCT t.vtigeraccountid)
                FROM transactions t
                JOIN accounts a  ON a.accountid = t.vtigeraccountid
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND t.confirmation_time::date >= %(date_from)s
                  AND t.confirmation_time::date <  %(date_to_excl)s
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            row = cur.fetchone()
            traders_count = int(row[0] or 0) if row else 0

        _result = {
            "leads":         {"daily": leads_today, "mtd": leads_mtd},
            "live_accounts": {"daily": live_today,  "mtd": live_mtd},
            "ftd":           {"daily": ftd_daily,   "mtd": ftd_mtd},
            "ftc":           {"daily": ftc_daily,   "mtd": ftc_mtd},
            "deposits":      round(deposits_total, 2),
            "withdrawals":   round(withdrawals_total, 2),
            "net_deposits":  round(net_total, 2),
            "traders_count": traders_count,
            "date_from":     date_from,
            "date_to":       date_to,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
