from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta, date as date_type
from zoneinfo import ZoneInfo
import calendar

_TZ = ZoneInfo("Europe/Nicosia")

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def last_day_of_month(d: date_type) -> date_type:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def count_working_days(start: date_type, end: date_type, holidays: set) -> int:
    if end < start:
        return 0
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            count += 1
        current += timedelta(days=1)
    return count


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") not in ("admin", "general"):
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("dashboard.html", {"request": request, "current_user": user})


def _dashboard_calc(today: date_type) -> dict:
    """Run all dashboard queries and return the result dict. Raises on error."""
    month_start = today.replace(day=1)
    month_end = last_day_of_month(today)
    tomorrow = today + timedelta(days=1)

    month_start_str = month_start.strftime("%Y-%m-%d")
    today_str       = today.strftime("%Y-%m-%d")
    tomorrow_str    = tomorrow.strftime("%Y-%m-%d")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Fetch public holidays
            try:
                cur.execute("SELECT holiday_date FROM public_holidays")
                holidays = {row[0] for row in cur.fetchall()}
            except Exception:
                conn.rollback()
                holidays = set()

            wd_total  = count_working_days(month_start, month_end, holidays)
            wd_passed = count_working_days(month_start, today, holidays)
            wd_left   = wd_total - wd_passed
            safe_wdp  = wd_passed if wd_passed > 0 else 1

            # Get last_mtd date (for PnL cards)
            cur.execute("""
                SELECT MAX(date)::date
                FROM dealio_daily_profits
                WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
            """)
            last_mtd_date      = cur.fetchone()[0] or (today - timedelta(days=1))
            last_mtd_str       = last_mtd_date.strftime("%Y-%m-%d")
            last_mtd_plus1_str = (last_mtd_date + timedelta(days=1)).strftime("%Y-%m-%d")

            p = {"month_start": month_start_str, "today": today_str, "tomorrow": tomorrow_str}

            # Q1 — Net Deposits grand total  (mv_run_rate 'all')
            cur.execute("""
                SELECT
                    COALESCE(SUM(net_usd) FILTER (WHERE tx_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(net_usd), 0)                                           AS monthly
                FROM mv_run_rate
                WHERE dept_group = 'all'
                  AND tx_date >= %(month_start)s AND tx_date <= %(today)s
            """, p)
            row = cur.fetchone()
            nd_daily, nd_monthly = float(row[0] or 0), float(row[1] or 0)

            # Q2 — Net Deposits – Sales  (mv_run_rate 'sales')
            cur.execute("""
                SELECT
                    COALESCE(SUM(net_usd) FILTER (WHERE tx_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(net_usd), 0)                                           AS monthly
                FROM mv_run_rate
                WHERE dept_group = 'sales'
                  AND tx_date >= %(month_start)s AND tx_date <= %(today)s
            """, p)
            row = cur.fetchone()
            nd_sales_daily, nd_sales_monthly = float(row[0] or 0), float(row[1] or 0)

            # Q3 — Net Deposits – Retention  (mv_run_rate 'retention')
            cur.execute("""
                SELECT
                    COALESCE(SUM(net_usd) FILTER (WHERE tx_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(net_usd), 0)                                           AS monthly
                FROM mv_run_rate
                WHERE dept_group = 'retention'
                  AND tx_date >= %(month_start)s AND tx_date <= %(today)s
            """, p)
            row = cur.fetchone()
            nd_ret_daily, nd_ret_monthly = float(row[0] or 0), float(row[1] or 0)

            # Q4 — FTD count  (mv_run_rate 'all', tx_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(ftd_count) FILTER (WHERE tx_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(ftd_count), 0)                                           AS monthly
                FROM mv_run_rate
                WHERE dept_group = 'all'
                  AND tx_date >= %(month_start)s AND tx_date <= %(today)s
            """, p)
            row = cur.fetchone()
            ftd_daily, ftd_monthly = int(row[0] or 0), int(row[1] or 0)

            # Q5 — FTC count  (mv_run_rate 'all', qual_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(ftc_count) FILTER (WHERE qual_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(ftc_count), 0)                                             AS monthly
                FROM mv_run_rate
                WHERE dept_group = 'all'
                  AND qual_date >= %(month_start)s AND qual_date <= %(today)s
            """, p)
            row = cur.fetchone()
            ftc_daily, ftc_monthly = int(row[0] or 0), int(row[1] or 0)

            # Q6 — Open Volume  (mv_volume_stats — SUM notional_usd)
            cur.execute("""
                SELECT
                    COALESCE(SUM(notional_usd) FILTER (WHERE open_date = %(today)s::date), 0) AS daily,
                    COALESCE(SUM(notional_usd), 0)                                             AS monthly
                FROM mv_volume_stats
                WHERE open_date >= %(month_start)s AND open_date <= %(today)s
            """, p)
            row = cur.fetchone()
            ov_daily, ov_monthly = float(row[0] or 0), float(row[1] or 0)

            # Q7 — Daily PnL  (live from dealio_daily_profits)
            cur.execute("""
                SELECT COALESCE(SUM(
                    COALESCE(convertedclosedpnl, 0) + COALESCE(converteddeltafloatingpnl, 0)
                ), 0)
                FROM dealio_daily_profits d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE d.date >= %(last_mtd)s::date
                  AND d.date <  %(last_mtd)s::date + INTERVAL '1 day'
                  AND a.is_test_account = 0
            """, {"last_mtd": last_mtd_str})
            pnl_daily = round(float(cur.fetchone()[0] or 0), 2)

            # Q8 — New Leads + Live Accounts (from mv_account_stats)
            cur.execute("""
                SELECT new_leads_today, new_leads_month, new_live_today, new_live_month
                FROM mv_account_stats
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                new_leads_today, new_leads_month, new_live_today, new_live_month = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                )
            else:
                new_leads_today = new_leads_month = new_live_today = new_live_month = 0

        def rr_money(val):
            return round(val / safe_wdp * wd_total, 2)

        def rr_int(val):
            return round(val / safe_wdp * wd_total)

        return {
            "date":                 today_str,
            "month_start":          month_start_str,
            "working_days":         wd_total,
            "working_days_passed":  wd_passed,
            "working_days_left":    wd_left,
            "net_deposits":           {"daily": round(nd_daily, 2),       "monthly": round(nd_monthly, 2),       "rr": rr_money(nd_monthly)},
            "net_deposits_sales":     {"daily": round(nd_sales_daily, 2), "monthly": round(nd_sales_monthly, 2), "rr": rr_money(nd_sales_monthly)},
            "net_deposits_retention": {"daily": round(nd_ret_daily, 2),   "monthly": round(nd_ret_monthly, 2),   "rr": rr_money(nd_ret_monthly)},
            "ftd":         {"daily": ftd_daily,  "monthly": ftd_monthly,  "rr": rr_int(ftd_monthly)},
            "ftc":         {"daily": ftc_daily,  "monthly": ftc_monthly,  "rr": rr_int(ftc_monthly)},
            "open_volume": {"daily": round(ov_daily, 2), "monthly": round(ov_monthly, 2), "rr": rr_money(ov_monthly)},
            "pnl_cash": {
                "daily":    pnl_daily,
                "pnl_date": last_mtd_str,
            },
            "new_leads": {"daily": new_leads_today, "monthly": new_leads_month},
            "new_live":  {"daily": new_live_today,  "monthly": new_live_month},
        }
    finally:
        conn.close()


@router.get("/api/dashboard")
async def dashboard_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    today = datetime.now(_TZ).date()
    _ck = f"dashboard_v9:{today.isoformat()}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        _result = _dashboard_calc(today)
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
