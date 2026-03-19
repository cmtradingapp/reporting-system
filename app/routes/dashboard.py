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
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("dashboard.html", {"request": request, "current_user": user})


def _dashboard_calc(today: date_type) -> dict:
    """Run all dashboard queries and return the result dict. Raises on error."""
    month_start = today.replace(day=1)
    month_end = last_day_of_month(today)
    tomorrow = today + timedelta(days=1)

    month_start_str = month_start.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

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

            wd_total = count_working_days(month_start, month_end, holidays)
            wd_passed = count_working_days(month_start, today, holidays)
            wd_left = wd_total - wd_passed
            safe_wdp = wd_passed if wd_passed > 0 else 1

            # Get last_mtd date (for display on PnL cards)
            cur.execute("""
                SELECT MAX(date)::date
                FROM dealio_daily_profits
                WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
            """)
            last_mtd_date = cur.fetchone()[0] or (today - timedelta(days=1))
            last_mtd_str       = last_mtd_date.strftime("%Y-%m-%d")
            last_mtd_plus1_str = (last_mtd_date + timedelta(days=1)).strftime("%Y-%m-%d")

            # Q1 — Net Deposits (grand overall)
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END) FILTER (WHERE t.confirmation_time::date = CURRENT_DATE), 0) AS daily,
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0) AS monthly
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            nd_daily = float(row[0] or 0)
            nd_monthly = float(row[1] or 0)

            # Q2 — Net Deposits – Sales
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END) FILTER (WHERE t.confirmation_time::date = CURRENT_DATE), 0) AS daily,
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0) AS monthly
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND u.department_ = 'Sales'
                  AND u.team = 'Conversion'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            nd_sales_daily = float(row[0] or 0)
            nd_sales_monthly = float(row[1] or 0)

            # Q3 — Net Deposits – Retention
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END) FILTER (WHERE t.confirmation_time::date = CURRENT_DATE), 0) AS daily,
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0) AS monthly
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND a.is_test_account = 0
                  AND u.department_ = 'Retention'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            nd_ret_daily = float(row[0] or 0)
            nd_ret_monthly = float(row[1] or 0)

            # Q4 — FTD #
            cur.execute("""
                SELECT
                  COUNT(t.mttransactionsid) FILTER (WHERE t.confirmation_time::date = CURRENT_DATE) AS daily,
                  COUNT(t.mttransactionsid) AS monthly
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND a.is_test_account = 0
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            ftd_daily = int(row[0] or 0)
            ftd_monthly = int(row[1] or 0)

            # Q5 — FTC #
            cur.execute("""
                SELECT
                  COUNT(DISTINCT t.vtigeraccountid) FILTER (WHERE a.client_qualification_date::date = CURRENT_DATE) AS daily,
                  COUNT(DISTINCT t.vtigeraccountid) AS monthly
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND a.client_qualification_date >= %(month_start)s
                  AND a.client_qualification_date <  %(tomorrow)s
                  AND a.is_test_account = 0
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            ftc_daily = int(row[0] or 0)
            ftc_monthly = int(row[1] or 0)

            # Q6 — # Traders
            cur.execute("""
                SELECT
                  COUNT(DISTINCT ta.vtigeraccountid) FILTER (WHERE d.open_time::date = CURRENT_DATE) AS daily,
                  COUNT(DISTINCT ta.vtigeraccountid) AS monthly
                FROM dealio_trades_mt4 d
                JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE d.notional_value > 0
                  AND ta.vtigeraccountid IS NOT NULL
                  AND ta.vtigeraccountid::text != ''
                  AND d.open_time::date >= %(month_start)s
                  AND d.open_time::date <= %(today)s
                  AND a.is_test_account = 0
            """, {"month_start": month_start_str, "today": today_str})
            row = cur.fetchone()
            traders_daily = int(row[0] or 0)
            traders_monthly = int(row[1] or 0)

            # Q7 — Open Volume
            cur.execute("""
                SELECT
                  COALESCE(SUM(d.notional_value) FILTER (WHERE d.open_time::date = CURRENT_DATE), 0) AS daily,
                  COALESCE(SUM(d.notional_value), 0) AS monthly
                FROM dealio_trades_mt4 d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = a.assigned_to
                WHERE d.open_time::date >= %(month_start)s
                  AND d.open_time::date <= %(today)s
                  AND EXTRACT(YEAR FROM d.open_time) >= 2024
                  AND ta.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            """, {"month_start": month_start_str, "today": today_str})
            row = cur.fetchone()
            ov_daily = float(row[0] or 0)
            ov_monthly = float(row[1] or 0)

            # Q8 — End Equity Zeroed (snapshot)
            cur.execute("""
                WITH latest_equity AS (
                    SELECT DISTINCT ON (login)
                        login, convertedbalance, convertedfloatingpnl
                    FROM dealio_daily_profits
                    WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
                      AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
                    ORDER BY login, date DESC
                ),
                old_bonus_balance AS (
                    SELECT login, SUM(net_amount) AS old_bonus_balance
                    FROM bonus_transactions
                    WHERE confirmation_time::date <= CURRENT_DATE
                    GROUP BY login
                )
                SELECT COALESCE(SUM(
                    CASE
                        WHEN COALESCE(d.convertedbalance, 0) + COALESCE(d.convertedfloatingpnl, 0) <= 0 THEN 0
                        ELSE GREATEST(
                            COALESCE(d.convertedbalance, 0) + COALESCE(d.convertedfloatingpnl, 0)
                                - COALESCE(ob.old_bonus_balance, 0),
                            0
                        )
                    END
                ), 0) AS end_equity_zeroed
                FROM latest_equity d
                JOIN trading_accounts ta  ON ta.login::bigint = d.login
                JOIN accounts a           ON a.accountid = ta.vtigeraccountid
                JOIN crm_users u          ON u.id = a.assigned_to
                LEFT JOIN old_bonus_balance ob ON ob.login::bigint = d.login
                WHERE a.is_test_account = 0
                  AND (ta.deleted = 0 OR ta.deleted IS NULL)
            """)
            end_equity_zeroed = float(cur.fetchone()[0] or 0)

            # Q9 — ABS Exposure (snapshot)
            cur.execute("""
                SELECT COALESCE(
                  CASE WHEN ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END)) < 1
                       THEN 0
                       ELSE ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END))
                  END, 0)
                FROM dealio_trades_mt4 d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE CAST(d.close_time AS DATE) = '1970-01-01'
                  AND d.symbol NOT IN ('ZeroingZAR','ZeroingUSD','ZeroingNGN','ZeroingKES','ZeroingJPY','ZeroingGBP','ZeroingEUR')
                  AND a.is_test_account = 0
            """)
            abs_exposure = float(cur.fetchone()[0] or 0)

            # Q10 — Daily PnL
            cur.execute("""
                SELECT COALESCE(SUM(
                    COALESCE(convertedclosedpnl, 0) + COALESCE(converteddeltafloatingpnl, 0)
                ), 0)
                FROM dealio_daily_profits d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE d.date::date = %(last_mtd)s
                  AND a.is_test_account = 0
            """, {"last_mtd": last_mtd_str})
            pnl_daily = round(float(cur.fetchone()[0] or 0), 2)

        # Monthly PnL
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(
                        COALESCE(convertedclosedpnl, 0) + COALESCE(converteddeltafloatingpnl, 0)
                    ), 0)
                    FROM dealio_daily_profits d
                    JOIN trading_accounts ta ON ta.login::bigint = d.login
                    JOIN accounts a ON a.accountid = ta.vtigeraccountid
                    WHERE d.date::date >= %(month_start)s
                      AND d.date::date < %(tomorrow)s
                      AND a.is_test_account = 0
                """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
                pnl_monthly = round(float(cur.fetchone()[0] or 0), 2)
        except Exception:
            pnl_monthly = None

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
            "ftd":     {"daily": ftd_daily,     "monthly": ftd_monthly,     "rr": rr_int(ftd_monthly)},
            "ftc":     {"daily": ftc_daily,     "monthly": ftc_monthly,     "rr": rr_int(ftc_monthly)},
            "traders": {"daily": traders_daily, "monthly": traders_monthly, "rr": rr_int(traders_monthly)},
            "open_volume": {"daily": round(ov_daily, 2), "monthly": round(ov_monthly, 2), "rr": rr_money(ov_monthly)},
            "end_equity_zeroed": round(end_equity_zeroed, 2),
            "abs_exposure":      round(abs_exposure, 2),
            "pnl_cash": {
                "daily":    pnl_daily,
                "monthly":  pnl_monthly,
                "pnl_date": last_mtd_str,
            },
        }
    finally:
        conn.close()


@router.get("/api/dashboard")
async def dashboard_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    today = datetime.now(_TZ).date()
    _ck = f"dashboard_v6:{today.isoformat()}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        _result = _dashboard_calc(today)
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
