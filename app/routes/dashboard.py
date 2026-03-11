from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta, date as date_type
import calendar

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
    return templates.TemplateResponse("dashboard.html", {"request": request, "current_user": user})


@router.get("/api/dashboard")
async def dashboard_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    today = datetime.utcnow().date()
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
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
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
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND u.department_ = 'Sales'
                  AND u.team = 'Conversion'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
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
                JOIN crm_users u ON u.id = a.assigned_to
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND COALESCE(t.comment, '') NOT ILIKE '%%bonus%%'
                  AND u.department_ = 'Retention'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
                  AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
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
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            ftd_daily = int(row[0] or 0)
            ftd_monthly = int(row[1] or 0)

            # Q5 — FTC #
            cur.execute("""
                SELECT
                  COUNT(DISTINCT t.vtigeraccountid) FILTER (WHERE a.client_qualification_date = CURRENT_DATE) AS daily,
                  COUNT(DISTINCT t.vtigeraccountid) AS monthly
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND a.client_qualification_date >= %(month_start)s
                  AND a.client_qualification_date <  %(tomorrow)s
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            row = cur.fetchone()
            ftc_daily = int(row[0] or 0)
            ftc_monthly = int(row[1] or 0)

            # Q6 — # Traders
            cur.execute("""
                SELECT
                  COUNT(DISTINCT ta.vtigeraccountid) FILTER (WHERE d.open_time::date = CURRENT_DATE) AS daily,
                  COUNT(DISTINCT ta.vtigeraccountid) AS monthly
                FROM dealio_mt4trades d
                JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
                WHERE d.notional_value > 0
                  AND ta.vtigeraccountid IS NOT NULL
                  AND ta.vtigeraccountid::text != ''
                  AND d.open_time::date >= %(month_start)s
                  AND d.open_time::date <= %(today)s
            """, {"month_start": month_start_str, "today": today_str})
            row = cur.fetchone()
            traders_daily = int(row[0] or 0)
            traders_monthly = int(row[1] or 0)

            # Q7 — Open Volume
            cur.execute("""
                SELECT
                  COALESCE(SUM(d.notional_value) FILTER (WHERE d.open_time::date = CURRENT_DATE), 0) AS daily,
                  COALESCE(SUM(d.notional_value), 0) AS monthly
                FROM dealio_mt4trades d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = a.assigned_to
                WHERE d.open_time::date >= %(month_start)s
                  AND d.open_time::date <= %(today)s
                  AND EXTRACT(YEAR FROM d.open_time) >= 2024
                  AND ta.vtigeraccountid IS NOT NULL
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            """, {"month_start": month_start_str, "today": today_str})
            row = cur.fetchone()
            ov_daily = float(row[0] or 0)
            ov_monthly = float(row[1] or 0)

            # Q8 — End Equity Zeroed (snapshot)
            cur.execute("""
                WITH last_date AS (
                    SELECT MAX(date) AS last_available_date
                    FROM dealio_daily_profit
                    WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
                      AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
                ),
                old_bal_bonus AS (
                    SELECT
                        t.login,
                        t.confirmation_time::date AS bonus_date,
                        SUM(CASE WHEN t.transactiontype IN ('FRF Commission', 'Bonus') THEN t.usdamount ELSE 0 END)
                      - SUM(CASE WHEN t.transactiontype IN ('FRF Commission Cancelled', 'BonusCancelled') THEN t.usdamount ELSE 0 END)
                            AS old_bonus_usd
                    FROM transactions t
                    WHERE t.transactionapproval = 'Approved'
                      AND (t.deleted = 0 OR t.deleted IS NULL)
                      AND t.transactiontype IN ('FRF Commission', 'Bonus', 'FRF Commission Cancelled', 'BonusCancelled')
                    GROUP BY t.login, t.confirmation_time::date
                ),
                old_bonus_balance AS (
                    SELECT login, SUM(old_bonus_usd) AS old_bonus_balance
                    FROM old_bal_bonus
                    WHERE bonus_date <= (SELECT last_available_date FROM last_date)
                    GROUP BY login
                )
                SELECT COALESCE(SUM(
                    GREATEST(
                        GREATEST(d.convertedbalance + d.convertedfloatingpnl, 0)
                            - COALESCE(ob.old_bonus_balance, 0),
                        0
                    )
                ), 0) AS end_equity_zeroed
                FROM dealio_daily_profit d
                JOIN trading_accounts ta  ON ta.login::bigint = d.login
                JOIN accounts a           ON a.accountid = ta.vtigeraccountid
                JOIN crm_users u          ON u.id = d.assigned_to
                LEFT JOIN old_bonus_balance ob ON ob.login::bigint = d.login
                WHERE d.date = (SELECT last_available_date FROM last_date)
            """)
            end_equity_zeroed = float(cur.fetchone()[0] or 0)

            # Q9 — ABS Exposure (snapshot)
            cur.execute("""
                SELECT COALESCE(
                  CASE WHEN ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END)) < 1
                       THEN 0
                       ELSE ABS(SUM(CASE WHEN cmd=0 THEN notional_value ELSE -notional_value END))
                  END, 0)
                FROM dealio_mt4trades
                WHERE CAST(close_time AS DATE) = '1970-01-01'
                  AND symbol NOT IN ('ZeroingZAR','ZeroingUSD','ZeroingNGN','ZeroingKES','ZeroingJPY','ZeroingGBP','ZeroingEUR')
            """)
            abs_exposure = float(cur.fetchone()[0] or 0)

        def rr_money(val):
            return round(val / safe_wdp * wd_total, 2)

        def rr_int(val):
            return round(val / safe_wdp * wd_total)

        return JSONResponse(content={
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
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
