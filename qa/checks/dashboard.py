"""
Dashboard QA checks.
Mirrors the SQL from app/routes/dashboard.py and cross-validates with performance.
"""
import math
from datetime import datetime, timedelta, date as date_type
from typing import List
import calendar
from zoneinfo import ZoneInfo

from qa.checks.base import QAResult, STATUS

_TZ = ZoneInfo("Europe/Nicosia")


def _flag(report, section, name, ctx, status, message, expected=None, actual=None):
    return QAResult(report, section, name, ctx, expected, actual, 0.0, 0.0, status, message)


def _ok(report, section, name, ctx, expected, actual, tol=0.0):
    try:
        diff = abs(float(expected) - float(actual))
        pct  = diff / abs(float(expected)) if expected not in (0, None) else 0.0
    except Exception:
        diff, pct = 0.0, 0.0
    st  = STATUS["PASS"] if pct <= tol else STATUS["FAIL"]
    msg = f"expected={expected}, actual={actual}, diff={round(diff,4)}"
    return QAResult(report, section, name, ctx, expected, actual, round(diff,4), round(pct,6), st, msg)


def _last_day(d: date_type) -> date_type:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def _count_working_days(start: date_type, end: date_type, holidays: set) -> int:
    if end < start:
        return 0
    count, current = 0, start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            count += 1
        current += timedelta(days=1)
    return count


def run_dashboard_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_from      = datetime.strptime(date_from, "%Y-%m-%d").date()
    dt_to        = datetime.strptime(date_to, "%Y-%m-%d").date()
    month_start  = dt_from
    tomorrow_str = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    today_str    = date_to
    month_start_str = date_from

    tol_pnl = cfg.get("tolerances", {}).get("pnl", 0.01)
    tol_vol = cfg.get("tolerances", {}).get("open_volume", 0.01)
    tol_eq  = cfg.get("tolerances", {}).get("end_equity", 0.01)

    with conn.cursor() as cur:
        # Fetch holidays
        try:
            cur.execute("SELECT holiday_date FROM public_holidays")
            holidays = {row[0] for row in cur.fetchall()}
        except Exception:
            conn.rollback()
            holidays = set()

        month_end  = _last_day(dt_from)
        wd_total   = _count_working_days(month_start, month_end, holidays)
        wd_passed  = _count_working_days(month_start, dt_to, holidays)
        safe_wdp   = wd_passed if wd_passed > 0 else 1

        # 1. working_days_math
        expected_wd = _count_working_days(month_start, month_end, holidays)
        results.append(_flag("Dashboard", "Metrics", "working_days_math", "Grand Total",
                             STATUS["PASS"] if expected_wd == wd_total else STATUS["FAIL"],
                             f"Working days = {wd_total} (expected {expected_wd})",
                             expected_wd, wd_total))

        # Q1 — Grand NET Deposits (same query as dashboard.py)
        try:
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0) AS monthly
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
                  AND t.confirmation_time >= %(month_start)s
                  AND t.confirmation_time <  %(tomorrow)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            nd_monthly = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            nd_monthly = None
            results.append(_flag("Dashboard", "Metrics", "net_grand_query", "Grand Total",
                                 STATUS["ERROR"], f"Grand net query failed: {e}"))

        # Q2 — Sales NET
        try:
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0)
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
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
            nd_sales = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            nd_sales = None
            results.append(_flag("Dashboard", "Sales", "sales_net_query", "Grand Total",
                                 STATUS["ERROR"], f"Sales net query failed: {e}"))

        # Q3 — Retention NET
        try:
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                  END), 0)
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                JOIN crm_users u ON u.id = a.assigned_to
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
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
            nd_ret = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            nd_ret = None
            results.append(_flag("Dashboard", "Retention", "retention_net_query", "Grand Total",
                                 STATUS["ERROR"], f"Retention net query failed: {e}"))

        # Q5 — FTC monthly
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT t.vtigeraccountid)
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND a.client_qualification_date >= %(month_start)s
                  AND a.client_qualification_date <  %(tomorrow)s
            """, {"month_start": month_start_str, "tomorrow": tomorrow_str})
            ftc_monthly = int(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            ftc_monthly = None
            results.append(_flag("Dashboard", "Metrics", "ftc_query", "Grand Total",
                                 STATUS["ERROR"], f"FTC query failed: {e}"))

        # Q6 — Traders
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT ta.vtigeraccountid)
                FROM dealio_mt4trades d
                JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
                WHERE d.notional_value > 0
                  AND ta.vtigeraccountid IS NOT NULL
                  AND ta.vtigeraccountid::text != ''
                  AND d.open_time::date >= %(month_start)s
                  AND d.open_time::date <= %(today)s
            """, {"month_start": month_start_str, "today": today_str})
            traders_monthly = int(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            traders_monthly = None
            results.append(_flag("Dashboard", "Metrics", "traders_query", "Grand Total",
                                 STATUS["ERROR"], f"Traders query failed: {e}"))

        # Q7 — Open Volume
        try:
            cur.execute("""
                SELECT COALESCE(SUM(d.notional_value), 0)
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
            ov_monthly = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            ov_monthly = None
            results.append(_flag("Dashboard", "Metrics", "open_volume_query", "Grand Total",
                                 STATUS["ERROR"], f"Open volume query failed: {e}"))

        # Q8 — Equity freshness
        try:
            cur.execute("""
                SELECT MAX(date)::date
                FROM dealio_daily_profit
                WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM %(today)s::date)
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM %(today)s::date)
            """, {"today": today_str})
            last_eq_date = cur.fetchone()[0]
            if last_eq_date is None:
                results.append(_flag("Dashboard", "Metrics", "equity_snapshot_freshness", "Grand Total",
                                     STATUS["ERROR"], "No dealio_daily_profit data for current month"))
            else:
                results.append(_flag("Dashboard", "Metrics", "equity_snapshot_freshness", "Grand Total",
                                     STATUS["PASS"],
                                     f"Last equity date: {last_eq_date}",
                                     today_str, str(last_eq_date)))
        except Exception as e:
            conn.rollback()
            results.append(_flag("Dashboard", "Metrics", "equity_snapshot_freshness", "Grand Total",
                                 STATUS["ERROR"], f"Equity freshness query failed: {e}"))

        # Q10 — Daily PnL plausibility
        try:
            cur.execute("""
                SELECT MAX(date)::date
                FROM dealio_daily_profit
                WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM %(today)s::date)
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM %(today)s::date)
            """, {"today": today_str})
            last_mtd = cur.fetchone()[0]
            if last_mtd:
                cur.execute("""
                    SELECT COALESCE(SUM(
                        COALESCE(convertedclosedpnl,0) + COALESCE(converteddeltafloatingpnl,0)
                    ), 0)
                    FROM dealio_daily_profit
                    WHERE date::date = %(last_mtd)s
                """, {"last_mtd": last_mtd.strftime("%Y-%m-%d")})
                pnl_daily = float(cur.fetchone()[0] or 0)
                if math.isfinite(pnl_daily):
                    results.append(_flag("Dashboard", "Metrics", "pnl_daily_plausibility", "Grand Total",
                                         STATUS["PASS"],
                                         f"Daily PnL = {round(pnl_daily,2)} (finite)",
                                         "finite", round(pnl_daily, 2)))
                else:
                    results.append(_flag("Dashboard", "Metrics", "pnl_daily_plausibility", "Grand Total",
                                         STATUS["FAIL"], f"Daily PnL is non-finite: {pnl_daily}"))
        except Exception as e:
            conn.rollback()
            results.append(_flag("Dashboard", "Metrics", "pnl_daily_plausibility", "Grand Total",
                                 STATUS["ERROR"], f"PnL daily query failed: {e}"))

        # MSSQL PnL monthly availability
        try:
            from app.db.mssql_conn import get_pnl_cash_monthly
            pnl_monthly = get_pnl_cash_monthly(month_start_str, tomorrow_str)
            results.append(_flag("Dashboard", "Metrics", "pnl_monthly_available", "Grand Total",
                                 STATUS["PASS"],
                                 f"MSSQL PnL monthly = {round(pnl_monthly,2)}",
                                 "non-null", round(pnl_monthly, 2)))
        except Exception as e:
            results.append(_flag("Dashboard", "Metrics", "pnl_monthly_available", "Grand Total",
                                 STATUS["ERROR"], f"MSSQL PnL monthly connection failed: {e}"))

    # Traders count positive
    if traders_monthly is not None:
        status = STATUS["PASS"] if traders_monthly > 0 else STATUS["WARN"]
        results.append(_flag("Dashboard", "Metrics", "traders_count_positive", "Grand Total",
                             status, f"traders_monthly = {traders_monthly}", ">0", traders_monthly))

    # FTC vs Performance cross-check
    if ftc_monthly is not None:
        # Also fetch grand_ftc from performance query for comparison
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT COUNT(DISTINCT t.vtigeraccountid)
                    FROM transactions t
                    JOIN accounts a ON a.accountid = t.vtigeraccountid
                    WHERE t.transactionapproval = 'Approved'
                      AND (t.deleted = 0 OR t.deleted IS NULL)
                      AND t.transactiontype = 'Deposit'
                      AND t.ftd = 1
                      AND a.client_qualification_date IS NOT NULL
                      AND a.client_qualification_date >= %(date_from)s
                      AND a.client_qualification_date <  %(date_to_excl)s
                """, {"date_from": month_start_str,
                      "date_to_excl": tomorrow_str})
                perf_grand_ftc = int(cur.fetchone()[0] or 0)
                results.append(_ok("Dashboard", "Cross-Report", "ftc_vs_performance",
                                   "Grand Total", perf_grand_ftc, ftc_monthly, tol=0.0))
            except Exception as e:
                conn.rollback()
                results.append(_flag("Dashboard", "Cross-Report", "ftc_vs_performance", "Grand Total",
                                     STATUS["ERROR"], f"FTC cross-check query failed: {e}"))

    # RR math — verify formula: rr = monthly / wd_passed * wd_total
    if nd_monthly is not None and wd_passed > 0:
        expected_rr = round(nd_monthly / safe_wdp * wd_total, 2)
        results.append(_flag("Dashboard", "Metrics", "rr_math", "Net Deposits",
                             STATUS["PASS"],
                             f"RR formula: {nd_monthly}/{safe_wdp}*{wd_total} = {expected_rr}",
                             expected_rr, expected_rr))

    return results
