"""
Performance report QA checks — Sales + Retention.
Mirrors the SQL from app/routes/scoreboard.py.
"""
import math
from datetime import datetime, timedelta, date as date_type
from typing import List
import calendar
from zoneinfo import ZoneInfo

from qa.checks.base import QAResult, STATUS

_TZ = ZoneInfo("Europe/Nicosia")


def _ok(report, section, name, ctx, expected, actual, tol=0.0):
    """Build a QAResult, computing diff/pct and assigning PASS/FAIL."""
    try:
        diff = abs(float(expected) - float(actual))
        pct  = diff / abs(float(expected)) if expected not in (0, None) else 0.0
    except Exception:
        diff, pct = 0.0, 0.0
    status = STATUS["PASS"] if pct <= tol else STATUS["FAIL"]
    msg = f"expected={expected}, actual={actual}, diff={round(diff,4)}"
    return QAResult(report, section, name, ctx, expected, actual, round(diff,4), round(pct,6), status, msg)


def _flag(report, section, name, ctx, status, message, expected=None, actual=None):
    return QAResult(report, section, name, ctx, expected, actual, 0.0, 0.0, status, message)


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


# ---------------------------------------------------------------------------
# Sales checks
# ---------------------------------------------------------------------------

_SALES_SQL = """
    SELECT
        COALESCE(u.office_name, 'N/A')              AS office_name,
        COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
        COALESCE(ftc.cnt, 0)                         AS ftc,
        COALESCE(tgt.target_ftc, 0)                  AS target_ftc,
        COALESCE(f100.ftd100_cnt, 0)                 AS ftd100,
        COALESCE(net.net_usd, 0)::float              AS net_deposits
    FROM crm_users u
    LEFT JOIN (
        SELECT t.original_deposit_owner AS agent_id,
               COUNT(DISTINCT t.vtigeraccountid) AS cnt
        FROM transactions t
        JOIN accounts a ON a.accountid = t.vtigeraccountid
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype = 'Deposit'
          AND t.ftd = 1
          AND a.client_qualification_date IS NOT NULL
          AND a.client_qualification_date >= %(date_from)s
          AND a.client_qualification_date <  %(date_to_excl)s
        GROUP BY t.original_deposit_owner
    ) ftc ON ftc.agent_id = u.id
    LEFT JOIN (
        SELECT agent_id::bigint, SUM(ftc)::int AS target_ftc
        FROM targets
        WHERE date >= %(date_from)s AND date < %(date_to_excl)s
        GROUP BY agent_id
    ) tgt ON tgt.agent_id = u.id
    LEFT JOIN (
        SELECT f.original_deposit_owner AS agent_id,
               COUNT(DISTINCT f.accountid) AS ftd100_cnt
        FROM ftd100_clients f
        WHERE f.ftd_100_date >= %(date_from)s
          AND f.ftd_100_date <  %(date_to_excl)s
        GROUP BY f.original_deposit_owner
    ) f100 ON f100.agent_id = u.id
    LEFT JOIN (
        SELECT t.original_deposit_owner AS agent_id,
               SUM(CASE
                   WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                   WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
               END) AS net_usd
        FROM transactions t
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND t.confirmation_time >= %(date_from)s
          AND t.confirmation_time <  %(date_to_excl)s
        GROUP BY t.original_deposit_owner
    ) net ON net.agent_id = u.id
    WHERE u.department_ = 'Sales'
      AND u.team = 'Conversion'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
    ORDER BY u.office_name NULLS LAST, ftc.cnt DESC NULLS LAST, u.agent_name
"""

_GRAND_FTC_SQL = """
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
"""


def run_performance_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_to   = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_excl = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    params = {"date_from": date_from, "date_to_excl": date_to_excl}
    min_sales = cfg.get("checks", {}).get("min_sales_agents", 5)

    with conn.cursor() as cur:
        # Fetch holidays
        try:
            cur.execute("SELECT holiday_date FROM public_holidays")
            holidays = {row[0] for row in cur.fetchall()}
        except Exception:
            conn.rollback()
            holidays = set()

        # Run sales query
        try:
            cur.execute(_SALES_SQL, params)
            rows = cur.fetchall()
        except Exception as e:
            conn.rollback()
            return [_flag("Performance", "Sales", "query", "Grand Total",
                          STATUS["ERROR"], f"Sales query failed: {e}")]

        # Run grand FTC query
        try:
            cur.execute(_GRAND_FTC_SQL, params)
            grand_ftc = int(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            grand_ftc = None
            results.append(_flag("Performance", "Sales", "grand_ftc_query", "Grand Total",
                                 STATUS["ERROR"], f"Grand FTC query failed: {e}"))

    # Process sales rows
    # cols: office_name, agent_name, ftc, target_ftc, ftd100, net_deposits
    sales_data = [
        {
            "office_name":  r[0],
            "agent_name":   r[1],
            "ftc":          int(r[2] or 0),
            "target_ftc":   int(r[3] or 0),
            "ftd100":       int(r[4] or 0),
            "net_deposits": float(r[5] or 0),
        }
        for r in rows
    ]

    # 1. agent_count
    n = len(sales_data)
    status = STATUS["PASS"] if n >= min_sales else STATUS["WARN"]
    results.append(_flag("Performance", "Sales", "agent_count", "Grand Total",
                         status, f"{n} sales agents returned (min={min_sales})", min_sales, n))

    # 2. null_check
    for row in sales_data:
        if not row["office_name"] or row["office_name"] == "N/A":
            results.append(_flag("Performance", "Sales", "null_check", row["agent_name"],
                                 STATUS["WARN"], "office_name is NULL/N/A"))
        if not row["agent_name"] or row["agent_name"] == "N/A":
            results.append(_flag("Performance", "Sales", "null_check", str(row.get("agent_name")),
                                 STATUS["WARN"], "agent_name is NULL/N/A"))

    if not any(r.check_name == "null_check" for r in results if r.section == "Sales"):
        results.append(_flag("Performance", "Sales", "null_check", "All Agents",
                             STATUS["PASS"], "No NULL names found"))

    # 3. ftc_per_agent — ftc must be non-negative integer
    ftc_issues = [r for r in sales_data if r["ftc"] < 0]
    if ftc_issues:
        for r in ftc_issues:
            results.append(_flag("Performance", "Sales", "ftc_per_agent", r["agent_name"],
                                 STATUS["FAIL"], f"Negative FTC: {r['ftc']}", 0, r["ftc"]))
    else:
        results.append(_flag("Performance", "Sales", "ftc_per_agent", "All Agents",
                             STATUS["PASS"], "All FTC values >= 0"))

    # 4. net_per_agent — must be finite float
    net_issues = [r for r in sales_data if not math.isfinite(r["net_deposits"])]
    if net_issues:
        for r in net_issues:
            results.append(_flag("Performance", "Sales", "net_per_agent", r["agent_name"],
                                 STATUS["FAIL"], f"Non-finite net_deposits: {r['net_deposits']}"))
    else:
        results.append(_flag("Performance", "Sales", "net_per_agent", "All Agents",
                             STATUS["PASS"], "All net_deposits values finite"))

    # 5. ftd100_per_agent — ftd100 <= ftc
    ftd100_issues = [r for r in sales_data if r["ftd100"] > r["ftc"] and r["ftc"] > 0]
    if ftd100_issues:
        for r in ftd100_issues:
            results.append(_flag("Performance", "Sales", "ftd100_per_agent", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"FTD100 ({r['ftd100']}) > FTC ({r['ftc']})",
                                 r["ftc"], r["ftd100"]))
    else:
        results.append(_flag("Performance", "Sales", "ftd100_per_agent", "All Agents",
                             STATUS["PASS"], "All FTD100 <= FTC"))

    # 6. target_coverage — >= 80% of agents should have targets
    agents_with_target = sum(1 for r in sales_data if r["target_ftc"] > 0)
    total_agents = len(sales_data)
    if total_agents > 0:
        coverage = agents_with_target / total_agents
        status = STATUS["PASS"] if coverage >= 0.8 else STATUS["WARN"]
        results.append(_flag("Performance", "Sales", "target_coverage", "Grand Total",
                             status, f"{agents_with_target}/{total_agents} agents have targets ({round(coverage*100,1)}%)",
                             total_agents, agents_with_target))

    # 7. office_totals — sum of agent FTCs per office (self-consistency)
    from collections import defaultdict
    office_ftc = defaultdict(int)
    for r in sales_data:
        office_ftc[r["office_name"]] += r["ftc"]
    for office, total in office_ftc.items():
        status = STATUS["PASS"] if total >= 0 else STATUS["FAIL"]
        results.append(_flag("Performance", "Sales", "office_totals", office,
                             status, f"Office FTC sum = {total}", total, total))

    # 8. grand_ftc_vs_sales — grand_ftc >= sum(sales ftc)
    if grand_ftc is not None:
        sales_ftc_total = sum(r["ftc"] for r in sales_data)
        status = STATUS["PASS"] if grand_ftc >= sales_ftc_total else STATUS["FAIL"]
        results.append(_flag("Performance", "Sales", "grand_ftc_vs_sales", "Grand Total",
                             status,
                             f"grand_ftc={grand_ftc} vs sales_total={sales_ftc_total}",
                             grand_ftc, sales_ftc_total))

    # -----------------------------------------------------------------------
    # Retention checks
    # -----------------------------------------------------------------------
    results += _run_retention_checks(conn, date_from, date_to, cfg)

    return results


_RETENTION_SQL = """
    SELECT
        COALESCE(u.office_name, 'N/A')                   AS office_name,
        COALESCE(u.department, 'N/A')                     AS dept_name,
        COALESCE(u.agent_name, u.full_name, 'N/A')        AS agent_name,
        COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
        COALESCE(net.net_usd, 0)::float                   AS net_usd,
        COALESCE(dep.deposit_usd, 0)::float               AS deposit_usd
    FROM crm_users u
    LEFT JOIN (
        SELECT agent_id::bigint, SUM(net)::float AS monthly_target_net
        FROM targets
        WHERE date >= %(date_from)s AND date <= %(last_day)s AND agent_id IS NOT NULL
        GROUP BY agent_id
    ) tgt ON tgt.agent_id = u.id
    LEFT JOIN (
        SELECT a.assigned_to AS agent_id,
               SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount
                        WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')  THEN -t.usdamount END) AS net_usd
        FROM transactions t
        JOIN accounts a ON a.accountid = t.vtigeraccountid
        WHERE t.transactionapproval = 'Approved' AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND t.confirmation_time >= %(date_from)s AND t.confirmation_time < %(date_to_excl)s
          AND COALESCE(t.comment, '') NOT ILIKE '%%bonus%%'
        GROUP BY a.assigned_to
    ) net ON net.agent_id = u.id
    LEFT JOIN (
        SELECT a.assigned_to AS agent_id, SUM(t.usdamount)::float AS deposit_usd
        FROM transactions t JOIN accounts a ON a.accountid = t.vtigeraccountid
        WHERE t.transactionapproval = 'Approved' AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled')
          AND t.confirmation_time >= %(date_from)s AND t.confirmation_time < %(date_to_excl)s
          AND COALESCE(t.comment, '') NOT ILIKE '%%bonus%%'
        GROUP BY a.assigned_to
    ) dep ON dep.agent_id = u.id
    WHERE u.department_ = 'Retention'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
    ORDER BY u.office_name NULLS LAST, dept_name, u.agent_name
"""


def _run_retention_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    dt_to   = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_excl = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    last_day = _last_day(dt_from).strftime("%Y-%m-%d")
    min_ret = cfg.get("checks", {}).get("min_retention_agents", 10)
    params = {"date_from": date_from, "date_to_excl": date_to_excl, "date_to": date_to, "last_day": last_day}

    with conn.cursor() as cur:
        try:
            cur.execute(_RETENTION_SQL, params)
            rows = cur.fetchall()
        except Exception as e:
            conn.rollback()
            return [_flag("Performance", "Retention", "query", "Grand Total",
                          STATUS["ERROR"], f"Retention query failed: {e}")]

    # cols: office_name, dept_name, agent_name, target_net, net_usd, deposit_usd
    data = [
        {
            "office_name": r[0],
            "dept_name":   r[1],
            "agent_name":  r[2],
            "target_net":  float(r[3] or 0),
            "net_usd":     float(r[4] or 0),
            "deposit_usd": float(r[5] or 0),
        }
        for r in rows
    ]

    # 1. agent_count
    n = len(data)
    status = STATUS["PASS"] if n >= min_ret else STATUS["WARN"]
    results.append(_flag("Performance", "Retention", "agent_count", "Grand Total",
                         status, f"{n} retention agents (min={min_ret})", min_ret, n))

    # 2. null_check
    null_found = False
    for row in data:
        for field in ("office_name", "dept_name", "agent_name"):
            if not row[field] or row[field] == "N/A":
                null_found = True
                results.append(_flag("Performance", "Retention", "null_check", row["agent_name"],
                                     STATUS["WARN"], f"{field} is NULL/N/A"))
    if not null_found:
        results.append(_flag("Performance", "Retention", "null_check", "All Agents",
                             STATUS["PASS"], "No NULL names found"))

    # 3. target_net_per_agent — warn if target == 0
    zero_targets = [r for r in data if r["target_net"] == 0.0]
    if zero_targets:
        results.append(_flag("Performance", "Retention", "target_net_per_agent", "All Agents",
                             STATUS["WARN"],
                             f"{len(zero_targets)} agents have target_net == 0"))
    else:
        results.append(_flag("Performance", "Retention", "target_net_per_agent", "All Agents",
                             STATUS["PASS"], "All agents have target_net > 0"))

    # 4. net_usd_per_agent — must be finite; warn if net==0 and target>0
    for row in data:
        if not math.isfinite(row["net_usd"]):
            results.append(_flag("Performance", "Retention", "net_usd_per_agent", row["agent_name"],
                                 STATUS["FAIL"], f"Non-finite net_usd: {row['net_usd']}"))
        elif row["net_usd"] == 0 and row["target_net"] > 0:
            results.append(_flag("Performance", "Retention", "net_usd_per_agent", row["agent_name"],
                                 STATUS["WARN"],
                                 f"net_usd=0 but target_net={row['target_net']}"))

    if not any(r.check_name == "net_usd_per_agent" for r in results):
        results.append(_flag("Performance", "Retention", "net_usd_per_agent", "All Agents",
                             STATUS["PASS"], "All net_usd values finite"))

    # 5. deposit_vs_net — deposit_usd >= net_usd
    dep_issues = [r for r in data if r["deposit_usd"] < r["net_usd"] - 0.01]
    if dep_issues:
        for row in dep_issues:
            results.append(_flag("Performance", "Retention", "deposit_vs_net", row["agent_name"],
                                 STATUS["FAIL"],
                                 f"deposit_usd ({row['deposit_usd']}) < net_usd ({row['net_usd']})",
                                 row["deposit_usd"], row["net_usd"]))
    else:
        results.append(_flag("Performance", "Retention", "deposit_vs_net", "All Agents",
                             STATUS["PASS"], "deposit_usd >= net_usd for all agents"))

    # 6. office_totals — sum net_usd per office (self-consistency check)
    from collections import defaultdict
    office_net = defaultdict(float)
    for r in data:
        office_net[r["office_name"]] += r["net_usd"]
    for office, total in office_net.items():
        status = STATUS["PASS"] if math.isfinite(total) else STATUS["FAIL"]
        results.append(_flag("Performance", "Retention", "office_totals", office,
                             status, f"Office net_usd sum = {round(total, 2)}", round(total, 2), round(total, 2)))

    return results
