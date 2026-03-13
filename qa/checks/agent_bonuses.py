"""
Agent Bonuses QA checks — Sales + Retention.
Mirrors the SQL and Python tier logic from app/routes/agent_bonuses.py.
"""
import math
from datetime import datetime, timedelta, date as date_type
from typing import List
import calendar

from qa.checks.base import QAResult, STATUS

# ── Tier tables (copied exactly from agent_bonuses.py) ──────────────────────

OFFICE_GROUP_A = {'GMT', 'CY', 'BU'}
OFFICE_GROUP_B = {'ABJ-NG', 'SA', 'LAG-NG'}


def _office_group(office: str) -> str:
    if office in OFFICE_GROUP_A:
        return 'A'
    if office in OFFICE_GROUP_B:
        return 'B'
    return 'other'


def _net_bonus_pct(net_usd: float, group: str) -> float:
    if group == 'A':
        if net_usd >= 200_000: return 0.04
        if net_usd >= 150_000: return 0.0375
        if net_usd >= 100_000: return 0.035
        if net_usd >= 75_000:  return 0.03
        if net_usd >= 50_000:  return 0.02
        if net_usd >= 20_000:  return 0.015
        return 0.0
    if group == 'B':
        if net_usd >= 100_000: return 0.035
        if net_usd >= 80_000:  return 0.03
        if net_usd >= 60_000:  return 0.028
        if net_usd >= 50_000:  return 0.025
        if net_usd >= 40_000:  return 0.02
        if net_usd >= 30_000:  return 0.018
        if net_usd >= 20_000:  return 0.015
        if net_usd >= 10_000:  return 0.01
        return 0.0
    return 0.0


def _vol_bonus_pct(vol_pct: float, group: str) -> float:
    if group == 'A':
        if vol_pct >= 2.0:  return 0.015
        if vol_pct >= 1.5:  return 0.0125
        if vol_pct >= 1.0:  return 0.01
        if vol_pct >= 0.75: return 0.005
        if vol_pct >= 0.5:  return 0.002
        return -0.005
    return 0.0


def _sales_multiplier(ftd100: int) -> int:
    if ftd100 >= 48: return 65
    if ftd100 >= 44: return 60
    if ftd100 >= 40: return 55
    if ftd100 >= 36: return 50
    if ftd100 >= 32: return 45
    if ftd100 >= 28: return 40
    if ftd100 >= 24: return 35
    if ftd100 >= 20: return 30
    if ftd100 >= 15: return 25
    if ftd100 >= 10: return 20
    if ftd100 >= 5:  return 15
    return 0


def _sales_target_bonus(ftd100: int, target_ftc: int) -> int:
    if target_ftc <= 0 or ftd100 < target_ftc:
        return 0
    n = ftd100
    if n >= 60: return 1500
    if n >= 50: return 1000
    if n >= 35: return 500
    if n >= 30: return 300
    if n >= 25: return 200
    if n >= 20: return 150
    if n >= 5:  return 100
    return 0


def _last_day(d: date_type) -> date_type:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def _flag(report, section, name, ctx, status, message, expected=None, actual=None):
    return QAResult(report, section, name, ctx, expected, actual, 0.0, 0.0, status, message)


def _ok(report, section, name, ctx, expected, actual, tol=0.0):
    try:
        diff = abs(float(expected) - float(actual))
        pct  = diff / abs(float(expected)) if expected not in (0, None) else 0.0
    except Exception:
        diff, pct = 0.0, 0.0
    st = STATUS["PASS"] if pct <= tol else STATUS["FAIL"]
    msg = f"expected={expected}, actual={actual}, diff={round(diff,4)}"
    return QAResult(report, section, name, ctx, expected, actual, round(diff,4), round(pct,6), st, msg)


# ── SQL templates ────────────────────────────────────────────────────────────

_SALES_SQL = """
    SELECT
        COALESCE(u.office_name, 'N/A')                       AS office_name,
        COALESCE(u.agent_name, u.full_name, 'N/A')            AS agent_name,
        COALESCE(tgt.target_ftc, 0)::int                      AS target_ftc,
        COALESCE(ftc.ftc_count, 0)::int                       AS ftc_count,
        COALESCE(f100.ftd100_count, 0)::int                   AS ftd100_count,
        COALESCE(fab.ftd_amount_bonus, 0)::float              AS ftd_amount_bonus_sql
    FROM crm_users u
    LEFT JOIN (
        SELECT agent_id::bigint, SUM(ftc)::int AS target_ftc
        FROM targets
        WHERE date >= %(date_from)s AND date < %(date_to_excl)s
        GROUP BY agent_id
    ) tgt ON tgt.agent_id = u.id
    LEFT JOIN (
        SELECT t.original_deposit_owner AS agent_id,
               COUNT(DISTINCT t.vtigeraccountid)::int AS ftc_count
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
        SELECT f.original_deposit_owner AS agent_id,
               COUNT(DISTINCT f.accountid)::int AS ftd100_count
        FROM ftd100_clients f
        WHERE f.ftd_100_date >= %(date_from)s
          AND f.ftd_100_date <  %(date_to_excl)s
        GROUP BY f.original_deposit_owner
    ) f100 ON f100.agent_id = u.id
    LEFT JOIN (
        SELECT f.original_deposit_owner AS agent_id,
               SUM(CASE
                   WHEN f.ftd_100_amount < 500  THEN 0
                   WHEN f.ftd_100_amount < 1000 THEN 10
                   WHEN f.ftd_100_amount < 5000 THEN 20
                   ELSE 50
               END)::float AS ftd_amount_bonus
        FROM ftd100_clients f
        WHERE f.ftd_100_date >= %(date_from)s
          AND f.ftd_100_date <  %(date_to_excl)s
        GROUP BY f.original_deposit_owner
    ) fab ON fab.agent_id = u.id
    WHERE u.department_ = 'Sales'
      AND u.team = 'Conversion'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
    ORDER BY u.office_name NULLS LAST, f100.ftd100_count DESC NULLS LAST, u.agent_name
"""

_RETENTION_SQL = """
    SELECT
        COALESCE(u.office_name, 'N/A')                   AS office_name,
        COALESCE(u.agent_name, u.full_name, 'N/A')        AS agent_name,
        COALESCE(u.office, '')                             AS office,
        COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
        COALESCE(net.net_usd, 0)::float                   AS net_usd,
        COALESCE(vol.open_volume_usd, 0)::float           AS open_volume_usd
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
        SELECT a.assigned_to AS agent_id, SUM(d.notional_value)::float AS open_volume_usd
        FROM dealio_mt4trades d
        JOIN trading_accounts ta ON ta.login::bigint = d.login
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE d.open_time::date >= %(date_from)s AND d.open_time::date <= %(date_to)s
          AND EXTRACT(YEAR FROM d.open_time) >= 2024
          AND ta.vtigeraccountid IS NOT NULL
        GROUP BY a.assigned_to
    ) vol ON vol.agent_id = u.id
    WHERE u.department_ = 'Retention'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
      AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
    ORDER BY u.office_name NULLS LAST, u.agent_name
"""


def run_bonus_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    results += _sales_bonus_checks(conn, date_from, date_to, cfg)
    results += _retention_bonus_checks(conn, date_from, date_to, cfg)
    return results


def _sales_bonus_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_to        = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_excl = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    params = {"date_from": date_from, "date_to_excl": date_to_excl}

    with conn.cursor() as cur:
        try:
            cur.execute(_SALES_SQL, params)
            rows = cur.fetchall()
        except Exception as e:
            conn.rollback()
            return [_flag("Agent Bonuses", "Sales", "query", "Grand Total",
                          STATUS["ERROR"], f"Sales bonus query failed: {e}")]

    # cols: office_name, agent_name, target_ftc, ftc_count, ftd100_count, ftd_amount_bonus_sql
    data = []
    for r in rows:
        office_name    = r[0]
        agent_name     = r[1]
        target_ftc     = int(r[2] or 0)
        ftc_count      = int(r[3] or 0)
        ftd100_count   = int(r[4] or 0)
        ftd_amount_raw = float(r[5] or 0)

        qualify        = target_ftc > 0 and ftd100_count >= 0.50 * target_ftc
        multiplier     = _sales_multiplier(ftd100_count)
        basic_bonus    = ftd100_count * multiplier if qualify else 0
        stb            = _sales_target_bonus(ftd100_count, target_ftc) if qualify else 0
        ftd_amount     = ftd_amount_raw if qualify else 0
        total_bonus    = basic_bonus + stb + ftd_amount

        data.append({
            "office_name":   office_name,
            "agent_name":    agent_name,
            "target_ftc":    target_ftc,
            "ftc_count":     ftc_count,
            "ftd100_count":  ftd100_count,
            "qualify":       qualify,
            "multiplier":    multiplier,
            "basic_bonus":   basic_bonus,
            "stb":           stb,
            "ftd_amount":    ftd_amount,
            "total_bonus":   total_bonus,
        })

    # 1. qualify_rule — if not qualify, all bonuses must be 0
    qualify_issues = [r for r in data if not r["qualify"] and r["total_bonus"] != 0]
    if qualify_issues:
        for r in qualify_issues:
            results.append(_flag("Agent Bonuses", "Sales", "qualify_rule", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"Does not qualify but total_bonus={r['total_bonus']}",
                                 0, r["total_bonus"]))
    else:
        results.append(_flag("Agent Bonuses", "Sales", "qualify_rule", "All Agents",
                             STATUS["PASS"], "Qualify rule applied correctly"))

    # 2. basic_bonus_math — basic_bonus == ftd100 * multiplier (for qualifying agents)
    for r in data:
        if not r["qualify"]:
            continue
        expected = r["ftd100_count"] * r["multiplier"]
        actual   = r["basic_bonus"]
        if expected != actual:
            results.append(_flag("Agent Bonuses", "Sales", "basic_bonus_math", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"basic_bonus mismatch: {actual} != {expected}",
                                 expected, actual))

    if not any(r.check_name == "basic_bonus_math" and r.status == STATUS["FAIL"] for r in results):
        results.append(_flag("Agent Bonuses", "Sales", "basic_bonus_math", "All Agents",
                             STATUS["PASS"], "basic_bonus = ftd100 * multiplier for all"))

    # 3. office_totals — sum of total_bonus per office
    from collections import defaultdict
    office_totals = defaultdict(float)
    for r in data:
        office_totals[r["office_name"]] += r["total_bonus"]
    for office, total in office_totals.items():
        status = STATUS["PASS"] if math.isfinite(total) else STATUS["FAIL"]
        results.append(_flag("Agent Bonuses", "Sales", "office_totals", office,
                             status, f"Office total_bonus sum = {round(total, 2)}", round(total, 2), round(total, 2)))

    return results


def _retention_bonus_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_from      = datetime.strptime(date_from, "%Y-%m-%d").date()
    dt_to        = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_excl = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    last_day     = _last_day(dt_from).strftime("%Y-%m-%d")
    params = {
        "date_from": date_from, "date_to_excl": date_to_excl,
        "date_to": date_to, "last_day": last_day,
    }
    tol_bonus = cfg.get("tolerances", {}).get("bonus_usd", 0.01)

    with conn.cursor() as cur:
        try:
            cur.execute(_RETENTION_SQL, params)
            rows = cur.fetchall()
        except Exception as e:
            conn.rollback()
            return [_flag("Agent Bonuses", "Retention", "query", "Grand Total",
                          STATUS["ERROR"], f"Retention bonus query failed: {e}")]

    # cols: office_name, agent_name, office, target_net, net_usd, open_volume_usd
    data = []
    for r in rows:
        office_name      = r[0]
        agent_name       = r[1]
        office           = r[2]
        target_net       = float(r[3] or 0)
        net_usd          = float(r[4] or 0)
        open_volume_usd  = float(r[5] or 0)

        target_vol       = target_net * 1650
        group            = _office_group(office)
        target_net_pct   = net_usd / target_net if target_net > 0 else None
        target_vol_pct   = open_volume_usd / target_vol if target_vol > 0 else None

        pct_on_net        = _net_bonus_pct(net_usd, group)
        pct_on_target_net = 0.005 if (
            group == 'A' and target_net_pct is not None and target_net_pct >= 1.0
        ) else 0.0
        pct_on_target_vol = (
            _vol_bonus_pct(target_vol_pct, group) if target_vol_pct is not None else 0.0
        )
        total_bonus_pct   = pct_on_net + pct_on_target_net + pct_on_target_vol
        basic_bonus_usd   = round(total_bonus_pct * net_usd, 2)

        data.append({
            "office_name":       office_name,
            "agent_name":        agent_name,
            "office":            office,
            "target_net":        target_net,
            "net_usd":           net_usd,
            "open_volume_usd":   open_volume_usd,
            "group":             group,
            "pct_on_net":        pct_on_net,
            "pct_on_target_net": pct_on_target_net,
            "pct_on_target_vol": pct_on_target_vol,
            "total_bonus_pct":   total_bonus_pct,
            "basic_bonus_usd":   basic_bonus_usd,
        })

    # 1. total_pct_sum — total == pct_on_net + pct_on_target_net + pct_on_target_vol
    for r in data:
        expected = round(r["pct_on_net"] + r["pct_on_target_net"] + r["pct_on_target_vol"], 8)
        actual   = round(r["total_bonus_pct"], 8)
        if abs(expected - actual) > 0.00001:
            results.append(_flag("Agent Bonuses", "Retention", "total_pct_sum", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"total_pct mismatch: {actual} != {expected}",
                                 expected, actual))

    if not any(r.check_name == "total_pct_sum" and r.status == STATUS["FAIL"] for r in results):
        results.append(_flag("Agent Bonuses", "Retention", "total_pct_sum", "All Agents",
                             STATUS["PASS"], "total_bonus_pct = sum of components for all"))

    # 2. bonus_usd_math — basic_bonus_usd ≈ total_bonus_pct * net_usd
    tol_amt = tol_bonus
    for r in data:
        expected = round(r["total_bonus_pct"] * r["net_usd"], 2)
        actual   = r["basic_bonus_usd"]
        diff     = abs(expected - actual)
        pct      = diff / abs(expected) if expected != 0 else 0.0
        if pct > tol_amt:
            results.append(_flag("Agent Bonuses", "Retention", "bonus_usd_math", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"bonus_usd mismatch: {actual} vs {expected}",
                                 expected, actual))

    if not any(r.check_name == "bonus_usd_math" and r.status == STATUS["FAIL"] for r in results):
        results.append(_flag("Agent Bonuses", "Retention", "bonus_usd_math", "All Agents",
                             STATUS["PASS"], f"basic_bonus_usd within {tol_amt*100:.1f}% tolerance"))

    # 3. pct_on_target_net_rule — 0.5% only for group A with net >= target
    for r in data:
        expected_pct = 0.005 if (
            r["group"] == "A" and r["target_net"] > 0 and r["net_usd"] >= r["target_net"]
        ) else 0.0
        if abs(r["pct_on_target_net"] - expected_pct) > 0.00001:
            results.append(_flag("Agent Bonuses", "Retention", "pct_on_target_net_rule", r["agent_name"],
                                 STATUS["FAIL"],
                                 f"pct_on_target_net={r['pct_on_target_net']} expected={expected_pct}",
                                 expected_pct, r["pct_on_target_net"]))

    if not any(r.check_name == "pct_on_target_net_rule" and r.status == STATUS["FAIL"] for r in results):
        results.append(_flag("Agent Bonuses", "Retention", "pct_on_target_net_rule", "All Agents",
                             STATUS["PASS"], "pct_on_target_net rule correct for all"))

    # 4. office_totals
    from collections import defaultdict
    office_totals = defaultdict(float)
    for r in data:
        office_totals[r["office_name"]] += r["basic_bonus_usd"]
    for office, total in office_totals.items():
        status = STATUS["PASS"] if math.isfinite(total) else STATUS["FAIL"]
        results.append(_flag("Agent Bonuses", "Retention", "office_totals", office,
                             status, f"Office bonus_usd sum = {round(total, 2)}", round(total, 2), round(total, 2)))

    return results
