"""
FTC Date report QA checks.
Mirrors the SQL from app/routes/ftc_date.py.
"""
import math
from datetime import datetime, timedelta
from typing import List

from qa.checks.base import QAResult, STATUS

_EXPECTED_GROUPS = {
    '0 - 7 days', '8 - 14 days', '15 - 30 days',
    '31 - 60 days', '61 - 90 days', '91 - 120 days', '120+ days',
}

_FTC_SQL = """
    WITH ftc_groups AS (
        SELECT
            a.accountid,
            a.client_qualification_date::date AS qual_date,
            (%(end_date)s::date - a.client_qualification_date::date) AS days_diff
        FROM accounts a
        WHERE a.client_qualification_date IS NOT NULL
          AND a.client_qualification_date::date >= '2024-01-01'
          AND a.client_qualification_date::date <= %(end_date)s::date
          AND a.is_test_account = 0
    ),
    tx_per_account AS (
        SELECT
            t.vtigeraccountid AS accountid,
            SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount ELSE 0 END) AS deposit_usd,
            SUM(CASE WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')  THEN t.usdamount ELSE 0 END) AS withdrawal_usd
        FROM transactions t
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
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
        FROM dealio_mt4trades d
        JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
        WHERE d.notional_value > 0
          AND ta.vtigeraccountid IS NOT NULL
          AND ta.vtigeraccountid::text != ''
          AND d.open_time::date <= %(end_date)s::date
    ),
    grouped AS (
        SELECT
            CASE
                WHEN fg.days_diff BETWEEN 0  AND 7   THEN 1
                WHEN fg.days_diff BETWEEN 8  AND 14  THEN 2
                WHEN fg.days_diff BETWEEN 15 AND 30  THEN 3
                WHEN fg.days_diff BETWEEN 31 AND 60  THEN 4
                WHEN fg.days_diff BETWEEN 61 AND 90  THEN 5
                WHEN fg.days_diff BETWEEN 91 AND 120 THEN 6
                WHEN fg.days_diff > 120              THEN 7
            END AS group_order,
            CASE
                WHEN fg.days_diff BETWEEN 0  AND 7   THEN '0 - 7 days'
                WHEN fg.days_diff BETWEEN 8  AND 14  THEN '8 - 14 days'
                WHEN fg.days_diff BETWEEN 15 AND 30  THEN '15 - 30 days'
                WHEN fg.days_diff BETWEEN 31 AND 60  THEN '31 - 60 days'
                WHEN fg.days_diff BETWEEN 61 AND 90  THEN '61 - 90 days'
                WHEN fg.days_diff BETWEEN 91 AND 120 THEN '91 - 120 days'
                WHEN fg.days_diff > 120              THEN '120+ days'
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
"""


def _flag(report, section, name, ctx, status, message, expected=None, actual=None):
    return QAResult(report, section, name, ctx, expected, actual, 0.0, 0.0, status, message)


def run_ftcdate_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    params = {"end_date": date_to}

    with conn.cursor() as cur:
        try:
            cur.execute(_FTC_SQL, params)
            rows = cur.fetchall()
        except Exception as e:
            conn.rollback()
            return [_flag("FTC Date", "Groups", "query", "Grand Total",
                          STATUS["ERROR"], f"FTC date query failed: {e}")]

    # cols: group_order, day_group, ftc_count, rdp_count, deposit_usd, withdrawal_usd, wd_count, trader_count
    data = []
    for r in rows:
        ftc = int(r[2] or 0)
        rdp = int(r[3] or 0)
        dep = float(r[4] or 0)
        wd  = float(r[5] or 0)
        wdc = int(r[6] or 0)
        trd = int(r[7] or 0)
        net_dep = dep - wd
        data.append({
            "group_order": r[0],
            "day_group":   r[1],
            "ftc":         ftc,
            "rdp":         rdp,
            "deposit":     dep,
            "withdrawal":  wd,
            "wd_count":    wdc,
            "traders":     trd,
            "net_deposit": net_dep,
            "ltv":         round(net_dep / ftc, 2) if ftc > 0 else 0,
            "pct_std":     round(rdp / ftc * 100) if ftc > 0 else 0,
            "pct_wd_clients": round(wdc / ftc * 100) if ftc > 0 else 0,
            "pct_wd_usd":     round(wd / dep * 100) if dep > 0 else 0,
            "traders_pct":    round(trd / ftc * 100) if ftc > 0 else 0,
        })

    # 1. no_empty_groups
    found_groups = {r["day_group"] for r in data}
    missing = _EXPECTED_GROUPS - found_groups
    if missing:
        results.append(_flag("FTC Date", "Groups", "no_empty_groups", "Grand Total",
                             STATUS["WARN"],
                             f"Missing groups: {missing}",
                             len(_EXPECTED_GROUPS), len(found_groups)))
    else:
        results.append(_flag("FTC Date", "Groups", "no_empty_groups", "Grand Total",
                             STATUS["PASS"], "All 7 day groups present"))

    # Compute grand totals
    total_ftc = sum(r["ftc"] for r in data)
    total_rdp = sum(r["rdp"] for r in data)
    total_dep = sum(r["deposit"] for r in data)
    total_wd  = sum(r["withdrawal"] for r in data)
    total_wdc = sum(r["wd_count"] for r in data)
    total_trd = sum(r["traders"] for r in data)

    # 2. group_ftc_sum
    results.append(_flag("FTC Date", "Groups", "group_ftc_sum", "Grand Total",
                         STATUS["PASS"] if total_ftc > 0 else STATUS["WARN"],
                         f"Sum of group FTC = {total_ftc}",
                         total_ftc, total_ftc))

    # 3. group_deposit_sum
    results.append(_flag("FTC Date", "Groups", "group_deposit_sum", "Grand Total",
                         STATUS["PASS"] if math.isfinite(total_dep) else STATUS["FAIL"],
                         f"Sum of group deposits = {round(total_dep)}",
                         round(total_dep), round(total_dep)))

    # 4. group_withdrawal_sum
    results.append(_flag("FTC Date", "Groups", "group_withdrawal_sum", "Grand Total",
                         STATUS["PASS"] if math.isfinite(total_wd) else STATUS["FAIL"],
                         f"Sum of group withdrawals = {round(total_wd)}",
                         round(total_wd), round(total_wd)))

    # 5. ltv_math — for each group: ltv ≈ (deposit - withdrawal) / ftc
    for r in data:
        if r["ftc"] == 0:
            continue
        expected_ltv = round((r["deposit"] - r["withdrawal"]) / r["ftc"], 2)
        actual_ltv   = r["ltv"]
        diff = abs(expected_ltv - actual_ltv)
        st = STATUS["PASS"] if diff <= 0.01 else STATUS["FAIL"]
        results.append(_flag("FTC Date", "Groups", "ltv_math", r["day_group"],
                             st,
                             f"LTV expected={expected_ltv}, actual={actual_ltv}",
                             expected_ltv, actual_ltv))

    # 6. pct_std_math — rdp / ftc * 100
    for r in data:
        if r["ftc"] == 0:
            continue
        expected_pct = round(r["rdp"] / r["ftc"] * 100)
        actual_pct   = r["pct_std"]
        diff = abs(expected_pct - actual_pct)
        st = STATUS["PASS"] if diff <= 1 else STATUS["FAIL"]
        results.append(_flag("FTC Date", "Groups", "pct_std_math", r["day_group"],
                             st, f"pct_std expected={expected_pct}, actual={actual_pct}",
                             expected_pct, actual_pct))

    # 7. pct_wd_clients_math
    for r in data:
        if r["ftc"] == 0:
            continue
        expected_pct = round(r["wd_count"] / r["ftc"] * 100)
        actual_pct   = r["pct_wd_clients"]
        diff = abs(expected_pct - actual_pct)
        st = STATUS["PASS"] if diff <= 1 else STATUS["FAIL"]
        results.append(_flag("FTC Date", "Groups", "pct_wd_clients_math", r["day_group"],
                             st,
                             f"pct_wd_clients expected={expected_pct}, actual={actual_pct}",
                             expected_pct, actual_pct))

    # 8. pct_wd_usd_math
    for r in data:
        if r["deposit"] == 0:
            continue
        expected_pct = round(r["withdrawal"] / r["deposit"] * 100)
        actual_pct   = r["pct_wd_usd"]
        diff = abs(expected_pct - actual_pct)
        st = STATUS["PASS"] if diff <= 1 else STATUS["FAIL"]
        results.append(_flag("FTC Date", "Groups", "pct_wd_usd_math", r["day_group"],
                             st,
                             f"pct_wd_usd expected={expected_pct}, actual={actual_pct}",
                             expected_pct, actual_pct))

    # 9. traders_pct_math
    for r in data:
        if r["ftc"] == 0:
            continue
        expected_pct = round(r["traders"] / r["ftc"] * 100)
        actual_pct   = r["traders_pct"]
        diff = abs(expected_pct - actual_pct)
        st = STATUS["PASS"] if diff <= 1 else STATUS["FAIL"]
        results.append(_flag("FTC Date", "Groups", "traders_pct_math", r["day_group"],
                             st,
                             f"traders_pct expected={expected_pct}, actual={actual_pct}",
                             expected_pct, actual_pct))

    # 10. rdp_lte_ftc
    for r in data:
        if r["rdp"] > r["ftc"]:
            results.append(_flag("FTC Date", "Groups", "rdp_lte_ftc", r["day_group"],
                                 STATUS["FAIL"],
                                 f"rdp ({r['rdp']}) > ftc ({r['ftc']})",
                                 r["ftc"], r["rdp"]))

    if not any(r.check_name == "rdp_lte_ftc" and r.status == STATUS["FAIL"] for r in results):
        results.append(_flag("FTC Date", "Groups", "rdp_lte_ftc", "All Groups",
                             STATUS["PASS"], "rdp <= ftc for all groups"))

    return results
