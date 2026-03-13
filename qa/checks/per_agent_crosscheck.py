"""
Per-agent cross-source validation: MySQL source vs PostgreSQL warehouse.

Validates that for each agent, the values computed from raw MySQL broker_banking
match what the PostgreSQL-based reports show.

Checks:
  1. net_deposits_per_agent  — net $ per agent (Deposit+WD_Cancelled - Withdrawal-Dep_Cancelled)
  2. gross_deposits_per_agent — gross deposit $ per agent
  3. ftc_per_agent            — FTC count per agent (first-time clients)

MySQL uses:
  - bb.decision_time        → confirmation_time
  - bb.normalized_amount/100 → usdamount
  - l1.value = 'Success'    → transactionapproval = 'Approved'
  - l2.value                → transactiontype
  - bb.is_ftd               → ftd flag
  - CASE acquisition_status → original_deposit_owner
  - bu.is_demo = 0          → server_id = 2 (non-demo accounts only)
  - usdamount < 10,000,000  → exclude outliers
"""
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import pymysql
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

from qa.checks.base import QAResult, STATUS

# Tolerance for float amount comparison (1% — rounding + sync lag)
_NET_TOL  = 0.01
_DEP_TOL  = 0.01
# FTC tolerance: 0 = exact; mismatches flagged individually
_FTC_TOL  = 0


def _flag(section, name, ctx, status, message, expected=None, actual=None):
    return QAResult("Sync", section, name, ctx, expected, actual, 0.0, 0.0, status, message)


def _get_mysql():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        connect_timeout=15,
        read_timeout=120,
        ssl={"ssl": True},
    )


# ── MySQL queries ────────────────────────────────────────────────────────────

_MYSQL_NET_DEP = """
    SELECT
        CASE
            WHEN bb.acquisition_status = 1 OR bb.sales_rep_id = 0 THEN bb.retention_rep_id
            WHEN bb.acquisition_status = 0 OR bb.sales_rep_id != 0 THEN bb.sales_rep_id
        END AS agent_id,
        SUM(CASE
            WHEN l2.value IN ('Deposit', 'Withdrawal Cancelled') THEN  bb.normalized_amount / 100
            WHEN l2.value IN ('Withdrawal', 'Deposit Cancelled') THEN -(bb.normalized_amount / 100)
            ELSE 0
        END) AS net_usd,
        SUM(CASE
            WHEN l2.value IN ('Deposit', 'Withdrawal Cancelled') THEN bb.normalized_amount / 100
            ELSE 0
        END) AS deposit_usd
    FROM crmdb.broker_banking bb
    JOIN crmdb.v_ant_broker_user bu ON bb.broker_user_id = bu.id
    LEFT JOIN (SELECT `key`, value FROM crmdb.autolut WHERE type = 'TransactionStatus') l1
        ON l1.`key` = bb.status
    LEFT JOIN (SELECT `key`, value FROM crmdb.autolut WHERE type = 'BrokerBankingType') l2
        ON l2.`key` = bb.type
    WHERE l1.value = 'Success'
      AND l2.value IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
      AND bb.decision_time >= %s
      AND bb.decision_time <  %s
      AND bu.is_demo = 0
      AND (bb.normalized_amount / 100) < 10000000
    GROUP BY agent_id
    HAVING agent_id IS NOT NULL AND agent_id > 0
"""

_MYSQL_FTC = """
    SELECT
        CASE
            WHEN bb.acquisition_status = 1 OR bb.sales_rep_id = 0 THEN bb.retention_rep_id
            WHEN bb.acquisition_status = 0 OR bb.sales_rep_id != 0 THEN bb.sales_rep_id
        END AS agent_id,
        COUNT(DISTINCT bb.user_id) AS ftc_count
    FROM crmdb.broker_banking bb
    JOIN crmdb.v_ant_broker_user bu ON bb.broker_user_id = bu.id
    LEFT JOIN crmdb.user_additional_info_rel uair ON uair.user_id = bb.user_id
    LEFT JOIN (SELECT `key`, value FROM crmdb.autolut WHERE type = 'TransactionStatus') l1
        ON l1.`key` = bb.status
    LEFT JOIN (SELECT `key`, value FROM crmdb.autolut WHERE type = 'BrokerBankingType') l2
        ON l2.`key` = bb.type
    WHERE l1.value = 'Success'
      AND l2.value = 'Deposit'
      AND bb.is_ftd = 1
      AND uair.qualification_time >= %s
      AND uair.qualification_time <  %s
      AND bu.is_demo = 0
    GROUP BY agent_id
    HAVING agent_id IS NOT NULL AND agent_id > 0
"""


# ── PostgreSQL queries ───────────────────────────────────────────────────────

_PG_NET_DEP = """
    SELECT
        t.original_deposit_owner AS agent_id,
        SUM(CASE
            WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
            WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
        END) AS net_usd,
        SUM(CASE
            WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN t.usdamount
            ELSE 0
        END) AS deposit_usd
    FROM transactions t
    WHERE t.transactionapproval = 'Approved'
      AND (t.deleted = 0 OR t.deleted IS NULL)
      AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
      AND t.confirmation_time >= %(date_from)s
      AND t.confirmation_time <  %(date_to_excl)s
    GROUP BY t.original_deposit_owner
    HAVING t.original_deposit_owner IS NOT NULL
"""

_PG_FTC = """
    SELECT
        t.original_deposit_owner AS agent_id,
        COUNT(DISTINCT t.vtigeraccountid) AS ftc_count
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
    HAVING t.original_deposit_owner IS NOT NULL
"""


# ── helpers ──────────────────────────────────────────────────────────────────

def _compare_dicts(
    mysql_data: Dict[int, dict],
    pg_data: Dict[int, dict],
    field: str,
    tol: float,
) -> Tuple[int, int, List[dict]]:
    """
    Compare field values between mysql_data and pg_data (both keyed by agent_id).
    Returns (total_compared, match_count, list_of_mismatch_dicts).
    Only considers agents that have a non-zero value in either source.
    """
    all_agents = set(mysql_data.keys()) | set(pg_data.keys())
    mismatches = []
    match_count = 0

    for agent_id in all_agents:
        mysql_val = float(mysql_data.get(agent_id, {}).get(field, 0) or 0)
        pg_val    = float(pg_data.get(agent_id, {}).get(field, 0) or 0)

        # Skip agents with tiny activity (< $1 or 0 FTC) in both — avoids float noise
        if field != "ftc_count" and abs(mysql_val) < 1 and abs(pg_val) < 1:
            continue
        if field == "ftc_count" and mysql_val == 0 and pg_val == 0:
            continue

        diff = abs(mysql_val - pg_val)
        pct  = diff / abs(mysql_val) if mysql_val != 0 else (1.0 if diff > 0 else 0.0)

        if pct <= tol:
            match_count += 1
        else:
            mismatches.append({
                "agent_id": agent_id,
                "mysql":    round(mysql_val, 2),
                "pg":       round(pg_val, 2),
                "diff":     round(diff, 2),
                "pct":      round(pct * 100, 2),
            })

    total = match_count + len(mismatches)
    return total, match_count, sorted(mismatches, key=lambda x: x["diff"], reverse=True)


# ── main entry point ─────────────────────────────────────────────────────────

def run_per_agent_crosscheck(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    dt_to        = datetime.strptime(date_to, "%Y-%m-%d")
    date_to_excl = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")

    # ── Fetch from MySQL ─────────────────────────────────────────────────────
    mysql_net: Dict[int, dict] = {}
    mysql_ftc: Dict[int, dict] = {}
    try:
        my_conn = _get_mysql()
        try:
            with my_conn.cursor() as cur:
                # Net + gross deposits
                cur.execute(_MYSQL_NET_DEP, (date_from, date_to_excl))
                for row in cur.fetchall():
                    agent_id = int(row[0]) if row[0] else None
                    if agent_id:
                        mysql_net[agent_id] = {
                            "net_usd":     float(row[1] or 0),
                            "deposit_usd": float(row[2] or 0),
                        }
                # FTC
                cur.execute(_MYSQL_FTC, (date_from, date_to_excl))
                for row in cur.fetchall():
                    agent_id = int(row[0]) if row[0] else None
                    if agent_id:
                        mysql_ftc[agent_id] = {"ftc_count": int(row[1] or 0)}
        finally:
            my_conn.close()
    except Exception as e:
        return [_flag("Per-Agent", "mysql_fetch", "Grand Total",
                      STATUS["ERROR"], f"MySQL per-agent fetch failed: {e}")]

    # ── Fetch from PostgreSQL ────────────────────────────────────────────────
    pg_net: Dict[int, dict] = {}
    pg_ftc: Dict[int, dict] = {}
    params = {"date_from": date_from, "date_to_excl": date_to_excl}

    with conn.cursor() as cur:
        try:
            cur.execute(_PG_NET_DEP, params)
            for row in cur.fetchall():
                agent_id = int(row[0]) if row[0] else None
                if agent_id:
                    pg_net[agent_id] = {
                        "net_usd":     float(row[1] or 0),
                        "deposit_usd": float(row[2] or 0),
                    }
        except Exception as e:
            conn.rollback()
            return [_flag("Per-Agent", "pg_fetch", "Grand Total",
                          STATUS["ERROR"], f"PG net deposits per-agent fetch failed: {e}")]

        try:
            cur.execute(_PG_FTC, params)
            for row in cur.fetchall():
                agent_id = int(row[0]) if row[0] else None
                if agent_id:
                    pg_ftc[agent_id] = {"ftc_count": int(row[1] or 0)}
        except Exception as e:
            conn.rollback()
            return [_flag("Per-Agent", "pg_fetch", "Grand Total",
                          STATUS["ERROR"], f"PG FTC per-agent fetch failed: {e}")]

    # ── Compare: Net deposits ─────────────────────────────────────────────────
    total, match, mismatches = _compare_dicts(mysql_net, pg_net, "net_usd", _NET_TOL)
    _emit_comparison(results, "Net Deposits", "net_deposits_per_agent",
                     total, match, mismatches, _NET_TOL, "USD net deposits")

    # ── Compare: Gross deposits ───────────────────────────────────────────────
    total, match, mismatches = _compare_dicts(mysql_net, pg_net, "deposit_usd", _DEP_TOL)
    _emit_comparison(results, "Gross Deposits", "gross_deposits_per_agent",
                     total, match, mismatches, _DEP_TOL, "USD gross deposits")

    # ── Compare: FTC ──────────────────────────────────────────────────────────
    total, match, mismatches = _compare_dicts(mysql_ftc, pg_ftc, "ftc_count", _FTC_TOL)
    _emit_comparison(results, "FTC", "ftc_per_agent",
                     total, match, mismatches, _FTC_TOL, "FTC count")

    return results


def _emit_comparison(
    results: List[QAResult],
    section: str,
    check_name: str,
    total: int,
    match: int,
    mismatches: List[dict],
    tol: float,
    label: str,
) -> None:
    """Emit a summary result + individual FAIL results for mismatches."""
    if total == 0:
        results.append(_flag(section, check_name, "Grand Total",
                             STATUS["WARN"], f"No agents with {label} found in period"))
        return

    pct_match = round(match / total * 100, 1)
    n_miss    = len(mismatches)

    # Summary
    st = STATUS["PASS"] if n_miss == 0 else (STATUS["WARN"] if pct_match >= 95 else STATUS["FAIL"])
    results.append(_flag(
        section, check_name, "Grand Total", st,
        f"{match}/{total} agents match ({pct_match}%) — {n_miss} mismatches "
        f"(tolerance={round(tol*100,1)}%)",
        total, match,
    ))

    # Individual mismatches — cap at top 20 by $ diff
    for m in mismatches[:20]:
        diff_pct = m["pct"]
        st = STATUS["FAIL"] if diff_pct > tol * 100 else STATUS["WARN"]
        results.append(QAResult(
            report="Sync",
            section=section,
            check_name=check_name,
            context=f"agent_id={m['agent_id']}",
            expected=m["mysql"],
            actual=m["pg"],
            diff=m["diff"],
            pct_diff=round(m["pct"] / 100, 6),
            status=st,
            message=(
                f"MySQL={m['mysql']}, PG={m['pg']}, "
                f"diff={m['diff']} ({m['pct']}%)"
            ),
        ))
