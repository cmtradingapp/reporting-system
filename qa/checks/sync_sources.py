"""
Cross-source QA checks: PostgreSQL vs MySQL/MSSQL sync validation.
"""
import math
from datetime import datetime, timedelta
from typing import List

import pymysql
import pymssql
from app.config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB,
    MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DB,
)

from qa.checks.base import QAResult, STATUS


def _flag(section, name, ctx, status, message, expected=None, actual=None):
    return QAResult("Sync", section, name, ctx, expected, actual, 0.0, 0.0, status, message)


def _ok(section, name, ctx, expected, actual, tol=0.0):
    try:
        diff = abs(float(expected) - float(actual))
        pct  = diff / abs(float(expected)) if expected not in (0, None) else 0.0
    except Exception:
        diff, pct = 0.0, 0.0
    st  = STATUS["PASS"] if pct <= tol else STATUS["FAIL"]
    msg = f"expected={expected}, actual={actual}, diff={round(diff,4)}, pct={round(pct*100,3)}%"
    return QAResult("Sync", section, name, ctx, expected, actual, round(diff,4), round(pct,6), st, msg)


def _get_mysql():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        connect_timeout=10,
        read_timeout=60,
        ssl={"ssl": True},
    )


def _get_mssql():
    return pymssql.connect(
        server=MSSQL_HOST,
        port=str(MSSQL_PORT),
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DB,
        tds_version="7.4",
        conn_properties="",
    )


def run_sync_checks(conn, date_from: str, date_to: str, cfg: dict) -> List[QAResult]:
    results = []
    freshness_hours = cfg.get("checks", {}).get("sync_freshness_hours", 3)

    # ── 1. Sync freshness ────────────────────────────────────────────────────
    entities = ["accounts", "transactions", "targets", "dealio_mt4trades", "crm_users"]
    with conn.cursor() as cur:
        for entity in entities:
            try:
                cur.execute("""
                    SELECT MAX(created_at)
                    FROM sync_log
                    WHERE entity = %(entity)s
                """, {"entity": entity})
                row = cur.fetchone()
                last_sync = row[0] if row else None
                if last_sync is None:
                    results.append(_flag("Freshness", "sync_freshness", entity,
                                         STATUS["ERROR"], f"No sync_log entry for {entity}"))
                else:
                    from datetime import timezone
                    now = datetime.now(timezone.utc)
                    if last_sync.tzinfo is None:
                        from datetime import timezone as tz
                        last_sync = last_sync.replace(tzinfo=tz.utc)
                    age_hours = (now - last_sync).total_seconds() / 3600
                    st = STATUS["PASS"] if age_hours <= freshness_hours else STATUS["ERROR"]
                    results.append(_flag("Freshness", "sync_freshness", entity,
                                         st,
                                         f"{entity}: last sync {round(age_hours,1)}h ago (max={freshness_hours}h)",
                                         freshness_hours, round(age_hours, 1)))
            except Exception as e:
                conn.rollback()
                results.append(_flag("Freshness", "sync_freshness", entity,
                                     STATUS["ERROR"], f"Freshness query failed for {entity}: {e}"))

    # ── 2. Transaction count cross-check: PostgreSQL vs MySQL ────────────────
    tomorrow_str = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE confirmation_time >= %(date_from)s
                  AND confirmation_time <  %(tomorrow)s
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
            """, {"date_from": date_from, "tomorrow": tomorrow_str})
            pg_tx_count = int(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            pg_tx_count = None
            results.append(_flag("Transaction Count", "tx_count_crosscheck", "Grand Total",
                                 STATUS["ERROR"], f"PG tx count query failed: {e}"))

    # MySQL transaction count
    mysql_tx_count = None
    try:
        my_conn = _get_mysql()
        try:
            with my_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM broker_banking
                    WHERE confirmation_time >= %s
                      AND confirmation_time <  %s
                      AND statusmapping = 'Approved'
                """, (date_from, tomorrow_str))
                mysql_tx_count = int(cur.fetchone()[0] or 0)
        finally:
            my_conn.close()
    except Exception as e:
        results.append(_flag("Transaction Count", "tx_count_crosscheck", "Grand Total",
                             STATUS["ERROR"], f"MySQL tx count query failed: {e}"))

    if pg_tx_count is not None and mysql_tx_count is not None:
        results.append(_ok("Transaction Count", "tx_count_crosscheck", "Grand Total",
                           mysql_tx_count, pg_tx_count, tol=0.005))

    # ── 3. Transaction SUM cross-check: Deposits only ───────────────────────
    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT COALESCE(SUM(usdamount), 0)
                FROM transactions
                WHERE confirmation_time >= %(date_from)s
                  AND confirmation_time <  %(tomorrow)s
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
                  AND transactiontype = 'Deposit'
            """, {"date_from": date_from, "tomorrow": tomorrow_str})
            pg_dep_sum = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            pg_dep_sum = None
            results.append(_flag("Transaction Sum", "tx_sum_crosscheck", "Grand Total",
                                 STATUS["ERROR"], f"PG deposit sum query failed: {e}"))

    mysql_dep_sum = None
    try:
        my_conn = _get_mysql()
        try:
            with my_conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(usdamount), 0)
                    FROM broker_banking
                    WHERE confirmation_time >= %s
                      AND confirmation_time <  %s
                      AND statusmapping = 'Approved'
                      AND transactiontype = 'Deposit'
                """, (date_from, tomorrow_str))
                mysql_dep_sum = float(cur.fetchone()[0] or 0)
        finally:
            my_conn.close()
    except Exception as e:
        results.append(_flag("Transaction Sum", "tx_sum_crosscheck", "Grand Total",
                             STATUS["ERROR"], f"MySQL deposit sum query failed: {e}"))

    if pg_dep_sum is not None and mysql_dep_sum is not None:
        results.append(_ok("Transaction Sum", "tx_sum_crosscheck", "Grand Total",
                           mysql_dep_sum, pg_dep_sum, tol=0.005))

    # ── 4. Targets cross-check: PostgreSQL vs MSSQL ─────────────────────────
    from datetime import date as date_type
    import calendar
    dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    last_day = dt_from.replace(day=calendar.monthrange(dt_from.year, dt_from.month)[1])
    last_day_str = last_day.strftime("%Y-%m-%d")

    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT COALESCE(SUM(net), 0)
                FROM targets
                WHERE date >= %(date_from)s AND date <= %(last_day)s
            """, {"date_from": date_from, "last_day": last_day_str})
            pg_targets_sum = float(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            pg_targets_sum = None
            results.append(_flag("Targets", "targets_crosscheck", "Grand Total",
                                 STATUS["ERROR"], f"PG targets sum query failed: {e}"))

    mssql_targets_sum = None
    try:
        ms_conn = _get_mssql()
        try:
            with ms_conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(net), 0)
                    FROM report.target
                    WHERE date >= %s AND date <= %s
                """, (date_from, last_day_str))
                row = cur.fetchone()
                mssql_targets_sum = float(row[0] or 0) if row else 0.0
        finally:
            ms_conn.close()
    except Exception as e:
        results.append(_flag("Targets", "targets_crosscheck", "Grand Total",
                             STATUS["ERROR"], f"MSSQL targets sum query failed: {e}"))

    if pg_targets_sum is not None and mssql_targets_sum is not None:
        results.append(_ok("Targets", "targets_crosscheck", "Grand Total",
                           mssql_targets_sum, pg_targets_sum, tol=0.0))

    # ── 5. CRM users count cross-check: PostgreSQL vs MySQL ─────────────────
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM crm_users WHERE is_active = 1")
            pg_crm_count = int(cur.fetchone()[0] or 0)
        except Exception as e:
            conn.rollback()
            try:
                cur.execute("SELECT COUNT(*) FROM crm_users")
                pg_crm_count = int(cur.fetchone()[0] or 0)
            except Exception as e2:
                conn.rollback()
                pg_crm_count = None
                results.append(_flag("CRM Users", "crm_users_crosscheck", "Grand Total",
                                     STATUS["ERROR"], f"PG crm_users count failed: {e2}"))

    mysql_crm_count = None
    try:
        my_conn = _get_mysql()
        try:
            with my_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM v_ant_operators
                    WHERE status = 'Active'
                """)
                mysql_crm_count = int(cur.fetchone()[0] or 0)
        finally:
            my_conn.close()
    except Exception as e:
        results.append(_flag("CRM Users", "crm_users_crosscheck", "Grand Total",
                             STATUS["ERROR"], f"MySQL crm_users count failed: {e}"))

    if pg_crm_count is not None and mysql_crm_count is not None:
        try:
            diff = abs(pg_crm_count - mysql_crm_count)
            pct  = diff / mysql_crm_count if mysql_crm_count > 0 else 0
            st   = STATUS["PASS"] if pct <= 0.05 else STATUS["WARN"]
            results.append(_flag("CRM Users", "crm_users_crosscheck", "Grand Total",
                                 st,
                                 f"PG={pg_crm_count}, MySQL={mysql_crm_count}, diff={diff} ({round(pct*100,1)}%)",
                                 mysql_crm_count, pg_crm_count))
        except Exception as e:
            results.append(_flag("CRM Users", "crm_users_crosscheck", "Grand Total",
                                 STATUS["ERROR"], f"CRM users comparison failed: {e}"))

    return results
