import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.db.mysql_conn import get_operators, get_users, get_accounts, get_accounts_full, get_accounts_by_qual_date, get_accounts_by_created_date, get_crm_users, get_crm_users_full, get_transactions, get_transactions_full, get_transactions_by_confirmation_date, get_trading_accounts, get_trading_accounts_full, get_campaigns
from app.db.mssql_conn import get_targets, get_vtiger_users, get_client_classification, get_bonus_transactions, get_bonus_transactions_full
from app.db.dealio_conn import get_dealio_users, get_dealio_users_full, get_dealio_trades_mt4, get_dealio_trades_mt4_full, get_dealio_trades_mt4_missing, get_dealio_trades_mt4_by_open_time, get_dealio_daily_profits, get_dealio_daily_profits_full, get_dealio_daily_profits_daterange, get_dealio_trades_mt5, get_dealio_trades_mt5_full, get_dealio_trades_mt5_missing
from app.db.postgres_conn import (
    ensure_table, delete_all_performance, insert_records,
    upsert_users, upsert_accounts, cleanup_accounts, upsert_crm_users, truncate_crm_users, upsert_transactions,
    upsert_targets, upsert_trading_accounts, log_sync,
    truncate_and_insert_ftd100, compute_transaction_type_name,
    ensure_client_classification_table, upsert_client_classification,
    upsert_dealio_users, upsert_dealio_trades_mt4, truncate_dealio_trades_mt4,
    upsert_dealio_trades_mt5, truncate_dealio_trades_mt5,
    upsert_dealio_daily_profits,
    ensure_bonus_transactions_table, upsert_bonus_transactions,
    ensure_daily_equity_zeroed_table, upsert_daily_equity_zeroed,
    upsert_campaigns,
)
from app.db.postgres_conn import get_connection as _pg_conn

# Global lock: set to True while rebuild is running so the incremental scheduler skips
_dealio_trades_mt4_rebuilding = False


def run_etl() -> dict:
    ensure_table()

    # === USERS: fetch from MySQL and upsert into PostgreSQL ===
    users_df = get_users()
    upsert_users(users_df)

    # === AGENT PERFORMANCE: fetch from MySQL + MSSQL, store in PostgreSQL ===
    operators_df = get_operators()
    operators_df.rename(columns={"id": "agent_id"}, inplace=True)
    operators_df["agent_id"] = operators_df["agent_id"].astype(str)

    targets_df = get_targets()
    targets_df["agent_id"] = targets_df["agent_id"].astype(str)

    merged_df = targets_df.merge(operators_df, on="agent_id", how="left")
    merged_df["full_name"] = merged_df["full_name"].fillna("Unknown")

    delete_all_performance()
    insert_records(merged_df)

    return {
        "status": "success",
        "users_synced": len(users_df),
        "operators_fetched": len(operators_df),
        "target_rows_fetched": len(targets_df),
        "rows_stored": len(merged_df),
    }


def run_accounts_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        accounts_df = get_accounts(hours=hours)
        rows = len(accounts_df)
        upsert_accounts(accounts_df)
        cleanup_accounts()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {
        "status": status,
        "accounts_synced": rows,
        "lookback_hours": hours,
    }


def run_accounts_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_accounts_full():
            upsert_accounts(chunk)
            rows += len(chunk)
        cleanup_accounts()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "accounts_synced": rows, "type": "full"}


def run_accounts_by_qual_date_etl(from_date: str) -> dict:
    start = time.time()
    cutoff = datetime.strptime(from_date, "%Y-%m-%d")
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_accounts_by_qual_date(from_date)
        rows = len(df)
        upsert_accounts(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "accounts_synced": rows, "from_date": from_date}


def run_accounts_by_created_date_etl(from_date: str) -> dict:
    start = time.time()
    cutoff = datetime.strptime(from_date, "%Y-%m-%d")
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_accounts_by_created_date(from_date)
        rows = len(df)
        upsert_accounts(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "accounts_synced": rows, "from_date": from_date}


def run_transactions_by_confirmation_date_etl(from_date: str) -> dict:
    start = time.time()
    cutoff = datetime.strptime(from_date, "%Y-%m-%d")
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_transactions_by_confirmation_date(from_date)
        rows = len(df)
        upsert_transactions(df)
        if rows > 0:
            ids = df["mttransactionsid"].dropna().astype(int).tolist()
            compute_transaction_type_name(ids)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "transactions_synced": rows, "from_date": from_date}


def run_users_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_vtiger_users()
        rows = len(df)
        upsert_crm_users(df)
        from app.db.postgres_conn import sync_auth_users_from_crm
        sync_auth_users_from_crm()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_users_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_vtiger_users()
        rows = len(df)
        truncate_crm_users()
        upsert_crm_users(df)
        from app.db.postgres_conn import sync_auth_users_from_crm
        sync_auth_users_from_crm()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_transactions_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_transactions(hours=hours)
        rows = len(df)
        upsert_transactions(df)
        if rows > 0:
            ids = df["mttransactionsid"].dropna().astype(int).tolist()
            compute_transaction_type_name(ids)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_trading_accounts_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_trading_accounts(hours=hours)
        rows = len(df)
        upsert_trading_accounts(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("trading_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_trading_accounts_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_trading_accounts_full():
            upsert_trading_accounts(chunk)
            rows += len(chunk)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("trading_accounts", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_targets_etl() -> dict:
    """Full refresh — report.target has no modification timestamp column."""
    start = time.time()
    cutoff = datetime(1970, 1, 1)  # epoch = full sync marker
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_targets()
        df["agent_id"] = df["agent_id"].astype(str)
        rows = len(df)
        upsert_targets(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("targets", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows}


def _refresh_sales_bonuses_mv() -> None:
    """Refresh only mv_sales_bonuses after ftd100_clients rebuild."""
    conn = _pg_conn()
    try:
        try:
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_bonuses")
            conn.commit()
        except Exception:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW mv_sales_bonuses")
            conn.commit()
    except Exception as e:
        print(f"[ftd100_etl] mv_sales_bonuses refresh error: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_ftd100_etl() -> dict:
    """Full refresh — TRUNCATE + INSERT computed from transactions + accounts CTEs."""
    from app import cache
    start = time.time()
    cutoff = datetime(1970, 1, 1)  # full refresh marker
    status = "success"
    error_msg = None
    rows = 0
    try:
        rows = truncate_and_insert_ftd100()
        # Immediately refresh mv_sales_bonuses so it's never stale after rebuild
        _refresh_sales_bonuses_mv()
        # Clear sales bonus cache so next request gets fresh data
        cache.invalidate_all()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("ftd100_clients", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows}


def run_transactions_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_transactions_full():
            upsert_transactions(chunk)
            rows += len(chunk)
        compute_transaction_type_name()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_transaction_type_names_backfill_etl() -> dict:
    """Recompute transaction_type_name for all transactions using local CASE logic."""
    start = time.time()
    status = "success"
    error_msg = None
    rows = 0
    try:
        rows = compute_transaction_type_name()
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transaction_type_names", datetime(1970, 1, 1), rows, duration_ms, status, error_msg)
    return {"status": status, "rows_updated": rows, "type": "full_backfill"}


def run_dealio_users_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_users(hours=hours)
        rows = len(df)
        upsert_dealio_users(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_users_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_dealio_users_full():
            for attempt in range(3):
                try:
                    upsert_dealio_users(chunk)
                    break
                except Exception as e:
                    if "deadlock" in str(e).lower() and attempt < 2:
                        time.sleep(5)
                        continue
                    raise
            rows += len(chunk)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_dealio_trades_mt4_etl(hours: int = 24) -> dict:
    global _dealio_trades_mt4_rebuilding
    if _dealio_trades_mt4_rebuilding:
        return {"status": "skipped", "reason": "rebuild in progress"}
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_trades_mt4(hours=hours)
        rows = len(df)
        upsert_dealio_trades_mt4(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_trades_mt4_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        for chunk in get_dealio_trades_mt4_full():
            upsert_dealio_trades_mt4(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt4", cutoff, rows, elapsed, "running", f"chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_dealio_trades_mt4_missing_etl() -> dict:
    """Sync only rows with ticket > max(ticket) in local DB — adds missing rows without re-processing existing ones."""
    from app.db.postgres_conn import get_connection
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(ticket), 0) FROM dealio_trades_mt4")
                max_ticket = int(cur.fetchone()[0])
        finally:
            conn.close()
        for chunk in get_dealio_trades_mt4_missing(max_ticket):
            upsert_dealio_trades_mt4(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt4", cutoff, rows, elapsed, "running", f"missing: chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "missing", "start_ticket": max_ticket}


def run_dealio_trades_mt4_rebuild_etl() -> dict:
    """Drop all rows and re-sync everything from source. Blocks incremental scheduler while running."""
    global _dealio_trades_mt4_rebuilding
    _dealio_trades_mt4_rebuilding = True
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        truncate_dealio_trades_mt4()
        for chunk in get_dealio_trades_mt4_full():
            upsert_dealio_trades_mt4(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt4", cutoff, rows, elapsed, "running", f"rebuild: chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        _dealio_trades_mt4_rebuilding = False
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "rebuild"}


def run_dealio_trades_mt4_refresh_notional_etl(hours: int = 2160) -> dict:
    """Re-sync last N hours of trades to populate notional_value on existing rows (default 90 days)."""
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_trades_mt4(hours=hours)
        rows = len(df)
        upsert_dealio_trades_mt4(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "notional_refresh", "lookback_hours": hours}


def run_dealio_trades_mt4_by_open_time_etl(from_date: str = "2026-01-01") -> None:
    """Re-sync dealio_trades_mt4 for trades with open_time >= from_date (UTC+2 after conversion).
    Used after the timezone fix to correct stored open_time/close_time values."""
    global _dealio_trades_mt4_rebuilding
    start = time.time()
    cutoff = datetime.strptime(from_date, "%Y-%m-%d")
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        for chunk in get_dealio_trades_mt4_by_open_time(from_date):
            upsert_dealio_trades_mt4(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt4", cutoff, rows, elapsed, "running", f"by_open_time: chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt4", cutoff, rows, duration_ms, status, error_msg)


def run_client_classification_etl() -> dict:
    ensure_client_classification_table()
    start = time.time()
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_client_classification()
        rows = len(df)
        upsert_client_classification(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("client_classification", None, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows}


def run_dealio_daily_profits_etl(hours: int = 48) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_daily_profits(hours=hours)
        rows = len(df)
        upsert_dealio_daily_profits(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_daily_profits", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_daily_profits_daterange_etl(date_from: str, date_to: str) -> dict:
    start = time.time()
    cutoff = datetime.strptime(date_from, "%Y-%m-%d")
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_dealio_daily_profits_daterange(date_from, date_to):
            upsert_dealio_daily_profits(chunk)
            rows += len(chunk)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_daily_profits", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "date_from": date_from, "date_to": date_to}


def run_dealio_daily_profits_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        for chunk in get_dealio_daily_profits_full():
            upsert_dealio_daily_profits(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_daily_profits", cutoff, rows, elapsed, "running", f"chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_daily_profits", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_bonus_transactions_etl(hours: int = 24) -> dict:
    ensure_bonus_transactions_table()
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_bonus_transactions(hours=hours)
        rows = len(df)
        upsert_bonus_transactions(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("bonus_transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_bonus_transactions_full_etl() -> dict:
    ensure_bonus_transactions_table()
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_bonus_transactions_full():
            upsert_bonus_transactions(chunk)
            rows += len(chunk)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("bonus_transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_daily_equity_zeroed_snapshot(snapshot_date: str = None) -> dict:
    """
    Calculate end_equity_zeroed (for snapshot_date) and start_equity_zeroed
    (independently calculated from snapshot_date - 1 using the same EEZ formula),
    then upsert both into daily_equity_zeroed.
    """
    from datetime import date, timedelta, datetime as dt
    if snapshot_date is None:
        snapshot_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    prev_date = (dt.strptime(snapshot_date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Reusable EEZ query — parameterised by %(d)s so it works for both snapshot_date and prev_date
    sql_eez = """
        WITH latest_equity AS (
            SELECT DISTINCT ON (login)
                login, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %(d)s
            ORDER BY login, date DESC
        ),
        bonus_bal AS (
            SELECT login, SUM(net_amount) AS bonus_balance
            FROM bonus_transactions
            WHERE confirmation_time::date <= %(d)s
            GROUP BY login
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login, MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
            GROUP BY ta.login::bigint
        )
        SELECT
            le.login,
            ROUND(CASE
                WHEN COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0) <= 0 THEN 0
                ELSE GREATEST(
                    COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0)
                        - COALESCE(b.bonus_balance, 0),
                    0
                )
            END::numeric, 2) AS eez
        FROM latest_equity le
        LEFT JOIN bonus_bal b  ON b.login = le.login
        JOIN test_flags tf ON tf.login = le.login
        WHERE tf.is_test = 0
    """

    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            # End EEZ: snapshot_date
            cur.execute(sql_eez, {"d": snapshot_date})
            end_rows = cur.fetchall()
            # Start EEZ: snapshot_date - 1 (same formula, date shifted back 1 day)
            cur.execute(sql_eez, {"d": prev_date})
            start_rows = cur.fetchall()
    finally:
        conn.close()

    start_map = {login: eez for login, eez in start_rows}
    combined = [(login, end_eez, start_map.get(login)) for login, end_eez in end_rows]

    upsert_daily_equity_zeroed(combined, snapshot_date)
    return {"status": "success", "snapshot_date": snapshot_date, "rows": len(combined)}


def run_dealio_trades_mt5_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_trades_mt5(hours=hours)
        rows = len(df)
        upsert_dealio_trades_mt5(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt5", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_trades_mt5_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        for chunk in get_dealio_trades_mt5_full():
            upsert_dealio_trades_mt5(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt5", cutoff, rows, elapsed, "running", f"chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt5", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_dealio_trades_mt5_missing_etl() -> dict:
    """Sync only rows with ticket > max(ticket) in local DB."""
    from app.db.postgres_conn import get_connection
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    max_ticket = 0
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(ticket), 0) FROM dealio_trades_mt5")
                max_ticket = int(cur.fetchone()[0])
        finally:
            conn.close()
        for chunk in get_dealio_trades_mt5_missing(max_ticket):
            upsert_dealio_trades_mt5(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt5", cutoff, rows, elapsed, "running", f"missing: chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt5", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "missing", "start_ticket": max_ticket}


def run_dealio_trades_mt5_rebuild_etl() -> dict:
    """Truncate and re-sync all MT5 trades from source."""
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        truncate_dealio_trades_mt5()
        for chunk in get_dealio_trades_mt5_full():
            upsert_dealio_trades_mt5(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_trades_mt5", cutoff, rows, elapsed, "running", f"rebuild: chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_trades_mt5", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "rebuild"}


def run_campaigns_etl() -> dict:
    """Full refresh — campaigns table has no modification timestamp."""
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_campaigns()
        rows = len(df)
        upsert_campaigns(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("campaigns", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows}
