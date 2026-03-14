import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.db.mysql_conn import get_operators, get_users, get_accounts, get_accounts_full, get_crm_users, get_crm_users_full, get_transactions, get_transactions_full, get_trading_accounts, get_trading_accounts_full
from app.db.mssql_conn import get_targets, get_dealio_mt4trades, get_dealio_mt4trades_full, get_vtiger_users, get_dealio_daily_profit, get_dealio_daily_profit_full, get_client_classification
from app.db.dealio_conn import get_dealio_users, get_dealio_users_full, get_dealio_trades_mt4, get_dealio_trades_mt4_full
from app.db.postgres_conn import (
    ensure_table, delete_all_performance, insert_records,
    upsert_users, upsert_accounts, cleanup_accounts, upsert_crm_users, truncate_crm_users, upsert_transactions,
    upsert_targets, upsert_dealio_mt4trades, upsert_trading_accounts, log_sync,
    truncate_and_insert_ftd100, upsert_dealio_daily_profit,
    ensure_client_classification_table, upsert_client_classification,
    upsert_dealio_users, upsert_dealio_trades_mt4,
)


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


def run_dealio_mt4trades_etl(hours: int = 24) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_mt4trades(hours=hours)
        rows = len(df)
        upsert_dealio_mt4trades(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_mt4trades", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_mt4trades_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    chunk_num = 0
    try:
        for chunk in get_dealio_mt4trades_full():
            upsert_dealio_mt4trades(chunk)
            rows += len(chunk)
            chunk_num += 1
            if chunk_num % 10 == 0:
                elapsed = int((time.time() - start) * 1000)
                log_sync("dealio_mt4trades", cutoff, rows, elapsed, "running", f"chunk {chunk_num}, {rows} rows so far")
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_mt4trades", cutoff, rows, duration_ms, status, error_msg)
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


def run_ftd100_etl() -> dict:
    """Full refresh — TRUNCATE + INSERT computed from transactions + accounts CTEs."""
    start = time.time()
    cutoff = datetime(1970, 1, 1)  # full refresh marker
    status = "success"
    error_msg = None
    rows = 0
    try:
        rows = truncate_and_insert_ftd100()
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
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_dealio_daily_profit_etl(hours: int = 48) -> dict:
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_dealio_daily_profit(hours=hours)
        rows = len(df)
        upsert_dealio_daily_profit(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_daily_profit", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_dealio_daily_profit_full_etl() -> dict:
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        for chunk in get_dealio_daily_profit_full():
            upsert_dealio_daily_profit(chunk)
            rows += len(chunk)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("dealio_daily_profit", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


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
