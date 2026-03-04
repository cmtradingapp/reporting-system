import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.db.mysql_conn import get_operators, get_users, get_accounts, get_accounts_full, get_crm_users, get_crm_users_full, get_transactions, get_transactions_full
from app.db.mssql_conn import get_targets
from app.db.postgres_conn import (
    ensure_table, delete_all_performance, insert_records,
    upsert_users, upsert_accounts, upsert_crm_users, upsert_transactions, log_sync
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
    ensure_table()
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        accounts_df = get_accounts(hours=hours)
        rows = len(accounts_df)
        upsert_accounts(accounts_df)
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
    ensure_table()
    start = time.time()
    cutoff = datetime(1970, 1, 1)  # epoch — indicates full sync in log
    status = "success"
    error_msg = None
    rows = 0
    try:
        accounts_df = get_accounts_full()
        rows = len(accounts_df)
        upsert_accounts(accounts_df)
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
        "type": "full",
    }


def run_users_etl(hours: int = 24) -> dict:
    ensure_table()
    start = time.time()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_crm_users(hours=hours)
        rows = len(df)
        upsert_crm_users(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "lookback_hours": hours}


def run_users_full_etl() -> dict:
    ensure_table()
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_crm_users_full()
        rows = len(df)
        upsert_crm_users(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("crm_users", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}


def run_transactions_etl(hours: int = 24) -> dict:
    ensure_table()
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


def run_transactions_full_etl() -> dict:
    ensure_table()
    start = time.time()
    cutoff = datetime(1970, 1, 1)
    status = "success"
    error_msg = None
    rows = 0
    try:
        df = get_transactions_full()
        rows = len(df)
        upsert_transactions(df)
    except Exception as e:
        status = "error"
        error_msg = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log_sync("transactions", cutoff, rows, duration_ms, status, error_msg)
    return {"status": status, "rows_synced": rows, "type": "full"}
