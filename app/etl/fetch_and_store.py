import pandas as pd
from app.db.mysql_conn import get_operators, get_users
from app.db.mssql_conn import get_targets
from app.db.postgres_conn import (
    ensure_table, delete_all_performance, insert_records, upsert_users
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
