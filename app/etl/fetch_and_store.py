import pandas as pd
from app.db.mysql_conn import get_operators
from app.db.mssql_conn import get_targets
from app.db.postgres_conn import ensure_table, delete_current_month, insert_records


def run_etl() -> dict:
    ensure_table()

    # 1. Fetch from MySQL
    operators_df = get_operators()
    operators_df.rename(columns={"id": "agent_id"}, inplace=True)
    operators_df["agent_id"] = operators_df["agent_id"].astype(str)

    # 2. Fetch from MSSQL (current month only)
    targets_df = get_targets()
    targets_df["agent_id"] = targets_df["agent_id"].astype(str)

    # 3. Join on agent_id
    merged_df = targets_df.merge(operators_df, on="agent_id", how="left")
    merged_df["full_name"] = merged_df["full_name"].fillna("Unknown")

    # 4. Clear current month data in Postgres and re-insert
    delete_current_month()
    insert_records(merged_df)

    return {
        "status": "success",
        "operators_fetched": len(operators_df),
        "target_rows_fetched": len(targets_df),
        "rows_stored": len(merged_df),
    }
