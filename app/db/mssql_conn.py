import pymssql
import pandas as pd
from app.config import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DB

CHUNK_SIZE = 50000


def _get_mssql_connection():
    return pymssql.connect(
        server=MSSQL_HOST,
        port=str(MSSQL_PORT),
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DB,
        tds_version="7.4",
        conn_properties="",
    )


def _normalize_dealio_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the ambiguous computed_agent_commission column name."""
    for col in list(df.columns):
        if col in ("computed_agent_commission", "computer_agent_commission"):
            df = df.rename(columns={col: "computed_agent_commission"})
            break
    return df


def get_targets() -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = "SELECT date, agent_id, ftc, net FROM report.target"
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_dealio_mt4trades(hours: int = 24) -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = f"""
            SELECT * FROM report.dealio_mt4trades
            WHERE last_modified >= DATEADD(hour, -{hours}, GETUTCDATE())
               OR updated_at    >= DATEADD(hour, -{hours}, GETUTCDATE())
        """
        df = pd.read_sql(query, conn)
        return _normalize_dealio_cols(df)
    finally:
        conn.close()


def get_dealio_mt4trades_full():
    """Generator yielding chunks of all rows — avoids OOM for large tables."""
    conn = _get_mssql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM report.dealio_mt4trades")
        cols = [d[0] for d in cursor.description]
        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=cols)
            yield _normalize_dealio_cols(df)
    finally:
        conn.close()
