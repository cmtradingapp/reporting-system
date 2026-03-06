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


_DEALIO_EXCLUDED_SYMBOLS = (
    "'ZeroingZAR','ZeroingUSD','ZeroingNGN','ZeroingKES',"
    "'ZeroingJPY','ZeroingGBP','ZeroingEUR'"
)


def get_dealio_mt4trades(hours: int = 24) -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = f"""
            SELECT * FROM report.dealio_mt4trades
            WHERE symbol NOT IN ({_DEALIO_EXCLUDED_SYMBOLS})
              AND (last_modified >= DATEADD(hour, -{hours}, GETUTCDATE())
               OR  updated_at    >= DATEADD(hour, -{hours}, GETUTCDATE()))
        """
        df = pd.read_sql(query, conn)
        return _normalize_dealio_cols(df)
    finally:
        conn.close()


def get_dealio_mt4trades_full():
    """
    Generator using keyset pagination on ticket (clustered PK).
    Opens a fresh connection per chunk — safe for 10M+ rows regardless
    of pymssql client-side buffering behaviour.
    """
    last_ticket = 0
    while True:
        conn = _get_mssql_connection()
        try:
            query = f"""
                SELECT TOP {CHUNK_SIZE} *
                FROM report.dealio_mt4trades
                WHERE ticket > {last_ticket}
                  AND symbol NOT IN ({_DEALIO_EXCLUDED_SYMBOLS})
                ORDER BY ticket
            """
            df = pd.read_sql(query, conn)
        finally:
            conn.close()

        if df.empty:
            break

        df = _normalize_dealio_cols(df)
        last_ticket = int(df["ticket"].max())
        yield df
