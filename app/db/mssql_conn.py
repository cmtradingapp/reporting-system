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


def get_vtiger_users() -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = """
            SELECT
                id, email, full_name, status, first_name, last_name,
                role_id, desk_id, language, last_logon_time, last_update_time,
                desk_name, team, department, desk, type, office_id, office, position,
                full_name AS agent_name,
                CASE
                    WHEN office IS NULL OR LTRIM(RTRIM(office)) = '' OR office = 'General' THEN 'General'
                    WHEN desk_name = 'Laila Desk' THEN 'Laila'
                    WHEN office = 'IN'          THEN 'India'
                    WHEN office = 'UY'          THEN 'Uruguay'
                    WHEN office = 'SA'          THEN 'South Africa'
                    WHEN office = 'LAG-NG'      THEN 'LAG Nigeria'
                    WHEN office = 'IL'          THEN 'Israel'
                    WHEN office = 'GMT'         THEN 'GMT'
                    WHEN office = 'Global'      THEN 'General'
                    WHEN office = 'DU'          THEN 'Dubai'
                    WHEN office = 'CO'          THEN 'Columbia'
                    WHEN office = 'CY'          THEN 'Cyprus'
                    WHEN office = 'BG'          THEN 'Bulgaria'
                    WHEN office = 'ABJ-NG'      THEN 'ABJ Nigeria'
                    WHEN office = 'WL-BG'       THEN 'WL Bulgaria'
                    WHEN office = 'WL-PK'       THEN 'WL Pakistan'
                    WHEN office = 'WL-SL'       THEN 'WL Sri Lanka'
                    WHEN office = 'WL-IL'       THEN 'WL IL'
                    WHEN office = 'VN'          THEN 'Vietnam'
                    WHEN office = 'WL-Belgrad'  THEN 'WL Belgrad'
                    WHEN office = 'WL-SNS-UAW'  THEN 'WL UAE'
                    WHEN office = 'WL-ABUKING'  THEN 'WL ABUKING'
                    ELSE office
                END AS office_name,
                CASE
                    WHEN LOWER(ISNULL(team, ''))       LIKE '%conversion%'
                      OR LOWER(ISNULL(department, '')) LIKE '%conversion%'
                    THEN 'Sales' ELSE 'Retention'
                END AS department_
            FROM report.vtiger_users
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


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
