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
                id,
                user_name                           AS full_name,
                user_name                           AS agent_name,
                first_name,
                last_name,
                email,
                status,
                last_login                          AS last_logon_time,
                lastupdated                         AS last_update_time,
                department                          AS desk_name,
                department                          AS desk,
                fax                                 AS team,
                department,
                language,
                position,
                office,
                NULL                                AS role_id,
                NULL                                AS desk_id,
                NULL                                AS type,
                NULL                                AS office_id,
                CASE
                    WHEN office IS NULL OR LTRIM(RTRIM(office)) IN ('', 'General') THEN 'General'
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
                    WHEN LOWER(ISNULL(fax, '')) LIKE '%conversion%'
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


def get_dealio_daily_profit(hours: int = 48) -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = f"""
            SELECT date, sourceid, sourcename, sourcetype, book,
                   closedpnl, convertedclosedpnl, calculationcurrency,
                   floatingpnl, convertedfloatingpnl, netdeposit, convertednetdeposit,
                   equity, convertedequity, login, balance, convertedbalance,
                   groupcurrency, conversionratio, equityprevday, groupname,
                   deltafloatingpnl, converteddeltafloatingpnl, assigned_to
            FROM report.dealio_daily_profit
            WHERE date >= DATEADD(hour, -{hours}, GETUTCDATE())
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_dealio_daily_profit_full():
    """Generator using keyset pagination on (date, login)."""
    last_date = "1970-01-01"
    last_login = 0
    while True:
        conn = _get_mssql_connection()
        try:
            query = f"""
                SELECT TOP {CHUNK_SIZE}
                       date, sourceid, sourcename, sourcetype, book,
                       closedpnl, convertedclosedpnl, calculationcurrency,
                       floatingpnl, convertedfloatingpnl, netdeposit, convertednetdeposit,
                       equity, convertedequity, login, balance, convertedbalance,
                       groupcurrency, conversionratio, equityprevday, groupname,
                       deltafloatingpnl, converteddeltafloatingpnl, assigned_to
                FROM report.dealio_daily_profit
                WHERE date > '{last_date}'
                   OR (date = '{last_date}' AND login > {last_login})
                ORDER BY date, login
            """
            df = pd.read_sql(query, conn)
        finally:
            conn.close()

        if df.empty:
            break

        last_date = str(df["date"].max())[:10]
        last_login = int(df["login"].max())
        yield df


def get_pnl_cash_monthly(month_start: str, month_end_exclusive: str) -> float:
    """Sum convertedclosedpnl + converteddeltafloatingpnl from MSSQL for the given date range."""
    conn = _get_mssql_connection()
    try:
        query = f"""
            SELECT COALESCE(SUM(COALESCE(convertedclosedpnl, 0) + COALESCE(converteddeltafloatingpnl, 0)), 0)
            FROM report.dealio_daily_profit
            WHERE CAST(date AS DATE) >= '{month_start}'
              AND CAST(date AS DATE) < '{month_end_exclusive}'
        """
        df = pd.read_sql(query, conn)
        return float(df.iloc[0, 0] or 0)
    finally:
        conn.close()


def get_client_classification() -> pd.DataFrame:
    conn = _get_mssql_connection()
    try:
        query = """
            SELECT
                CAST(accountid AS BIGINT) AS accountid,
                client_classification
            FROM dbo.client_classification_date
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_country_map() -> dict:
    """Returns dict of iso2code (uppercase) → country name from report.countries."""
    conn = _get_mssql_connection()
    try:
        query = "SELECT iso2code, name FROM report.countries WHERE iso2code IS NOT NULL AND iso2code <> ''"
        df = pd.read_sql(query, conn)
        return {str(r['iso2code']).strip().upper(): str(r['name']).strip() for _, r in df.iterrows()}
    except Exception:
        return {}
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


def get_bonus_transactions(hours: int = 24) -> pd.DataFrame:
    """Fetch is_old_bonus transactions modified in the last N hours from MSSQL."""
    conn = _get_mssql_connection()
    try:
        query = f"""
            SELECT mttransactionsid,
                   CAST(login AS BIGINT) AS login,
                   CASE WHEN transactiontype = 'Deposit'    THEN  usdamount
                        WHEN transactiontype = 'Withdrawal' THEN -usdamount
                   END AS net_amount,
                   confirmation_time
            FROM report.vtiger_mttransactions
            WHERE ((transactiontype = 'Deposit'    AND transaction_type_name IN ('FRF Commission', 'Bonus'))
                OR (transactiontype = 'Withdrawal' AND transaction_type_name IN ('FRF Commission Cancelled', 'BonusCancelled')))
              AND transactionapproval = 'Approved'
              AND (deleted = 0 OR deleted IS NULL)
              AND (modifiedtime        >= DATEADD(hour, -{hours}, GETUTCDATE())
                OR confirmation_time  >= DATEADD(hour, -{hours}, GETUTCDATE()))
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_bonus_transactions_full():
    """Generator yielding all is_old_bonus transactions from MSSQL in chunks."""
    last_id = 0
    while True:
        conn = _get_mssql_connection()
        try:
            query = f"""
                SELECT TOP {CHUNK_SIZE}
                       mttransactionsid,
                       CAST(login AS BIGINT) AS login,
                       CASE WHEN transactiontype = 'Deposit'    THEN  usdamount
                            WHEN transactiontype = 'Withdrawal' THEN -usdamount
                       END AS net_amount,
                       confirmation_time
                FROM report.vtiger_mttransactions
                WHERE ((transactiontype = 'Deposit'    AND transaction_type_name IN ('FRF Commission', 'Bonus'))
                    OR (transactiontype = 'Withdrawal' AND transaction_type_name IN ('FRF Commission Cancelled', 'BonusCancelled')))
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
                  AND mttransactionsid > {last_id}
                ORDER BY mttransactionsid
            """
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
        if df.empty:
            break
        last_id = int(df["mttransactionsid"].max())
        yield df


def get_transaction_type_names_full():
    """Yields DataFrames of (mttransactionsid, transaction_type_name) from MSSQL in chunks."""
    last_id = 0
    while True:
        conn = _get_mssql_connection()
        try:
            query = f"""
                SELECT TOP {CHUNK_SIZE} mttransactionsid, transaction_type_name
                FROM report.vtiger_mttransactions
                WHERE mttransactionsid > {last_id}
                ORDER BY mttransactionsid
            """
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
        if df.empty:
            break
        last_id = int(df["mttransactionsid"].max())
        yield df


def get_transaction_type_names_for_ids(ids: list) -> pd.DataFrame:
    """Fetch transaction_type_name from MSSQL for specific mttransactionsids."""
    ids = [int(i) for i in ids if i is not None]
    if not ids:
        return pd.DataFrame(columns=["mttransactionsid", "transaction_type_name"])
    # Split into chunks of 2000 to avoid SQL IN clause limits
    results = []
    for i in range(0, len(ids), 2000):
        chunk_ids = ids[i:i + 2000]
        id_str = ",".join(str(i) for i in chunk_ids)
        conn = _get_mssql_connection()
        try:
            query = f"""
                SELECT mttransactionsid, transaction_type_name
                FROM report.vtiger_mttransactions
                WHERE mttransactionsid IN ({id_str})
            """
            results.append(pd.read_sql(query, conn))
        finally:
            conn.close()
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame(columns=["mttransactionsid", "transaction_type_name"])
