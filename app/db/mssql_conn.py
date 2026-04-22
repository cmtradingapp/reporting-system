import pymssql
import pandas as pd
from app.config import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DB

CHUNK_SIZE = 50000


def _get_mssql_connection(timeout=300):
    return pymssql.connect(
        server=MSSQL_HOST,
        port=str(MSSQL_PORT),
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DB,
        tds_version="7.4",
        conn_properties="",
        login_timeout=30,
        timeout=timeout,
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


def get_country_region_map() -> dict:
    """Returns dict of iso2code (uppercase) → region from report.countries."""
    conn = _get_mssql_connection()
    try:
        query = "SELECT iso2code, region FROM report.countries WHERE iso2code IS NOT NULL AND iso2code <> '' AND region IS NOT NULL AND region <> ''"
        df = pd.read_sql(query, conn)
        return {str(r['iso2code']).strip().upper(): str(r['region']).strip() for _, r in df.iterrows()}
    except Exception:
        return {}
    finally:
        conn.close()


def get_ret_status_map() -> dict:
    """Returns dict of status_key (str) → value from report.ant_ret_status."""
    conn = _get_mssql_connection()
    try:
        df = pd.read_sql("SELECT status_key, value FROM report.ant_ret_status", conn)
        return {str(int(r['status_key'])): str(r['value']).strip() for _, r in df.iterrows()}
    except Exception:
        return {}
    finally:
        conn.close()


def get_sales_status_map() -> dict:
    """Returns dict of status_key (str) → value from report.ant_sales_status."""
    conn = _get_mssql_connection()
    try:
        df = pd.read_sql("SELECT status_key, value FROM report.ant_sales_status", conn)
        return {str(int(r['status_key'])): str(r['value']).strip() for _, r in df.iterrows()}
    except Exception:
        return {}
    finally:
        conn.close()


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


def get_mssql_dealio_mt5trades_full(start_ticket: int = 0):
    """Generator yielding all rows from report.dealio_mt5trades in chunks, paginated by ticket."""
    _BIG_CHUNK = 500000  # larger chunks to reduce round-trips on this 17M+ row table
    last_ticket = start_ticket
    while True:
        conn = _get_mssql_connection(timeout=600)
        try:
            query = f"""
                SELECT TOP {_BIG_CHUNK}
                    ticket, login, symbol, digit, cmd, volume,
                    opentime        AS open_time,
                    openprice       AS open_price,
                    closetime       AS close_time,
                    reason, commission,
                    agentid         AS agent_id,
                    swap,
                    closeprice      AS close_price,
                    profit, tax, comment,
                    timestamp       AS mssql_timestamp,
                    symbolplain     AS symbol_plain,
                    computedprofit  AS computed_profit,
                    computedswap    AS computed_swap,
                    computedcommission AS computed_commission,
                    groupname       AS group_name,
                    groupcurrency   AS group_currency,
                    calculationcurrency AS calculation_currency,
                    book,
                    notionalvalue   AS notional_value,
                    sourcename      AS source_name,
                    sourcetype      AS source_type,
                    sourceid        AS source_id,
                    positionid      AS position_id,
                    entry,
                    volumeclosed    AS volume_closed,
                    synctime        AS sync_time,
                    isfinalized     AS is_finalized,
                    spread,
                    conversionrate  AS conversion_rate,
                    calculationcurrencydigits AS calculation_currency_digits
                FROM report.dealio_mt5trades
                WHERE ticket > {last_ticket}
                ORDER BY ticket
            """
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
        if df.empty:
            break
        last_ticket = int(df["ticket"].max())
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
