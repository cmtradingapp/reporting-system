import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from app.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
    POSTGRES_PASSWORD, POSTGRES_DB,
)


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def ensure_table():
    sql = """
        CREATE TABLE IF NOT EXISTS agent_performance (
            id          SERIAL PRIMARY KEY,
            agent_id    VARCHAR(100),
            full_name   VARCHAR(255),
            report_date DATE,
            ftc         NUMERIC,
            net         NUMERIC,
            synced_at   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_agent_performance_date
            ON agent_performance (report_date);

        CREATE TABLE IF NOT EXISTS accounts (
            accountid                   INTEGER PRIMARY KEY,
            is_test_account             SMALLINT,
            first_name                  VARCHAR(100),
            last_name                   VARCHAR(100),
            full_name                   VARCHAR(255),
            email                       VARCHAR(255),
            gender                      VARCHAR(10),
            customer_language           VARCHAR(20),
            country_iso                 VARCHAR(10),
            campaign                    VARCHAR(100),
            campaign_code_legacy        VARCHAR(255),
            client_source               VARCHAR(255),
            original_affiliate          VARCHAR(255),
            is_trading_active           SMALLINT,
            is_demo                     SMALLINT,
            compliance_status           VARCHAR(100),
            accountstatus               VARCHAR(50),
            sales_status                VARCHAR(100),
            retention_status            VARCHAR(100),
            kyc_workflow_status         VARCHAR(100),
            assigned_to                 INTEGER,
            sales_rep_id                INTEGER,
            sales_desk_id               INTEGER,
            retention_rep_id            INTEGER,
            retention_desk_id           INTEGER,
            first_sales_desk_id         INTEGER,
            first_retention_rep_id      INTEGER,
            compliance_agent            INTEGER,
            last_agent_assignment_time  TIMESTAMP,
            last_trade_opened_time      TIMESTAMP,
            has_notes                   SMALLINT,
            last_action_time            TIMESTAMP,
            source                      VARCHAR(255),
            has_frd                     SMALLINT,
            frd_time                    TIMESTAMP,
            last_trade_date             TIMESTAMP,
            first_deposit_date          TIMESTAMP,
            countdeposits               INTEGER,
            last_deposit_date           TIMESTAMP,
            last_interaction_date       TIMESTAMP,
            balance                     NUMERIC(20,4),
            net_deposit                 NUMERIC(20,4),
            first_trade_date            TIMESTAMP,
            ftd_amount                  NUMERIC(20,4),
            funded                      SMALLINT,
            login_date                  TIMESTAMP,
            total_deposit               NUMERIC(20,4),
            total_withdrawal            NUMERIC(20,4),
            createdtime                 TIMESTAMP,
            modifiedtime                TIMESTAMP,
            questionnaire_completed     VARCHAR(100),
            client_category             VARCHAR(100),
            client_qualification_date   TIMESTAMP,
            segmentation                VARCHAR(100),
            google_uid                  VARCHAR(255),
            birth_date                  DATE,
            customer_id                 INTEGER,
            regulation                  VARCHAR(100),
            sales_client_potential      VARCHAR(100),
            synced_at                   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_modifiedtime
            ON accounts (modifiedtime);

        ALTER TABLE accounts ALTER COLUMN gender            TYPE VARCHAR(100);
        ALTER TABLE accounts ALTER COLUMN customer_language TYPE VARCHAR(100);
        ALTER TABLE accounts ALTER COLUMN country_iso       TYPE VARCHAR(100);

        CREATE TABLE IF NOT EXISTS sync_log (
            id            SERIAL PRIMARY KEY,
            table_name    VARCHAR(100)  NOT NULL,
            cutoff_used   TIMESTAMP     NOT NULL,
            rows_affected INT           DEFAULT 0,
            duration_ms   INT           DEFAULT 0,
            status        VARCHAR(20)   DEFAULT 'success',
            error_message TEXT,
            ran_at        TIMESTAMP     NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_sync_log_table_name ON sync_log (table_name);
        CREATE INDEX IF NOT EXISTS idx_sync_log_ran_at ON sync_log (ran_at);

        CREATE TABLE IF NOT EXISTS crm_users (
            id                  BIGINT          PRIMARY KEY,
            email               VARCHAR(255),
            full_name           VARCHAR(255),
            status              VARCHAR(20),
            first_name          VARCHAR(100),
            last_name           VARCHAR(100),
            role_id             BIGINT,
            desk_id             BIGINT,
            desk_name           VARCHAR(255),
            team                VARCHAR(255),
            department          VARCHAR(255),
            desk                VARCHAR(255),
            type                VARCHAR(50),
            office_id           BIGINT,
            office              VARCHAR(255),
            position            VARCHAR(100),
            language            VARCHAR(10),
            last_logon_time     TIMESTAMP,
            last_update_time    TIMESTAMP,
            synced_at           TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_crm_users_desk_id ON crm_users (desk_id);
        CREATE INDEX IF NOT EXISTS idx_crm_users_office_id ON crm_users (office_id);
        CREATE INDEX IF NOT EXISTS idx_crm_users_status ON crm_users (status);
        CREATE INDEX IF NOT EXISTS idx_crm_users_position ON crm_users (position);
        CREATE INDEX IF NOT EXISTS idx_crm_users_last_update_time ON crm_users (last_update_time);

        CREATE TABLE IF NOT EXISTS transactions (
            mttransactionsid            BIGINT          PRIMARY KEY,
            tradingaccountsid           BIGINT,
            transaction_no              VARCHAR(100),
            vtigeraccountid             BIGINT,
            manualorauto                SMALLINT,
            paymenttype                 VARCHAR(100),
            transactionapproval         VARCHAR(100),
            amount                      NUMERIC(20,4),
            creditcardlast              VARCHAR(50),
            transactiontype             VARCHAR(100),
            login                       VARCHAR(100),
            platform                    VARCHAR(100),
            cardtype                    VARCHAR(100),
            cvv2pin                     VARCHAR(100),
            expmon                      VARCHAR(20),
            expyear                     VARCHAR(20),
            server                      VARCHAR(100),
            comment                     TEXT,
            transactionid               VARCHAR(255),
            receipt                     VARCHAR(255),
            bank_name                   VARCHAR(255),
            bank_acccount_holder        VARCHAR(255),
            bank_acccount_number        VARCHAR(255),
            referencenum                VARCHAR(255),
            expiration                  VARCHAR(100),
            actionok                    VARCHAR(100),
            cleared_by                  VARCHAR(100),
            mtorder_id                  VARCHAR(255),
            approved_by                 BIGINT,
            ewalletid                   VARCHAR(255),
            transaction_source          VARCHAR(100),
            currency_id                 VARCHAR(20),
            bank_country_id             VARCHAR(20),
            bank_state                  VARCHAR(100),
            bank_city                   VARCHAR(100),
            bank_address                TEXT,
            swift                       VARCHAR(100),
            need_revise                 VARCHAR(100),
            original_deposit_owner      BIGINT,
            decline_reason              TEXT,
            ftd                         SMALLINT,
            usdamount                   NUMERIC(20,4),
            chb_type                    VARCHAR(100),
            chb_status                  VARCHAR(100),
            chb_date                    DATE,
            cellexpert                  VARCHAR(100),
            client_source               VARCHAR(100),
            iban                        VARCHAR(100),
            deposifromip                VARCHAR(50),
            cardownername               VARCHAR(255),
            server_id                   SMALLINT,
            ticket                      VARCHAR(100),
            payment_method_id           VARCHAR(100),
            confirmation_time           TIMESTAMP,
            payment_processor           VARCHAR(255),
            withdrawal_reason           TEXT,
            deposit_ip                  VARCHAR(50),
            expiration_card             VARCHAR(20),
            original_owner_department   SMALLINT,
            dod                         TIMESTAMP,
            granted_by                  VARCHAR(100),
            destination_wallet          VARCHAR(255),
            payment_method              VARCHAR(255),
            compliance_status           VARCHAR(100),
            ftd_owner                   VARCHAR(100),
            email                       VARCHAR(255),
            created_time                TIMESTAMP,
            modifiedtime                TIMESTAMP,
            psp_transaction_id          VARCHAR(255),
            finance_status              VARCHAR(100),
            session_id                  VARCHAR(100),
            gateway_name                VARCHAR(100),
            payment_subtype             VARCHAR(100),
            legacy_mtt                  VARCHAR(100),
            fee_type                    VARCHAR(100),
            fee                         NUMERIC(20,4),
            fee_included                SMALLINT,
            transaction_promo           VARCHAR(100),
            assisted_by                 VARCHAR(100),
            deleted                     SMALLINT,
            is_frd                      SMALLINT,
            synced_at                   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_transactions_modifiedtime    ON transactions (modifiedtime);
        CREATE INDEX IF NOT EXISTS idx_transactions_confirmation     ON transactions (confirmation_time);
        CREATE INDEX IF NOT EXISTS idx_transactions_vtigeraccountid ON transactions (vtigeraccountid);
        CREATE INDEX IF NOT EXISTS idx_transactions_approval        ON transactions (transactionapproval);
        CREATE INDEX IF NOT EXISTS idx_transactions_ftd             ON transactions (ftd);

        CREATE TABLE IF NOT EXISTS targets (
            date       DATE    NOT NULL,
            agent_id   VARCHAR(100) NOT NULL,
            ftc        NUMERIC(20,4),
            net        NUMERIC(20,4),
            synced_at  TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (date, agent_id)
        );
        CREATE INDEX IF NOT EXISTS idx_targets_date     ON targets (date);
        CREATE INDEX IF NOT EXISTS idx_targets_agent_id ON targets (agent_id);

        CREATE TABLE IF NOT EXISTS users (
            id               VARCHAR(100) PRIMARY KEY,
            email            VARCHAR(255),
            full_name        VARCHAR(255),
            status           VARCHAR(20),
            first_name       VARCHAR(100),
            last_name        VARCHAR(100),
            role_id          VARCHAR(100),
            desk_id          VARCHAR(100),
            language         VARCHAR(20),
            last_logon_time  TIMESTAMP,
            last_update_time TIMESTAMP,
            desk_name        VARCHAR(255),
            team             VARCHAR(255),
            department       VARCHAR(255),
            desk             VARCHAR(255),
            type             VARCHAR(50),
            office_id        VARCHAR(100),
            office           VARCHAR(255),
            position         VARCHAR(100),
            synced_at        TIMESTAMP DEFAULT NOW()
        );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def delete_all_performance():
    sql = "TRUNCATE TABLE agent_performance"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _clean(val):
    """Convert pandas NaT/NaN to None and strip NUL bytes for PostgreSQL compatibility."""
    if val is None:
        return None
    try:
        if pd.isnull(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str):
        val = val.replace('\x00', '')
    return val


def upsert_users(df: pd.DataFrame):
    rows = [
        (
            str(row["id"]),
            _clean(row.get("email")),
            _clean(row.get("full_name")),
            _clean(row.get("status")),
            _clean(row.get("first_name")),
            _clean(row.get("last_name")),
            _clean(str(row["role_id"])) if _clean(row.get("role_id")) is not None else None,
            _clean(str(row["desk_id"])) if _clean(row.get("desk_id")) is not None else None,
            _clean(row.get("language")),
            _clean(row.get("last_logon_time")),
            _clean(row.get("last_update_time")),
            _clean(row.get("desk_name")),
            _clean(row.get("team")),
            _clean(row.get("department")),
            _clean(row.get("desk")),
            _clean(row.get("type")),
            _clean(str(row["office_id"])) if _clean(row.get("office_id")) is not None else None,
            _clean(row.get("office")),
            _clean(row.get("position")),
        )
        for _, row in df.iterrows()
    ]
    sql = """
        INSERT INTO users (id, email, full_name, status, first_name, last_name,
            role_id, desk_id, language, last_logon_time, last_update_time,
            desk_name, team, department, desk, type, office_id, office, position)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            email            = EXCLUDED.email,
            full_name        = EXCLUDED.full_name,
            status           = EXCLUDED.status,
            first_name       = EXCLUDED.first_name,
            last_name        = EXCLUDED.last_name,
            role_id          = EXCLUDED.role_id,
            desk_id          = EXCLUDED.desk_id,
            language         = EXCLUDED.language,
            last_logon_time  = EXCLUDED.last_logon_time,
            last_update_time = EXCLUDED.last_update_time,
            desk_name        = EXCLUDED.desk_name,
            team             = EXCLUDED.team,
            department       = EXCLUDED.department,
            desk             = EXCLUDED.desk,
            type             = EXCLUDED.type,
            office_id        = EXCLUDED.office_id,
            office           = EXCLUDED.office,
            position         = EXCLUDED.position,
            synced_at        = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def upsert_accounts(df: pd.DataFrame):
    cols = [
        "accountid", "is_test_account", "first_name", "last_name", "full_name",
        "email", "gender", "customer_language", "country_iso", "campaign",
        "campaign_code_legacy", "client_source", "original_affiliate",
        "is_trading_active", "is_demo", "compliance_status", "accountstatus",
        "sales_status", "retention_status", "kyc_workflow_status", "assigned_to",
        "sales_rep_id", "sales_desk_id", "retention_rep_id", "retention_desk_id",
        "first_sales_desk_id", "first_retention_rep_id", "compliance_agent",
        "last_agent_assignment_time", "last_trade_opened_time", "has_notes",
        "last_action_time", "source", "has_frd", "frd_time", "last_trade_date",
        "first_deposit_date", "countdeposits", "last_deposit_date",
        "last_interaction_date", "balance", "net_deposit", "first_trade_date",
        "ftd_amount", "funded", "login_date", "total_deposit", "total_withdrawal",
        "createdtime", "modifiedtime", "questionnaire_completed", "client_category",
        "client_qualification_date", "segmentation", "google_uid", "birth_date",
        "customer_id", "regulation", "sales_client_potential",
    ]
    rows = [
        tuple(_clean(row.get(c)) for c in cols)
        for _, row in df.iterrows()
    ]
    update_cols = [c for c in cols if c != "accountid"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO accounts ({col_list})
        VALUES %s
        ON CONFLICT (accountid) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def insert_records(df: pd.DataFrame):
    rows = [
        (
            str(row["agent_id"]),
            str(row["full_name"]),
            row["date"],
            row["ftc"],
            row["net"],
        )
        for _, row in df.iterrows()
    ]
    sql = """
        INSERT INTO agent_performance (agent_id, full_name, report_date, ftc, net)
        VALUES %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def fetch_report_data() -> pd.DataFrame:
    sql = """
        SELECT
            agent_id,
            full_name,
            SUM(ftc)  AS total_ftc,
            SUM(net)  AS total_net,
            COUNT(*)  AS trading_days
        FROM agent_performance
        WHERE DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY agent_id, full_name
        ORDER BY total_net DESC
    """
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def fetch_last_sync() -> str:
    sql = "SELECT MAX(synced_at) FROM agent_performance"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()[0]
            return result.strftime("%Y-%m-%d %H:%M:%S") if result else "Never"
    finally:
        conn.close()


def log_sync(table_name: str, cutoff_used, rows_affected: int, duration_ms: int, status: str, error_message: str = None):
    sql = """
        INSERT INTO sync_log (table_name, cutoff_used, rows_affected, duration_ms, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (table_name, cutoff_used, rows_affected, duration_ms, status, error_message))
        conn.commit()
    finally:
        conn.close()


def fetch_accounts_stats() -> dict:
    sql = """
        SELECT
            COUNT(*) AS total_records,
            MAX(synced_at) AS last_synced_at,
            COUNT(*) FILTER (WHERE funded = 1) AS funded_accounts,
            COUNT(*) FILTER (WHERE accountstatus = 'Sales') AS sales_accounts,
            COUNT(*) FILTER (WHERE accountstatus = 'Retention') AS retention_accounts
        FROM accounts
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records": row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "funded_accounts": row[2] or 0,
                "sales_accounts": row[3] or 0,
                "retention_accounts": row[4] or 0,
            }
    finally:
        conn.close()


def upsert_crm_users(df: pd.DataFrame):
    cols = [
        "id", "email", "full_name", "status", "first_name", "last_name",
        "role_id", "desk_id", "language", "last_logon_time", "last_update_time",
        "desk_name", "team", "department", "desk", "type", "office_id", "office", "position",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c != "id"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO crm_users ({col_list})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def fetch_crm_users_stats() -> dict:
    sql = """
        SELECT
            COUNT(*) AS total_records,
            MAX(synced_at) AS last_synced_at,
            COUNT(*) FILTER (WHERE status = 'Active') AS active_users,
            COUNT(DISTINCT desk_id) FILTER (WHERE position = 'Agent') AS unique_desks,
            COUNT(DISTINCT office_id) AS unique_offices
        FROM crm_users
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records": row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "active_users": row[2] or 0,
                "unique_desks": row[3] or 0,
                "unique_offices": row[4] or 0,
            }
    finally:
        conn.close()


def upsert_transactions(df: pd.DataFrame):
    cols = [
        "mttransactionsid", "tradingaccountsid", "transaction_no", "vtigeraccountid",
        "manualorauto", "paymenttype", "transactionapproval", "amount", "creditcardlast",
        "transactiontype", "login", "platform", "cardtype", "cvv2pin", "expmon", "expyear",
        "server", "comment", "transactionid", "receipt", "bank_name", "bank_acccount_holder",
        "bank_acccount_number", "referencenum", "expiration", "actionok", "cleared_by",
        "mtorder_id", "approved_by", "ewalletid", "transaction_source", "currency_id",
        "bank_country_id", "bank_state", "bank_city", "bank_address", "swift", "need_revise",
        "original_deposit_owner", "decline_reason", "ftd", "usdamount", "chb_type",
        "chb_status", "chb_date", "cellexpert", "client_source", "iban", "deposifromip",
        "cardownername", "server_id", "ticket", "payment_method_id", "confirmation_time",
        "payment_processor", "withdrawal_reason", "deposit_ip", "expiration_card",
        "original_owner_department", "dod", "granted_by", "destination_wallet",
        "payment_method", "compliance_status", "ftd_owner", "email", "created_time",
        "modifiedtime", "psp_transaction_id", "finance_status", "session_id", "gateway_name",
        "payment_subtype", "legacy_mtt", "fee_type", "fee", "fee_included",
        "transaction_promo", "assisted_by", "deleted", "is_frd",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c != "mttransactionsid"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO transactions ({col_list})
        VALUES %s
        ON CONFLICT (mttransactionsid) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def fetch_transactions_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                                            AS total_records,
            MAX(synced_at)                                      AS last_synced_at,
            COUNT(*) FILTER (WHERE transactionapproval = 'Approved') AS approved,
            COUNT(*) FILTER (WHERE ftd = 1)                    AS ftd_count,
            COALESCE(SUM(usdamount), 0)                        AS total_usd
        FROM transactions
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records": row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "approved": row[2] or 0,
                "ftd_count": row[3] or 0,
                "total_usd": int(row[4] or 0),
            }
    finally:
        conn.close()


def upsert_targets(df: pd.DataFrame):
    cols = ["date", "agent_id", "ftc", "net"]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    sql = """
        INSERT INTO targets (date, agent_id, ftc, net)
        VALUES %s
        ON CONFLICT (date, agent_id) DO UPDATE SET
            ftc       = EXCLUDED.ftc,
            net       = EXCLUDED.net,
            synced_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def fetch_targets_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                 AS total_records,
            MAX(synced_at)           AS last_synced_at,
            COUNT(DISTINCT agent_id) AS unique_agents,
            COALESCE(SUM(ftc), 0)    AS total_ftc,
            COALESCE(SUM(net), 0)    AS total_net
        FROM targets
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":  row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_agents":  row[2] or 0,
                "total_ftc":      int(row[3] or 0),
                "total_net":      int(row[4] or 0),
            }
    finally:
        conn.close()


def fetch_sync_log(table_name: str, limit: int = 50) -> list:
    sql = """
        SELECT ran_at, cutoff_used, rows_affected, duration_ms, status, error_message
        FROM sync_log
        WHERE table_name = %s
        ORDER BY ran_at DESC
        LIMIT %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (table_name, limit))
            rows = cur.fetchall()
            return [
                {
                    "ran_at": r[0].strftime("%Y-%m-%d %H:%M:%S") if r[0] else "",
                    "cutoff_used": r[1].strftime("%Y-%m-%d %H:%M:%S") if r[1] else "",
                    "rows_affected": r[2] or 0,
                    "duration_ms": r[3] or 0,
                    "status": r[4] or "unknown",
                    "error_message": r[5] or "",
                }
                for r in rows
            ]
    finally:
        conn.close()


def fetch_users_with_targets() -> pd.DataFrame:
    sql = """
        SELECT
            u.id,
            u.full_name,
            u.email,
            u.position,
            u.office,
            u.team,
            u.department,
            u.desk_name,
            u.status,
            u.last_logon_time,
            COALESCE(ap.total_ftc, 0) AS total_ftc,
            COALESCE(ap.total_net, 0) AS total_net,
            COALESCE(ap.trading_days, 0) AS trading_days
        FROM users u
        LEFT JOIN (
            SELECT
                agent_id,
                SUM(ftc)  AS total_ftc,
                SUM(net)  AS total_net,
                COUNT(*)  AS trading_days
            FROM agent_performance
            WHERE DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY agent_id
        ) ap ON u.id = ap.agent_id
        WHERE u.status = 'Active'
        ORDER BY total_net DESC
    """
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()
