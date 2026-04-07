import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
import pandas as pd
import threading
from app.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
    POSTGRES_PASSWORD, POSTGRES_DB,
)

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(
                    minconn=2,
                    maxconn=25,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname=POSTGRES_DB,
                    connect_timeout=10,
                    options="-c statement_timeout=90000",  # 90s max per query
                )
    return _pool


class _PooledConnection:
    """Thin proxy around a psycopg2 connection that returns it to the pool on close()."""
    __slots__ = ("_conn", "_pool")

    def __init__(self, conn, pool):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_pool", pool)

    # Proxy all attribute access to the real connection
    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_conn"), name, value)

    def close(self):
        conn = object.__getattribute__(self, "_conn")
        pool = object.__getattribute__(self, "_pool")
        try:
            if not conn.closed and conn.status != psycopg2.extensions.STATUS_READY:
                conn.rollback()
        except Exception:
            pass
        pool.putconn(conn)

    def cursor(self, *args, **kwargs):
        return object.__getattribute__(self, "_conn").cursor(*args, **kwargs)

    def commit(self):
        return object.__getattribute__(self, "_conn").commit()

    def rollback(self):
        return object.__getattribute__(self, "_conn").rollback()


def get_connection() -> "_PooledConnection":
    """Get a connection from the pool. Call conn.close() as normal —
    it returns the connection to the pool instead of closing the socket."""
    pool = _get_pool()
    conn = pool.getconn()
    return _PooledConnection(conn, pool)


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
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'accounts'
                  AND column_name = 'classification_int' AND is_generated = 'ALWAYS'
            ) THEN
                ALTER TABLE accounts DROP COLUMN classification_int;
            END IF;
        END $$;
        ALTER TABLE accounts ADD COLUMN IF NOT EXISTS classification_int SMALLINT;
        CREATE INDEX IF NOT EXISTS idx_accounts_classification_int ON accounts (classification_int);

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

        ALTER TABLE crm_users ADD COLUMN IF NOT EXISTS office_name  VARCHAR(255);
        ALTER TABLE crm_users ADD COLUMN IF NOT EXISTS agent_name   VARCHAR(255);
        ALTER TABLE crm_users ADD COLUMN IF NOT EXISTS department_  VARCHAR(20);

        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS transactiontypename VARCHAR(100);

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

        CREATE TABLE IF NOT EXISTS trading_accounts (
            trading_account_id                  BIGINT           PRIMARY KEY,
            trading_account_name                VARCHAR(255),
            vtigeraccountid                     BIGINT,
            trade_group                         VARCHAR(255),
            last_update                         TIMESTAMP,
            equity                              NUMERIC(20,4),
            open_pnl                            NUMERIC(20,4),
            total_pnl                           NUMERIC(20,4),
            commission                          NUMERIC(20,4),
            enable                              SMALLINT,
            enable_read_only                    SMALLINT,
            login                               VARCHAR(100),
            currency                            VARCHAR(20),
            serverid                            SMALLINT,
            assigned_to                         BIGINT,
            balance                             NUMERIC(20,4),
            credit                              NUMERIC(20,4),
            swaps                               NUMERIC(20,4),
            total_taxes                         NUMERIC(20,4),
            leverage                            INTEGER,
            margin                              NUMERIC(20,4),
            margin_level                        NUMERIC(20,4),
            margin_free                         NUMERIC(20,4),
            created_time                        TIMESTAMP,
            trading_server_created_timestamp    TIMESTAMP,
            platform                            VARCHAR(100),
            deleted                             SMALLINT,
            synced_at                           TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_trading_accounts_last_update   ON trading_accounts (last_update);
        CREATE INDEX IF NOT EXISTS idx_trading_accounts_vtigeraccountid ON trading_accounts (vtigeraccountid);
        CREATE INDEX IF NOT EXISTS idx_trading_accounts_assigned_to   ON trading_accounts (assigned_to);
        CREATE INDEX IF NOT EXISTS idx_trading_accounts_enable        ON trading_accounts (enable);

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

        CREATE TABLE IF NOT EXISTS ftd100_clients (
            accountid                   BIGINT          PRIMARY KEY,
            accountstatus               VARCHAR(20),
            client_qualification_date   TIMESTAMP,
            assigned_to                 BIGINT,
            ftd_100_date                TIMESTAMP,
            ftd_100_amount              NUMERIC(18,2),
            original_deposit_owner      BIGINT,
            net_deposits_current        NUMERIC(18,2),
            net_until_qualification     NUMERIC(18,2),
            synced_at                   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ftd100_original_deposit_owner ON ftd100_clients (original_deposit_owner);
        CREATE INDEX IF NOT EXISTS idx_ftd100_ftd_100_date           ON ftd100_clients (ftd_100_date);
        CREATE INDEX IF NOT EXISTS idx_ftd100_accountstatus          ON ftd100_clients (accountstatus);

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

        CREATE TABLE IF NOT EXISTS public_holidays (
            holiday_date  DATE        NOT NULL,
            description   VARCHAR(255),
            CONSTRAINT pk_public_holidays PRIMARY KEY (holiday_date)
        );

        CREATE TABLE IF NOT EXISTS dealio_users (
            login          BIGINT           NOT NULL,
            sourceid       TEXT             NOT NULL,
            sourcename     TEXT,
            sourcetype     TEXT,
            groupname      TEXT,
            groupcurrency  TEXT,
            name           TEXT,
            email          TEXT,
            country        TEXT,
            city           TEXT,
            zipcode        TEXT,
            address        TEXT,
            phone          TEXT,
            comment        TEXT,
            balance        DOUBLE PRECISION,
            credit         DOUBLE PRECISION,
            leverage       INTEGER,
            status         TEXT,
            regdate        TIMESTAMP,
            lastdate       TIMESTAMP,
            lastupdate     TIMESTAMPTZ,
            agentaccount   BIGINT,
            isenabled      BOOLEAN,
            synced_at      TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (login, sourceid)
        );
        CREATE INDEX IF NOT EXISTS idx_dealio_users_lastupdate ON dealio_users (lastupdate);
        CREATE INDEX IF NOT EXISTS idx_dealio_users_group      ON dealio_users (groupname);

        CREATE TABLE IF NOT EXISTS dealio_trades_mt4 (
            ticket          BIGINT           NOT NULL,
            source_id       TEXT             NOT NULL,
            login           BIGINT,
            cmd             SMALLINT,
            volume          DOUBLE PRECISION,
            open_time       TIMESTAMP,
            close_time      TIMESTAMP,
            last_modified   TIMESTAMP,
            profit          DOUBLE PRECISION,
            computed_profit DOUBLE PRECISION,
            symbol          TEXT,
            core_symbol     TEXT,
            book            TEXT,
            open_price      DOUBLE PRECISION,
            close_price     DOUBLE PRECISION,
            commission      DOUBLE PRECISION,
            swaps           DOUBLE PRECISION,
            comment         TEXT,
            group_name      TEXT,
            group_currency  TEXT,
            source_name     TEXT,
            source_type     TEXT,
            reason          INTEGER,
            notional_value  DOUBLE PRECISION,
            computed_swap   DOUBLE PRECISION,
            computed_commission DOUBLE PRECISION,
            spread          VARCHAR(255),
            synced_at       TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (source_id, ticket)
        );
        ALTER TABLE dealio_trades_mt4 ADD COLUMN IF NOT EXISTS notional_value DOUBLE PRECISION;
        ALTER TABLE dealio_trades_mt4 ADD COLUMN IF NOT EXISTS computed_swap DOUBLE PRECISION;
        ALTER TABLE dealio_trades_mt4 ADD COLUMN IF NOT EXISTS computed_commission DOUBLE PRECISION;
        ALTER TABLE dealio_trades_mt4 ADD COLUMN IF NOT EXISTS spread VARCHAR(255);
        CREATE INDEX IF NOT EXISTS idx_dtm4_login         ON dealio_trades_mt4 (login);
        CREATE INDEX IF NOT EXISTS idx_dtm4_open_time     ON dealio_trades_mt4 (open_time);
        CREATE INDEX IF NOT EXISTS idx_dtm4_close_time    ON dealio_trades_mt4 (close_time);
        CREATE INDEX IF NOT EXISTS idx_dtm4_last_modified ON dealio_trades_mt4 (last_modified);
        CREATE INDEX IF NOT EXISTS idx_dtm4_symbol        ON dealio_trades_mt4 (symbol);

        CREATE TABLE IF NOT EXISTS dealio_trades_mt5 (
            ticket              BIGINT           NOT NULL,
            source_id           TEXT             NOT NULL,
            login               BIGINT,
            symbol              TEXT,
            digit               BIGINT,
            cmd                 SMALLINT,
            volume              DOUBLE PRECISION,
            open_time           TIMESTAMP,
            open_price          DOUBLE PRECISION,
            close_time          TIMESTAMP,
            close_price         DOUBLE PRECISION,
            reason              INTEGER,
            commission          DOUBLE PRECISION,
            agent_id            BIGINT,
            swap                DOUBLE PRECISION,
            profit              DOUBLE PRECISION,
            comment             TEXT,
            computed_profit     DOUBLE PRECISION,
            computed_swap       DOUBLE PRECISION,
            computed_commission DOUBLE PRECISION,
            group_name          TEXT,
            group_currency      TEXT,
            book                TEXT,
            notional_value      DOUBLE PRECISION,
            source_name         TEXT,
            source_type         TEXT,
            position_id         BIGINT,
            entry               SMALLINT,
            volume_closed       DOUBLE PRECISION,
            sync_time           TIMESTAMP,
            is_finalized        BOOLEAN,
            spread              TEXT,
            conversion_rate     DOUBLE PRECISION,
            synced_at           TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (source_id, ticket)
        );
        CREATE INDEX IF NOT EXISTS idx_dtm5_login      ON dealio_trades_mt5 (login);
        CREATE INDEX IF NOT EXISTS idx_dtm5_open_time  ON dealio_trades_mt5 (open_time);
        CREATE INDEX IF NOT EXISTS idx_dtm5_close_time ON dealio_trades_mt5 (close_time);
        CREATE INDEX IF NOT EXISTS idx_dtm5_sync_time  ON dealio_trades_mt5 (sync_time);
        CREATE INDEX IF NOT EXISTS idx_dtm5_symbol     ON dealio_trades_mt5 (symbol);

        CREATE TABLE IF NOT EXISTS dealio_daily_profits (
            date                        TIMESTAMP        NOT NULL,
            login                       BIGINT           NOT NULL,
            sourceid                    VARCHAR(50)      NOT NULL,
            sourcename                  VARCHAR(50),
            sourcetype                  VARCHAR(50),
            book                        VARCHAR(50),
            closedpnl                   DOUBLE PRECISION,
            convertedclosedpnl          DOUBLE PRECISION,
            calculationcurrency         VARCHAR(50),
            floatingpnl                 DOUBLE PRECISION,
            convertedfloatingpnl        DOUBLE PRECISION,
            netdeposit                  DOUBLE PRECISION,
            convertednetdeposit         DOUBLE PRECISION,
            equity                      DOUBLE PRECISION,
            convertedequity             DOUBLE PRECISION,
            balance                     DOUBLE PRECISION,
            convertedbalance            DOUBLE PRECISION,
            groupcurrency               VARCHAR(50),
            conversionratio             DOUBLE PRECISION,
            equityprevday               DOUBLE PRECISION,
            groupname                   VARCHAR(50),
            deltafloatingpnl            DOUBLE PRECISION,
            converteddeltafloatingpnl   DOUBLE PRECISION,
            synced_at                   TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (date, login, sourceid)
        );
        CREATE INDEX IF NOT EXISTS idx_ddps_login ON dealio_daily_profits (login);
        CREATE INDEX IF NOT EXISTS idx_ddps_date  ON dealio_daily_profits (date);

        CREATE TABLE IF NOT EXISTS campaigns (
            crmid                       VARCHAR         PRIMARY KEY,
            campaign_id                 VARCHAR,
            campaign_name               VARCHAR,
            campaign_legacy_id          VARCHAR,
            campaign_description        TEXT,
            campaign_channel            VARCHAR,
            campaign_sub_channel        VARCHAR,
            website                     VARCHAR,
            active                      SMALLINT,
            start_date                  DATE,
            assigned_to                 VARCHAR,
            disable_email_verification  VARCHAR,
            marketing_group             TEXT GENERATED ALWAYS AS (
                CASE
                    WHEN campaign_channel LIKE '%IB%'             THEN 'IB'
                    WHEN campaign_channel LIKE '%PPC%'            THEN 'PPC'
                    WHEN campaign_channel LIKE '%Media%'          THEN 'Media'
                    WHEN campaign_channel LIKE '%Affiliate%'      THEN 'Affiliates'
                    WHEN campaign_channel LIKE '%Direct%'         THEN 'Organic'
                    WHEN campaign_channel LIKE '%Organic%'        THEN 'Organic'
                    WHEN campaign_channel LIKE '%White%'          THEN 'White Label'
                    WHEN campaign_channel LIKE '%GMT%'            THEN 'White Label'
                    WHEN campaign_channel LIKE '%Refer a friend%' THEN 'FRF'
                    WHEN campaign_channel LIKE '%Automation%'     THEN 'Automation'
                    ELSE 'Other'
                END
            ) STORED,
            synced_at                   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_campaigns_campaign_id ON campaigns (campaign_id);
        CREATE INDEX IF NOT EXISTS idx_campaigns_active      ON campaigns (active);
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'campaigns' AND column_name = 'active'
                  AND data_type = 'boolean'
            ) THEN
                ALTER TABLE campaigns ALTER COLUMN active TYPE SMALLINT USING active::int;
            END IF;
        END $$;

    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Advisory lock so only one worker runs schema migrations at startup
            cur.execute("SELECT pg_advisory_lock(123456789)")
            try:
                cur.execute(sql)
                # Seed recurring holidays for 2024 through current year + 5
                from datetime import date
                current_year = date.today().year
                holiday_rows = []
                for y in range(2024, current_year + 11):
                    holiday_rows += [
                        (f"{y}-01-01", "New Year's Day"),
                        (f"{y}-12-24", "Christmas Eve"),
                        (f"{y}-12-25", "Christmas Day"),
                        (f"{y}-12-26", "Boxing Day"),
                        (f"{y}-12-31", "New Year's Eve"),
                    ]
                execute_values(
                    cur,
                    "INSERT INTO public_holidays (holiday_date, description) VALUES %s ON CONFLICT (holiday_date) DO NOTHING",
                    holiday_rows,
                )
            finally:
                cur.execute("SELECT pg_advisory_unlock(123456789)")
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


def cleanup_accounts():
    """Delete test accounts and null/blank accountid rows from accounts table."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM accounts WHERE is_test_account != 0 OR accountid IS NULL")
        conn.commit()
    finally:
        conn.close()


def _to_classification_int(v):
    if v is None:
        return None
    try:
        i = int(float(str(v).strip()))
        return i if 1 <= i <= 10 else None
    except (ValueError, TypeError, AttributeError):
        return None


def upsert_accounts(df: pd.DataFrame):
    src_cols = [
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
    cols = src_cols + ["classification_int"]
    rows = [
        tuple(_clean(row.get(c)) for c in src_cols) + (_to_classification_int(row.get("sales_client_potential")),)
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


def backfill_classification_int():
    """One-time backfill: compute classification_int for all rows where it is NULL.
    Runs in a background thread at startup — safe to fail.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE accounts
                SET classification_int = CASE
                    WHEN sales_client_potential ~ '^[0-9]+(\\.[0-9]+)?$'
                         AND sales_client_potential::numeric::int BETWEEN 1 AND 10
                    THEN sales_client_potential::numeric::int
                    ELSE NULL
                END
                WHERE classification_int IS NULL
            """)
        conn.commit()
        print("[backfill_classification_int] done")
    except Exception as e:
        conn.rollback()
        print(f"[backfill_classification_int] error: {e}")
    finally:
        conn.close()


def ensure_agent_dept_history_table():
    """Create agent_dept_history table to track department overrides for reporting.
    Agents who move between departments mid-month can be pinned to a report_dept
    for a specific date range so historical reports stay accurate.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent_dept_history (
                    id           SERIAL PRIMARY KEY,
                    agent_id     INTEGER NOT NULL,
                    report_dept  VARCHAR(50) NOT NULL,
                    effective_from DATE NOT NULL,
                    effective_to   DATE,
                    note         TEXT,
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (agent_id, report_dept, effective_from)
                );
                CREATE INDEX IF NOT EXISTS idx_adh_agent ON agent_dept_history(agent_id);
                CREATE INDEX IF NOT EXISTS idx_adh_dept  ON agent_dept_history(report_dept, effective_from, effective_to);
            """)
            # Seed the 5 ex-Retention agents for March 2026 (idempotent)
            cur.execute("""
                INSERT INTO agent_dept_history (agent_id, report_dept, effective_from, effective_to, note)
                VALUES
                    (3750, 'Retention', '2026-03-01', '2026-03-31', 'Tamara R — moved to Sales after March 2026'),
                    (3614, 'Retention', '2026-03-01', '2026-03-31', 'Temitope D — moved to Sales after March 2026'),
                    (6119, 'Retention', '2026-03-01', '2026-03-31', 'Ramy N — moved to Sales after March 2026'),
                    (6479, 'Retention', '2026-03-01', '2026-03-31', 'Princess C — moved to Sales after March 2026'),
                    (6492, 'Retention', '2026-03-01', '2026-03-31', 'Zinhle K — moved to Sales after March 2026')
                ON CONFLICT (agent_id, report_dept, effective_from) DO NOTHING;
            """)
        conn.commit()
    finally:
        conn.close()


def ensure_bonus_transactions_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bonus_transactions (
                    mttransactionsid BIGINT PRIMARY KEY,
                    login            BIGINT,
                    net_amount       NUMERIC(18, 2),
                    confirmation_time TIMESTAMP,
                    synced_at        TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_bonus_tx_login ON bonus_transactions(login);
                CREATE INDEX IF NOT EXISTS idx_bonus_tx_conf  ON bonus_transactions(confirmation_time);
                ALTER TABLE bonus_transactions ADD COLUMN IF NOT EXISTS manual_override BOOLEAN DEFAULT FALSE;
            """)
        conn.commit()
    finally:
        conn.close()


def upsert_bonus_transactions(df) -> int:
    if df is None or len(df) == 0:
        return 0
    import pandas as pd
    rows = [
        (int(r["mttransactionsid"]),
         int(r["login"]) if r["login"] is not None else None,
         float(r["net_amount"]) if r["net_amount"] is not None else None,
         None if pd.isnull(r["confirmation_time"]) else r["confirmation_time"])
        for _, r in df.iterrows()
        if r["mttransactionsid"] is not None
    ]
    if not rows:
        return 0
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO bonus_transactions (mttransactionsid, login, net_amount, confirmation_time)
                VALUES %s
                ON CONFLICT (mttransactionsid) DO UPDATE SET
                    login             = EXCLUDED.login,
                    net_amount        = CASE WHEN bonus_transactions.manual_override THEN bonus_transactions.net_amount ELSE EXCLUDED.net_amount END,
                    confirmation_time = EXCLUDED.confirmation_time,
                    synced_at         = NOW()
            """, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def fetch_bonus_transactions_stats() -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), MAX(synced_at), COUNT(DISTINCT login),
                       COALESCE(SUM(net_amount), 0)
                FROM bonus_transactions
            """)
            row = cur.fetchone()
            return {
                "total_records":  row[0] or 0,
                "last_synced_at": str(row[1]) if row[1] else "Never",
                "unique_logins":  row[2] or 0,
                "total_net_bonus": float(row[3] or 0),
            }
    except Exception:
        return {"total_records": 0, "last_synced_at": "Never", "unique_logins": 0, "total_net_bonus": 0.0}
    finally:
        conn.close()


def ensure_auth_table():
    sql = """
        CREATE TABLE IF NOT EXISTS auth_users (
            id                    SERIAL PRIMARY KEY,
            crm_user_id           BIGINT NULL,
            email                 VARCHAR(255) NOT NULL UNIQUE,
            full_name             VARCHAR(255) NOT NULL,
            password_hash         VARCHAR(255) NOT NULL,
            role                  VARCHAR(50)  NOT NULL,
            is_active             SMALLINT DEFAULT 1,
            force_password_change SMALLINT DEFAULT 0,
            created_at            TIMESTAMP DEFAULT NOW(),
            last_login            TIMESTAMP
        );
        ALTER TABLE auth_users ADD COLUMN IF NOT EXISTS allowed_pages TEXT NULL;
        UPDATE auth_users SET allowed_pages = '["performance","agent_bonuses"]'
            WHERE email = 'despina.n@cmtrading.com' AND allowed_pages IS NULL;
        CREATE INDEX IF NOT EXISTS idx_auth_users_email ON auth_users(email);
        CREATE INDEX IF NOT EXISTS idx_auth_users_crm_user_id ON auth_users(crm_user_id);
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def seed_admin_user(password_hash: str):
    sql = """
        INSERT INTO auth_users (email, full_name, password_hash, role, is_active, force_password_change)
        VALUES (%s, %s, %s, %s, 1, 0)
        ON CONFLICT (email) DO NOTHING
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, ('admin@cmtrading.com', 'Administrator', password_hash, 'admin'))
        conn.commit()
    finally:
        conn.close()


def get_auth_user_by_email(email: str) -> dict | None:
    sql = "SELECT id, crm_user_id, email, full_name, password_hash, role, is_active, force_password_change FROM auth_users WHERE email = %s"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (email,))
            row = cur.fetchone()
            if row is None:
                return None
            return {
                'id': row[0], 'crm_user_id': row[1], 'email': row[2],
                'full_name': row[3], 'password_hash': row[4], 'role': row[5],
                'is_active': row[6], 'force_password_change': row[7],
            }
    finally:
        conn.close()


def get_auth_user_by_id(user_id: int) -> dict | None:
    import json as _json
    sql = """
        SELECT a.id, a.crm_user_id, a.email, a.full_name, a.password_hash, a.role,
               a.is_active, a.force_password_change, c.department_, a.allowed_pages
        FROM auth_users a
        LEFT JOIN crm_users c ON c.id = a.crm_user_id
        WHERE a.id = %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            row = cur.fetchone()
            if row is None:
                return None
            ap_raw = row[9]
            try:
                ap_list = _json.loads(ap_raw) if ap_raw else None
            except Exception:
                ap_list = None
            return {
                'id': row[0], 'crm_user_id': row[1], 'email': row[2],
                'full_name': row[3], 'password_hash': row[4], 'role': row[5],
                'is_active': row[6], 'force_password_change': row[7],
                'department_': row[8],
                'allowed_pages': ap_raw,
                'allowed_pages_list': ap_list,
            }
    finally:
        conn.close()


def update_auth_user_last_login(user_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE auth_users SET last_login = NOW() WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        conn.close()


def list_auth_users() -> list:
    sql = """
        SELECT
            a.id, a.crm_user_id, a.email, a.full_name, a.role,
            a.is_active, a.force_password_change, a.created_at, a.last_login,
            COALESCE(c.agent_name, c.full_name) AS crm_name, a.allowed_pages
        FROM auth_users a
        LEFT JOIN crm_users c ON c.id = a.crm_user_id
        ORDER BY a.id
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [
                {
                    'id': r[0], 'crm_user_id': r[1], 'email': r[2], 'full_name': r[3],
                    'role': r[4], 'is_active': r[5], 'force_password_change': r[6],
                    'created_at': r[7].strftime('%Y-%m-%d %H:%M') if r[7] else '',
                    'last_login': r[8].strftime('%Y-%m-%d %H:%M') if r[8] else '',
                    'crm_name': r[9] or '',
                    'allowed_pages': r[10] or '',
                }
                for r in rows
            ]
    finally:
        conn.close()


def create_auth_user(email: str, full_name: str, password_hash: str, role: str, crm_user_id, allowed_pages: str | None = None) -> int:
    sql = """
        INSERT INTO auth_users (email, full_name, password_hash, role, crm_user_id, force_password_change, allowed_pages)
        VALUES (%s, %s, %s, %s, %s, 1, %s)
        RETURNING id
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (email, full_name, password_hash, role, crm_user_id or None, allowed_pages or None))
            new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    finally:
        conn.close()


def update_auth_user(user_id: int, full_name: str, email: str, role: str, is_active: int, crm_user_id, allowed_pages: str | None = None):
    sql = """
        UPDATE auth_users
        SET full_name = %s, email = %s, role = %s, is_active = %s, crm_user_id = %s, allowed_pages = %s
        WHERE id = %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (full_name, email, role, is_active, crm_user_id or None, allowed_pages or None, user_id))
        conn.commit()
    finally:
        conn.close()


def update_auth_user_password(user_id: int, password_hash: str, force_change: int = 1):
    sql = "UPDATE auth_users SET password_hash = %s, force_password_change = %s WHERE id = %s"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (password_hash, force_change, user_id))
        conn.commit()
    finally:
        conn.close()


def deactivate_auth_user(user_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE auth_users SET is_active = 0 WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        conn.close()


def sync_auth_users_from_crm():
    from app.auth.auth import hash_password  # local import to avoid circular import
    default_hash = hash_password('Welcome1!')
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Insert missing active CRM agents
            cur.execute("""
                INSERT INTO auth_users (crm_user_id, email, full_name, password_hash, role, force_password_change)
                SELECT
                    c.id,
                    COALESCE(NULLIF(TRIM(c.email), ''), c.id::text || '@agent.local'),
                    COALESCE(NULLIF(TRIM(c.agent_name), ''), NULLIF(TRIM(c.full_name), ''), c.id::text),
                    %s,
                    'agent',
                    1
                FROM crm_users c
                WHERE c.status = 'Active'
                  AND NOT EXISTS (
                      SELECT 1 FROM auth_users a WHERE a.crm_user_id = c.id
                  )
                ON CONFLICT (email) DO NOTHING
            """, (default_hash,))

            # Deactivate auth users whose CRM user is no longer active
            cur.execute("""
                UPDATE auth_users a
                SET is_active = 0
                FROM crm_users c
                WHERE a.crm_user_id = c.id
                  AND c.status != 'Active'
                  AND a.is_active = 1
                  AND a.role = 'agent'
            """)
        conn.commit()
    finally:
        conn.close()


def fetch_report_data(role_filter: dict = None) -> pd.DataFrame:
    base_sql = """
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
    if role_filter is None or role_filter.get('is_full_access'):
        conn = get_connection()
        try:
            return pd.read_sql(base_sql, conn)
        finally:
            conn.close()

    filter_type = role_filter.get('filter_type')
    if filter_type == 'agent':
        crm_user_id = role_filter['crm_params'][0]
        sql = """
            SELECT
                agent_id,
                full_name,
                SUM(ftc)  AS total_ftc,
                SUM(net)  AS total_net,
                COUNT(*)  AS trading_days
            FROM agent_performance
            WHERE DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE)
              AND agent_id = %s
            GROUP BY agent_id, full_name
            ORDER BY total_net DESC
        """
        conn = get_connection()
        try:
            return pd.read_sql(sql, conn, params=(str(crm_user_id),))
        finally:
            conn.close()

    # filter_type == 'crm': join crm_users and apply where fragment
    crm_where = role_filter['crm_where'].replace('u.', 'c.')
    params = role_filter['crm_params']
    sql = f"""
        SELECT
            ap.agent_id,
            ap.full_name,
            SUM(ap.ftc)  AS total_ftc,
            SUM(ap.net)  AS total_net,
            COUNT(*)     AS trading_days
        FROM agent_performance ap
        JOIN crm_users c ON c.id::text = ap.agent_id
        WHERE DATE_TRUNC('month', ap.report_date) = DATE_TRUNC('month', CURRENT_DATE)
          {crm_where}
        GROUP BY ap.agent_id, ap.full_name
        ORDER BY total_net DESC
    """
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
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
    from datetime import datetime
    if cutoff_used is None:
        cutoff_used = datetime(1970, 1, 1)
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


def truncate_crm_users():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE crm_users")
        conn.commit()
    finally:
        conn.close()


def upsert_crm_users(df: pd.DataFrame):
    cols = [
        "id", "email", "full_name", "status", "first_name", "last_name",
        "role_id", "desk_id", "language", "last_logon_time", "last_update_time",
        "desk_name", "team", "department", "desk", "type", "office_id", "office", "position",
        "office_name", "agent_name", "department_",
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
            # Preserve manual overrides for team leader agents whose CRM
            # classification differs from their actual Sales role.
            cur.execute("""
                UPDATE crm_users SET department_='Sales', team='Conversion', office='LAG-NG', office_name='LAG Nigeria'
                WHERE id IN (3750, 3614);
                UPDATE crm_users SET department_='Sales', team='Conversion', office='GMT', office_name='GMT'
                WHERE id = 6119;
                UPDATE crm_users SET department_='Sales', team='Conversion', office='ABJ-NG', office_name='ABJ Nigeria'
                WHERE id = 6479;
                UPDATE crm_users SET department_='Sales', team='Conversion', office='SA', office_name='South Africa'
                WHERE id = 6492;
            """)
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


def upsert_campaigns(df: pd.DataFrame):
    cols = [
        "crmid", "campaign_id", "campaign_name", "campaign_legacy_id",
        "campaign_description", "campaign_channel", "campaign_sub_channel",
        "website", "active", "start_date", "assigned_to", "disable_email_verification",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c != "crmid"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO campaigns ({col_list})
        VALUES %s
        ON CONFLICT (crmid) DO UPDATE SET
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


def fetch_campaigns_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                                        AS total_records,
            MAX(synced_at)                                  AS last_synced_at,
            COUNT(*) FILTER (WHERE active = 1)              AS active_campaigns,
            COUNT(DISTINCT campaign_channel)                AS unique_channels
        FROM campaigns
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":    row[0] or 0,
                "last_synced_at":   row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "active_campaigns": row[2] or 0,
                "unique_channels":  row[3] or 0,
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
        "transaction_promo", "assisted_by", "deleted", "is_frd", "transactiontypename",
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


def update_transactiontypename(df) -> int:
    """Batch UPDATE transaction_type_name for rows matched by mttransactionsid. Returns rows updated."""
    if df is None or len(df) == 0:
        return 0
    rows = [
        (str(r["transaction_type_name"]) if r["transaction_type_name"] is not None else None,
         int(r["mttransactionsid"]))
        for _, r in df.iterrows()
        if r["mttransactionsid"] is not None
    ]
    if not rows:
        return 0
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                UPDATE transactions SET transaction_type_name = data.ttn
                FROM (VALUES %s) AS data(ttn, id)
                WHERE transactions.mttransactionsid = data.id::bigint
            """, rows, template="(%s, %s)")
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _compute_type_name_batch(id_filter: str) -> int:
    """Run the transaction_type_name CASE UPDATE for a specific id_filter clause."""
    sql = f"""
        UPDATE transactions t
        SET transaction_type_name = CASE
            -- IB Commission
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'IB Commission'
                 AND t.created_time <= '2023-01-01' THEN 'IB Commission'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Commission'
                 AND (t.payment_subtype = 'IB Commission'
                      OR sub.mt4_comment = 'Affiliate Payment To Trading Pl')
                 AND t.created_time <= '2023-01-01' THEN 'IB Commission'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'IB Commission'
                 AND t.created_time <= '2023-01-01' THEN 'IB Commission Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Commission'
                 AND t.payment_subtype = 'IB Commission'
                 AND t.created_time <= '2023-01-01' THEN 'IB Commission Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'IB commission'
                 AND sub.mt4_comment IN ('IB Commission PayOut Cancelled', 'IB Commission PayOut Void')
                THEN 'IB Commission PayOut Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'IB commission'
                 AND sub.mt4_comment = 'IB Commission'
                THEN 'IB Commission'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'IB commission'
                 AND t.created_time <= '2023-01-01'
                THEN 'IB Commission PayOut'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'IB commission'
                 AND sub.mt4_comment = 'IB Commission Void'
                THEN 'IB Commission Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'IB commission'
                 AND sub.mt4_comment = 'IB Commission PayOut'
                THEN 'IB Commission PayOut'
            -- FRF Commission
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Commission'
                 AND t.payment_subtype = 'FRF Commission'
                 AND t.created_time <= '2022-12-05' THEN 'FRF Commission'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'FRF Commission'
                 AND t.created_time <= '2022-12-05' THEN 'FRF Commission'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Commission'
                 AND t.payment_subtype = 'FRF Commission'
                 AND t.created_time <= '2022-12-05' THEN 'FRF Commission Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'FRF Commission'
                 AND t.created_time <= '2022-12-05' THEN 'FRF Commission Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'FRF commission'
                 AND t.created_time >= '2022-12-05'
                 AND sub.mt4_comment IN ('FRF PayOut Cancelled', 'FRF PayOut Void')
                THEN 'FRF Commission PayOutCancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'FRF commission'
                 AND t.created_time >= '2022-12-05'
                 AND sub.mt4_comment = 'FRF - Transfer to Balance'
                THEN 'FRF Commission Transfer to Balance'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'FRF commission'
                 AND t.created_time >= '2022-12-05'
                 AND sub.mt4_comment = 'FRF - Transfer to Balance Void'
                THEN 'FRF Commission Transfer to Balance Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'FRF commission'
                 AND t.created_time >= '2022-12-05'
                 AND sub.mt4_comment = 'FRF PayOut'
                THEN 'FRF Commission PayOut'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'FRF commission'
                 AND t.created_time < '2022-12-05'
                THEN 'FRF Commission Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'FRF commission'
                 AND t.created_time < '2022-12-05'
                THEN 'FRF Commission'
            WHEN t.transactiontype = 'Credit In'
                 AND (sub.mt4_comment ILIKE '%FRF Commission%'
                      OR sub.mt4_comment ILIKE '%Refer a friend%'
                      OR sub.mt4_comment ILIKE '%FRF Bonus%'
                      OR sub.mt4_comment = 'FRF')
                THEN 'FRF Commission'
            WHEN t.transactiontype = 'Credit Out'
                 AND (sub.mt4_comment ILIKE '%FRF Commission%'
                      OR sub.mt4_comment ILIKE '%Refer a friend%'
                      OR sub.mt4_comment ILIKE '%FRF Bonus%'
                      OR sub.mt4_comment = 'FRF Void')
                THEN 'FRF Commission Cancelled'
            -- Credit Advance
            WHEN t.transactiontype = 'Credit In' AND sub.mt4_comment ILIKE '%advance%'
                THEN 'Credit In Advance'
            WHEN t.transactiontype = 'Credit Out' AND sub.mt4_comment ILIKE '%advance%'
                THEN 'Credit In Advance Cancelled'
            -- Bonus
            WHEN (t.transactiontype = 'Deposit' AND t.payment_method = 'Bonus'
                  AND t.created_time <= '2022-11-01')
              OR (t.transactiontype = 'Deposit' AND t.payment_method = 'Bonus'
                  AND sub.mt4_comment IN ('Deposit,Bonus', 'Bonus', 'CashBack Bonus'))
                THEN 'Bonus'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Bonus'
                 AND sub.mt4_comment LIKE '%Transfer to Balance%'
                THEN 'Bonus Transfer to Balance'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Bonus'
                 AND sub.mt4_comment LIKE 'Bonus Transfer to Balance Cancel%'
                THEN 'Bonus Transfer to Balance Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Bonus'
                 AND sub.mt4_comment IN ('Bonus PayOut Cancelled', 'Bonus PayOut Void')
                THEN 'Bonus PayOut Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Bonus'
                 AND t.created_time <= '2022-11-01'
                THEN 'BonusCancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Bonus'
                 AND sub.mt4_comment = 'Bonus PayOut'
                THEN 'Bonus PayOut'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Bonus'
                THEN 'BonusCancelled'
            WHEN t.transactiontype = 'Credit In' THEN 'Bonus'
            WHEN t.transactiontype = 'Credit Out' THEN 'BonusCancelled'
            -- Deposits and Withdrawals
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'Deposit Void'
                 AND t.created_time <= '2023-01-01' THEN 'Deposit Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'Withdrawal Void'
                 AND t.created_time <= '2023-01-01' THEN 'Withdrawal Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%'
                 AND t.payment_method IN ('Credit card', 'Electronic payment',
                                          'Wire transfer', 'CryptoWallet', 'Cash')
                 AND sub.mt4_comment IN ('Deposit Cancelled', 'Deposit Void')
                THEN 'Deposit Cancelled'
            WHEN t.transactiontype = 'Deposit'
                 AND t.payment_method IN ('Credit card', 'Electronic payment',
                                          'Wire transfer', 'CryptoWallet', 'Cash')
                 AND sub.mt4_comment IN ('Withdrawal Cancelled', 'Withdrawal Void')
                THEN 'Withdrawal Cancelled'
            WHEN t.transactiontype = 'Deposit'
                 AND t.payment_method IN ('None', 'Wire', 'Credit card', 'ElectronicPayment',
                                          'Electronic payment', 'Wire transfer', 'Crypto',
                                          'CryptoWallet', 'Cash', 'External', 'CreditCard')
                THEN 'Deposit'
            WHEN t.transactiontype LIKE 'Withdraw%'
                 AND t.payment_method IN ('None', 'Wire', 'Credit card', 'ElectronicPayment',
                                          'Electronic payment', 'Wire transfer', 'Crypto',
                                          'CryptoWallet', 'Cash', 'External', 'CreditCard')
                THEN 'Withdrawal'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Chargeback'
                THEN 'Charge Back Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Chargeback'
                THEN 'Charge Back'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Processing fees'
                THEN 'Fee Cancelled'
            WHEN t.transactiontype = 'Fee'
              OR (t.transactiontype LIKE 'Withdraw%'
                  AND t.payment_method IN ('Processing fees', 'Fee', 'ProcessingFee'))
                THEN 'Fee'
            WHEN (t.transactiontype = 'Deposit' AND t.payment_method = 'Transfer')
              OR t.transactiontype = 'TransferIn'
                THEN 'Transfer To Account'
            WHEN (t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Transfer')
              OR t.transactiontype = 'TransferOut'
                THEN 'Transfer From Account'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'Gift'
                 AND t.created_time < '2023-01-01' THEN 'Gift'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Commission'
                 AND t.payment_subtype = 'Gift'
                 AND t.created_time < '2023-01-01' THEN 'Gift'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                 AND t.payment_subtype = 'Gift'
                 AND t.created_time < '2023-01-01' THEN 'Gift Cancelled'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Commission'
                 AND t.payment_subtype = 'Gift'
                 AND t.created_time < '2023-01-01' THEN 'Gift Cancelled'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                 AND sub.mt4_comment ILIKE '%transfer%'
                THEN 'Transfer To Account'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                 AND sub.mt4_comment ILIKE '%transfer%'
                THEN 'Transfer From Account'
            WHEN t.transactiontype = 'Deposit' AND t.payment_method = 'Adjustment'
                THEN 'Positive Trading Adjustment'
            WHEN t.transactiontype LIKE 'Withdraw%' AND t.payment_method = 'Adjustment'
                THEN 'Negative Trading Adjustment'
            ELSE 'Not Set'
        END
        FROM (
            SELECT t2.ctid, m.comment AS mt4_comment
            FROM transactions t2
            LEFT JOIN dealio_trades_mt4 m
                ON m.cmd = 6
                AND t2.mtorder_id IS NOT NULL
                AND t2.mtorder_id ~ '^[0-9]+(\.[0-9]*)?$'
                AND CASE WHEN t2.mtorder_id ~ '^[0-9]+(\.[0-9]*)?$'
                         THEN t2.mtorder_id::numeric::bigint
                         ELSE NULL
                    END = m.ticket
            {id_filter}
        ) sub
        WHERE t.ctid = sub.ctid
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def compute_transaction_type_name(ids: list = None) -> int:
    """Compute and UPDATE transaction_type_name using CASE logic.

    Incremental (ids provided): UPDATE for those mttransactionsids + NULL-mttransactionsid rows
    with no type yet (broker_banking records).
    Full backfill (ids=None): mttransactionsid-range chunks + one NULL-mttransactionsid batch.
    Returns total rows updated.
    """
    if ids is not None:
        if len(ids) == 0:
            return 0
        id_list = ",".join(str(int(i)) for i in ids)
        n = _compute_type_name_batch(f"WHERE t2.mttransactionsid IN ({id_list})")
        # broker_banking rows have NULL mttransactionsid — process those too
        n += _compute_type_name_batch(
            "WHERE t2.mttransactionsid IS NULL AND t2.transaction_type_name IS NULL"
        )
        return n

    # Full backfill — mttransactionsid-range chunks for non-NULL rows
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(mttransactionsid), MAX(mttransactionsid) FROM transactions WHERE mttransactionsid IS NOT NULL")
            min_id, max_id = cur.fetchone()
    finally:
        conn.close()

    total = 0
    if min_id is not None:
        min_id, max_id = int(min_id), int(max_id)
        chunk_size = 50_000
        current = min_id
        while current <= max_id:
            chunk_end = min(current + chunk_size - 1, max_id)
            total += _compute_type_name_batch(
                f"WHERE t2.mttransactionsid BETWEEN {current} AND {chunk_end}"
            )
            print(f"[transaction_type_name backfill] up to {chunk_end}/{max_id}, updated {total} rows")
            current = chunk_end + 1

    # One batch for NULL mttransactionsid (broker_banking / Antilope records)
    total += _compute_type_name_batch("WHERE t2.mttransactionsid IS NULL")
    print(f"[transaction_type_name backfill] NULL mttransactionsid batch done, total {total}")

    return total


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


def upsert_trading_accounts(df: pd.DataFrame):
    cols = [
        "trading_account_id", "trading_account_name", "vtigeraccountid", "trade_group",
        "last_update", "equity", "open_pnl", "total_pnl", "commission",
        "enable", "enable_read_only", "login", "currency", "serverid", "assigned_to",
        "balance", "credit", "swaps", "total_taxes", "leverage", "margin",
        "margin_level", "margin_free", "created_time", "trading_server_created_timestamp",
        "platform", "deleted",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c != "trading_account_id"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO trading_accounts ({col_list})
        VALUES %s
        ON CONFLICT (trading_account_id) DO UPDATE SET
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


def fetch_trading_accounts_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                            AS total_records,
            MAX(synced_at)                      AS last_synced_at,
            COUNT(DISTINCT login)               AS unique_logins,
            COALESCE(SUM(balance), 0)           AS total_balance,
            COALESCE(SUM(equity), 0)            AS total_equity
        FROM trading_accounts
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":  row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_logins":  row[2] or 0,
                "total_balance":  int(row[3] or 0),
                "total_equity":   int(row[4] or 0),
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


def get_last_sync_times() -> dict:
    """Return most recent successful sync timestamp (ISO string) per table."""
    sql = """
        SELECT DISTINCT ON (table_name) table_name, ran_at
        FROM sync_log
        WHERE status = 'success'
        ORDER BY table_name, ran_at DESC
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return {row[0]: row[1].isoformat() if row[1] else None for row in rows}
    finally:
        conn.close()


def truncate_and_insert_ftd100() -> int:
    """Full refresh: TRUNCATE ftd100_clients then INSERT from CTE computed entirely within PostgreSQL."""
    cte_sql = """
        WITH ordered_tx AS (
            SELECT
                t.vtigeraccountid AS accountid,
                t.confirmation_time,
                t.original_deposit_owner,
                a.assigned_to,
                a.accountstatus,
                a.client_qualification_date,
                SUM(
                    CASE
                        WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled') THEN t.usdamount
                        WHEN t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY t.vtigeraccountid
                    ORDER BY t.confirmation_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_total,
                ROW_NUMBER() OVER (
                    PARTITION BY t.vtigeraccountid
                    ORDER BY t.confirmation_time DESC
                ) AS rn_desc
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND a.is_test_account = 0
        ),
        agg AS (
            SELECT
                accountid,
                MAX(CASE WHEN rn_desc = 1 THEN confirmation_time END) AS latest_time,
                MAX(CASE WHEN rn_desc = 1 THEN running_total END)     AS latest_running_total,
                MIN(CASE WHEN running_total >= 95 THEN confirmation_time END) AS ftd_time
            FROM ordered_tx
            GROUP BY accountid
        ),
        net_latest AS (
            SELECT DISTINCT ON (o.accountid)
                o.accountid,
                o.confirmation_time      AS latest_tx_time,
                o.running_total          AS net_deposits_current,
                o.original_deposit_owner AS latest_original_deposit_owner
            FROM ordered_tx o
            JOIN agg a ON a.accountid = o.accountid AND a.latest_time = o.confirmation_time
            ORDER BY o.accountid, o.confirmation_time DESC
        ),
        cutoff AS (
            SELECT
                acc.accountid, acc.accountstatus, acc.client_qualification_date,
                acc.assigned_to, a.latest_running_total, a.ftd_time,
                CASE
                    WHEN acc.client_qualification_date IS NULL THEN NULL
                    WHEN a.ftd_time IS NULL THEN acc.client_qualification_date
                    ELSE GREATEST(acc.client_qualification_date, a.ftd_time)
                END AS effective_cutoff_time
            FROM accounts acc
            JOIN agg a ON a.accountid = acc.accountid
            WHERE acc.is_test_account = 0
        ),
        net_at_effective_cutoff AS (
            SELECT accountid, confirmation_time AS cutoff_tx_time,
                   running_total AS net_at_cutoff, original_deposit_owner
            FROM (
                SELECT o.accountid, o.confirmation_time, o.running_total, o.original_deposit_owner,
                       ROW_NUMBER() OVER (PARTITION BY o.accountid ORDER BY o.confirmation_time DESC) AS rn
                FROM ordered_tx o
                JOIN cutoff c ON c.accountid = o.accountid
                WHERE c.effective_cutoff_time IS NOT NULL
                  AND o.confirmation_time <= c.effective_cutoff_time
            ) x
            WHERE rn = 1
        )
        INSERT INTO ftd100_clients (
            accountid, accountstatus, client_qualification_date, assigned_to,
            ftd_100_date, ftd_100_amount, original_deposit_owner,
            net_deposits_current, net_until_qualification, synced_at
        )
        SELECT
            c.accountid, c.accountstatus, c.client_qualification_date, c.assigned_to,
            CASE WHEN c.accountstatus = 'Sales' THEN nl.latest_tx_time   ELSE ne.cutoff_tx_time  END AS ftd_100_date,
            CASE WHEN c.accountstatus = 'Sales' THEN nl.net_deposits_current ELSE ne.net_at_cutoff END AS ftd_100_amount,
            CASE WHEN c.accountstatus = 'Sales' THEN nl.latest_original_deposit_owner ELSE ne.original_deposit_owner END AS original_deposit_owner,
            nl.net_deposits_current,
            CASE
                WHEN c.accountstatus = 'Sales' OR c.client_qualification_date IS NULL THEN nl.net_deposits_current
                ELSE ne.net_at_cutoff
            END AS net_until_qualification,
            NOW()
        FROM cutoff c
        LEFT JOIN net_latest nl              ON nl.accountid = c.accountid
        LEFT JOIN net_at_effective_cutoff ne ON ne.accountid = c.accountid
        WHERE
            (c.accountstatus = 'Sales'  AND nl.net_deposits_current >= 95)
         OR (c.accountstatus <> 'Sales' AND ne.net_at_cutoff >= 95)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ftd100_clients")
            cur.execute(cte_sql)
            rows = cur.rowcount
        conn.commit()
        return rows
    finally:
        conn.close()


def fetch_ftd100_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                                                    AS total_records,
            MAX(synced_at)                                              AS last_synced_at,
            COUNT(*) FILTER (WHERE accountstatus = 'Sales')            AS sales_count,
            COUNT(*) FILTER (WHERE accountstatus <> 'Sales')           AS retention_count,
            COALESCE(SUM(net_deposits_current), 0)                     AS total_net_deposits
        FROM ftd100_clients
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":      row[0] or 0,
                "last_synced_at":     row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "sales_count":        row[2] or 0,
                "retention_count":    row[3] or 0,
                "total_net_deposits": int(row[4] or 0),
            }
    finally:
        conn.close()


def ensure_client_classification_table():
    sql = """
        CREATE TABLE IF NOT EXISTS client_classification (
            accountid               BIGINT PRIMARY KEY,
            classification_category VARCHAR(20) NOT NULL,
            synced_at               TIMESTAMP DEFAULT NOW()
        );
        ALTER TABLE client_classification ADD COLUMN IF NOT EXISTS classification_value SMALLINT;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def upsert_client_classification(df: pd.DataFrame):
    def _category(val):
        try:
            v = int(val)
            if 1 <= v <= 5:
                return 'Low Quality'
            if 6 <= v <= 10:
                return 'High Quality'
        except (TypeError, ValueError):
            pass
        return 'No segmentation'

    def _raw_value(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    rows = []
    for _, row in df.iterrows():
        accountid = _clean(row.get('accountid'))
        if accountid is None:
            continue
        raw_val = row.get('client_classification')
        category = _category(raw_val)
        numeric_val = _raw_value(raw_val)
        rows.append((int(accountid), category, numeric_val))

    if not rows:
        return

    sql = """
        INSERT INTO client_classification (accountid, classification_category, classification_value, synced_at)
        VALUES %s
        ON CONFLICT (accountid) DO UPDATE SET
            classification_category = EXCLUDED.classification_category,
            classification_value    = EXCLUDED.classification_value,
            synced_at               = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()


def fetch_users_with_targets(role_filter: dict = None) -> pd.DataFrame:
    base_sql = """
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
    if role_filter is None or role_filter.get('is_full_access'):
        conn = get_connection()
        try:
            return pd.read_sql(base_sql, conn)
        finally:
            conn.close()

    filter_type = role_filter.get('filter_type')
    if filter_type == 'agent':
        crm_user_id = role_filter['crm_params'][0]
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
              AND u.id = %s
            ORDER BY total_net DESC
        """
        conn = get_connection()
        try:
            return pd.read_sql(sql, conn, params=(str(crm_user_id),))
        finally:
            conn.close()

    # filter_type == 'crm': rename users alias to uu, join crm_users as c
    crm_where = role_filter['crm_where'].replace('u.', 'c.')
    params = role_filter['crm_params']
    sql = f"""
        SELECT
            uu.id,
            uu.full_name,
            uu.email,
            uu.position,
            uu.office,
            uu.team,
            uu.department,
            uu.desk_name,
            uu.status,
            uu.last_logon_time,
            COALESCE(ap.total_ftc, 0) AS total_ftc,
            COALESCE(ap.total_net, 0) AS total_net,
            COALESCE(ap.trading_days, 0) AS trading_days
        FROM users uu
        JOIN crm_users c ON c.id = uu.id::BIGINT
        LEFT JOIN (
            SELECT
                agent_id,
                SUM(ftc)  AS total_ftc,
                SUM(net)  AS total_net,
                COUNT(*)  AS trading_days
            FROM agent_performance
            WHERE DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY agent_id
        ) ap ON uu.id = ap.agent_id
        WHERE uu.status = 'Active'
          {crm_where}
        ORDER BY total_net DESC
    """
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


# ── Dealio Users (from dealio PG replica) ────────────────────────────────────

def upsert_dealio_users(df: pd.DataFrame):
    import time as _time
    cols = [
        "login", "sourceid", "sourcename", "sourcetype",
        "groupname", "groupcurrency", "name", "email",
        "country", "city", "zipcode", "address", "phone", "comment",
        "balance", "credit", "leverage", "status",
        "regdate", "lastdate", "lastupdate", "agentaccount", "isenabled",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c not in ("login", "sourceid")]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO dealio_users ({col_list})
        VALUES %s
        ON CONFLICT (login, sourceid) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    for attempt in range(3):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
            conn.commit()
            return
        except Exception as e:
            conn.rollback()
            if "deadlock" in str(e).lower() and attempt < 2:
                _time.sleep(5)
                continue
            raise
        finally:
            conn.close()


def fetch_dealio_users_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                             AS total_records,
            MAX(synced_at)                       AS last_synced_at,
            COUNT(DISTINCT groupname)            AS unique_groups,
            COUNT(DISTINCT groupcurrency)        AS unique_currencies,
            COUNT(*) FILTER (WHERE balance > 0)  AS users_with_balance
        FROM dealio_users
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":      row[0] or 0,
                "last_synced_at":     row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_groups":      row[2] or 0,
                "unique_currencies":  row[3] or 0,
                "users_with_balance": row[4] or 0,
            }
    finally:
        conn.close()


# ── Dealio Trades MT4 (from dealio PG replica) ───────────────────────────────

def upsert_dealio_trades_mt4(df: pd.DataFrame):
    import time as _time
    cols = [
        "ticket", "source_id", "login", "cmd", "volume",
        "open_time", "close_time", "last_modified", "profit", "computed_profit",
        "symbol", "core_symbol", "book", "open_price", "close_price",
        "commission", "swaps", "comment", "group_name", "group_currency",
        "source_name", "source_type", "reason",
        "notional_value", "computed_swap", "computed_commission", "spread",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c not in ("ticket", "source_id")]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO dealio_trades_mt4 ({col_list})
        VALUES %s
        ON CONFLICT (source_id, ticket) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    for attempt in range(3):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
            conn.commit()
            return
        except Exception as e:
            conn.rollback()
            if "deadlock" in str(e).lower() and attempt < 2:
                _time.sleep(5)
                continue
            raise
        finally:
            conn.close()


def truncate_dealio_trades_mt4():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dealio_trades_mt4")
        conn.commit()
    finally:
        conn.close()


def upsert_dealio_trades_mt5(df: pd.DataFrame):
    import time as _time
    cols = [
        "ticket", "source_id", "login", "symbol", "digit", "cmd", "volume",
        "open_time", "open_price", "close_time", "close_price",
        "reason", "commission", "agent_id", "swap", "profit", "comment",
        "computed_profit", "computed_swap", "computed_commission",
        "group_name", "group_currency", "book", "notional_value",
        "source_name", "source_type", "position_id", "entry", "volume_closed",
        "sync_time", "is_finalized", "spread", "conversion_rate",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c not in ("ticket", "source_id")]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO dealio_trades_mt5 ({col_list})
        VALUES %s
        ON CONFLICT (source_id, ticket) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    for attempt in range(3):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
            conn.commit()
            return
        except Exception as e:
            conn.rollback()
            if "deadlock" in str(e).lower() and attempt < 2:
                _time.sleep(5)
                continue
            raise
        finally:
            conn.close()


def truncate_dealio_trades_mt5():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dealio_trades_mt5")
        conn.commit()
    finally:
        conn.close()


# ── dealio_positions ──────────────────────────────────────────────────────────

def ensure_dealio_positions_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dealio_positions (
                    id                   BIGINT PRIMARY KEY,
                    login                BIGINT,
                    cmd                  SMALLINT,
                    volume               DOUBLE PRECISION,
                    symbol               TEXT,
                    core_symbol          TEXT,
                    book                 TEXT,
                    open_price           DOUBLE PRECISION,
                    close_price          DOUBLE PRECISION,
                    profit               DOUBLE PRECISION,
                    computed_profit      DOUBLE PRECISION,
                    swap                 DOUBLE PRECISION,
                    computed_swap        DOUBLE PRECISION,
                    commission           DOUBLE PRECISION,
                    computed_commission  DOUBLE PRECISION,
                    comment              TEXT,
                    group_name           TEXT,
                    group_currency       TEXT,
                    notional_value       DOUBLE PRECISION,
                    contract_size        DOUBLE PRECISION,
                    source_name          TEXT,
                    source_type          TEXT,
                    source_id            TEXT,
                    open_time            TIMESTAMP,
                    last_update          TIMESTAMPTZ,
                    reason               INTEGER,
                    conversion_rate      DOUBLE PRECISION,
                    calculation_currency TEXT,
                    currency_base        TEXT,
                    currency_profit      TEXT,
                    exposure_base        DOUBLE PRECISION,
                    exposure_profit      DOUBLE PRECISION,
                    synced_at            TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dealio_positions_login ON dealio_positions(login)"
            )
        conn.commit()
    finally:
        conn.close()


def truncate_and_insert_dealio_positions(df: pd.DataFrame) -> int:
    cols = [
        "id", "login", "cmd", "volume", "symbol", "core_symbol", "book",
        "open_price", "close_price", "profit", "computed_profit",
        "swap", "computed_swap", "commission", "computed_commission",
        "comment", "group_name", "group_currency", "notional_value", "contract_size",
        "source_name", "source_type", "source_id", "open_time", "last_update",
        "reason", "conversion_rate", "calculation_currency",
        "currency_base", "currency_profit", "exposure_base", "exposure_profit",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    col_list = ", ".join(cols)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dealio_positions")
            if rows:
                execute_values(
                    cur,
                    f"INSERT INTO dealio_positions ({col_list}) VALUES %s",
                    rows,
                )
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_dealio_trades_mt4_stats() -> dict:
    sql = """
        SELECT
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'dealio_trades_mt4') AS total_records,
            MAX(synced_at)            AS last_synced_at,
            COUNT(DISTINCT login)     AS unique_logins,
            COALESCE(SUM(profit), 0)  AS total_profit,
            COUNT(DISTINCT symbol)    AS unique_symbols
        FROM dealio_trades_mt4
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":  row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_logins":  row[2] or 0,
                "total_profit":   int(row[3] or 0),
                "unique_symbols": row[4] or 0,
            }
    finally:
        conn.close()


def fetch_dealio_trades_mt5_stats() -> dict:
    sql = """
        SELECT
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'dealio_trades_mt5') AS total_records,
            MAX(synced_at)            AS last_synced_at,
            COUNT(DISTINCT login)     AS unique_logins,
            COALESCE(SUM(profit), 0)  AS total_profit,
            COUNT(DISTINCT symbol)    AS unique_symbols
        FROM dealio_trades_mt5
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":  row[0] or 0,
                "last_synced_at": row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_logins":  row[2] or 0,
                "total_profit":   int(row[3] or 0),
                "unique_symbols": row[4] or 0,
            }
    finally:
        conn.close()


# ── Dealio Daily Profits (from dealio PG replica) ─────────────────────────────

def upsert_dealio_daily_profits(df: pd.DataFrame):
    cols = [
        "date", "login", "sourceid", "sourcename", "sourcetype", "book",
        "closedpnl", "convertedclosedpnl", "calculationcurrency",
        "floatingpnl", "convertedfloatingpnl", "netdeposit", "convertednetdeposit",
        "equity", "convertedequity", "balance", "convertedbalance",
        "groupcurrency", "conversionratio", "equityprevday", "groupname",
        "deltafloatingpnl", "converteddeltafloatingpnl",
    ]
    rows = [tuple(_clean(row.get(c)) for c in cols) for _, row in df.iterrows()]
    update_cols = [c for c in cols if c not in ("date", "login", "sourceid")]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_list = ", ".join(cols)
    sql = f"""
        INSERT INTO dealio_daily_profits ({col_list})
        VALUES %s
        ON CONFLICT (date, login, sourceid) DO UPDATE SET
            {update_set},
            synced_at = NOW()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_daily_equity_zeroed_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_equity_zeroed (
                    id                  SERIAL PRIMARY KEY,
                    login               INTEGER NOT NULL,
                    day                 DATE NOT NULL,
                    end_equity_zeroed   NUMERIC(18, 2),
                    start_equity_zeroed NUMERIC(18, 2),
                    created_at          TIMESTAMP DEFAULT NOW(),
                    UNIQUE(login, day)
                );
                CREATE INDEX IF NOT EXISTS idx_daily_equity_zeroed_login_day
                    ON daily_equity_zeroed (login, day);
            """)
            conn.commit()
    finally:
        conn.close()


def upsert_daily_equity_zeroed(rows: list[tuple], snapshot_date: str):
    """
    Upsert (login, end_equity_zeroed, start_equity_zeroed) for snapshot_date.
    start_equity_zeroed is independently calculated from snapshot_date - 1
    using the same EEZ formula (not derived from end_equity_zeroed).

    rows: list of (login, end_equity_zeroed, start_equity_zeroed)
    snapshot_date: 'YYYY-MM-DD' string
    """
    if not rows:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO daily_equity_zeroed (login, day, end_equity_zeroed, start_equity_zeroed)
                VALUES %s
                ON CONFLICT (login, day) DO UPDATE
                    SET end_equity_zeroed   = EXCLUDED.end_equity_zeroed,
                        start_equity_zeroed = EXCLUDED.start_equity_zeroed,
                        created_at          = NOW()
            """, [(login, snapshot_date, end_eez, start_eez) for login, end_eez, start_eez in rows])

            conn.commit()
    finally:
        conn.close()


def fetch_dealio_daily_profits_stats() -> dict:
    sql = """
        SELECT
            COUNT(*)                                  AS total_records,
            MAX(synced_at)                            AS last_synced_at,
            COUNT(DISTINCT login)                     AS unique_logins,
            COALESCE(SUM(convertedclosedpnl), 0)      AS total_closed_pnl,
            COUNT(DISTINCT date::date)                AS unique_dates
        FROM dealio_daily_profits
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_records":    row[0] or 0,
                "last_synced_at":   row[1].strftime("%Y-%m-%d %H:%M:%S") if row[1] else "Never",
                "unique_logins":    row[2] or 0,
                "total_closed_pnl": int(row[3] or 0),
                "unique_dates":     row[4] or 0,
            }
    finally:
        conn.close()


# =============================================================================
# Materialized view helpers
# =============================================================================

_MV_SETUP_SQL = [
    # ── mv_daily_kpis ────────────────────────────────────────────────────────
    # Drop first so definition changes (e.g. bonus filter removal) take effect on restart.
    # CASCADE also drops mv_run_rate which depends on it.
    "DROP MATERIALIZED VIEW IF EXISTS mv_run_rate CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_daily_kpis CASCADE",
    """
    CREATE MATERIALIZED VIEW mv_daily_kpis AS
    SELECT
        t.original_deposit_owner                                             AS agent_id,
        t.confirmation_time::date                                            AS tx_date,
        a.client_qualification_date::date                                    AS qual_date,
        COALESCE(SUM(CASE
            WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')  THEN  t.usdamount
            WHEN t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled')  THEN -t.usdamount
        END), 0)                                                             AS net_usd,
        COALESCE(SUM(CASE
            WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')  THEN t.usdamount
            ELSE 0
        END), 0)                                                             AS deposit_usd,
        COALESCE(SUM(CASE
            WHEN t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled')  THEN t.usdamount
            ELSE 0
        END), 0)                                                             AS withdrawal_usd,
        SUM(CASE WHEN t.transaction_type_name = 'Deposit' AND t.ftd = 1 THEN 1 ELSE 0 END)::int AS ftd_count,
        COUNT(DISTINCT CASE WHEN t.transaction_type_name = 'Deposit' AND t.ftd = 1
                            THEN t.vtigeraccountid END)::int                 AS ftc_count
    FROM transactions t
    JOIN accounts  a  ON a.accountid = t.vtigeraccountid
    LEFT JOIN crm_users u ON u.id   = t.original_deposit_owner
    WHERE t.transactionapproval = 'Approved'
      AND (t.deleted = 0 OR t.deleted IS NULL)
      AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
      AND t.vtigeraccountid IS NOT NULL
      AND a.is_test_account = 0
      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
    GROUP BY t.original_deposit_owner, t.confirmation_time::date, a.client_qualification_date::date
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_kpis_u    ON mv_daily_kpis (COALESCE(agent_id, -1), tx_date, COALESCE(qual_date, '1900-01-01'::date))",
    "CREATE INDEX IF NOT EXISTS idx_mv_daily_kpis_tx_date     ON mv_daily_kpis (tx_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_daily_kpis_qual_date   ON mv_daily_kpis (qual_date) WHERE qual_date IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_mv_daily_kpis_agent       ON mv_daily_kpis (agent_id)",

    # ── mv_volume_stats (MT5 + open positions) ───────────────────────────────
    # Rename old MT4-based MV to backup (first deploy only — subsequent runs no-op via IF EXISTS)
    "ALTER MATERIALIZED VIEW IF EXISTS mv_volume_stats RENAME TO mv_volume_stats_mt4_backup",
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_volume_stats AS
    SELECT
        agent_id,
        accountid,
        open_date,
        SUM(notional_usd)                                               AS notional_usd,
        MAX(CASE WHEN notional_usd > 0 THEN 1 ELSE 0 END)::smallint    AS has_positive_notional
    FROM (
        -- Part 1: Currently open positions (dealio_positions)
        SELECT
            a.assigned_to       AS agent_id,
            ta.vtigeraccountid  AS accountid,
            p.open_time::date   AS open_date,
            p.notional_value    AS notional_usd
        FROM dealio_positions p
        JOIN trading_accounts ta ON ta.login::bigint = p.login
        JOIN accounts         a  ON a.accountid      = ta.vtigeraccountid
        LEFT JOIN crm_users   u  ON u.id             = a.assigned_to
        WHERE ta.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0
          AND p.open_time::date >= '2024-01-01'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'

        UNION ALL

        -- Part 2: Closed MT5 trades (exit deal joined to entry deal for open_time)
        SELECT
            a.assigned_to       AS agent_id,
            ta.vtigeraccountid  AS accountid,
            en.open_time::date  AS open_date,
            ex.notional_value   AS notional_usd
        FROM dealio_trades_mt5 ex
        JOIN dealio_trades_mt5 en
            ON en.position_id = ex.position_id
           AND en.source_id   = ex.source_id
           AND en.entry       = 0
        JOIN trading_accounts ta ON ta.login::bigint = ex.login
        JOIN accounts         a  ON a.accountid      = ta.vtigeraccountid
        LEFT JOIN crm_users   u  ON u.id             = a.assigned_to
        WHERE ex.entry = 1
          AND ex.close_time > '1971-01-01'
          AND ta.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0
          AND en.open_time::date >= '2024-01-01'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
    ) combined
    GROUP BY agent_id, accountid, open_date
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_volume_stats_u         ON mv_volume_stats (COALESCE(agent_id, -1), accountid, open_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_volume_stats_open_date        ON mv_volume_stats (open_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_volume_stats_agent            ON mv_volume_stats (agent_id)",

    # ── mv_sales_bonuses ───────────────────────────────────────────────────────────
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_bonuses AS
    SELECT
        f.original_deposit_owner                AS agent_id,
        f.ftd_100_date,
        COUNT(DISTINCT f.accountid)::int        AS ftd100_count,
        COUNT(DISTINCT CASE WHEN f.ftd_100_amount >= 240 THEN f.accountid END)::int AS ftd100_full_count,
        COUNT(DISTINCT CASE WHEN f.ftd_100_amount <  240 THEN f.accountid END)::int AS ftd100_half_count,
        COALESCE(SUM(f.net_until_qualification), 0) AS total_sales_net,
        COALESCE(SUM(CASE
            WHEN f.ftd_100_amount < 500   THEN 0
            WHEN f.ftd_100_amount < 1000  THEN 10
            WHEN f.ftd_100_amount < 5000  THEN 20
            ELSE 50
        END), 0)::float                         AS ftd_amount_bonus
    FROM ftd100_clients f
    WHERE f.original_deposit_owner IS NOT NULL
    GROUP BY f.original_deposit_owner, f.ftd_100_date
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_bon_u    ON mv_sales_bonuses (agent_id, ftd_100_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_sales_bon_date        ON mv_sales_bonuses (ftd_100_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_sales_bon_agent       ON mv_sales_bonuses (agent_id)",

    # ── mv_run_rate  (depends on mv_daily_kpis — must come last) ─────────────
    """
    CREATE MATERIALIZED VIEW mv_run_rate AS
    WITH tagged AS (
        SELECT k.agent_id, k.tx_date, k.qual_date,
               k.net_usd, k.deposit_usd, k.ftd_count, k.ftc_count,
               CASE
                   WHEN u.department_ = 'Sales'
                    AND u.team = 'Conversion'
                    AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%'
                    AND TRIM(COALESCE(u.full_name, ''))               NOT ILIKE 'test%'
                   THEN 'sales'
                   WHEN u.department_ = 'Retention'
                    AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%'
                    AND TRIM(COALESCE(u.full_name, ''))               NOT ILIKE 'test%'
                    AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Retention%'
                    AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Conversion%'
                    AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Support%'
                    AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%General%'
                   THEN 'retention'
                   ELSE 'other'
               END AS dept_group
        FROM mv_daily_kpis k
        JOIN crm_users u ON u.id = k.agent_id
    )
    SELECT dept_group, tx_date, qual_date,
           SUM(net_usd) AS net_usd, SUM(deposit_usd) AS deposit_usd,
           SUM(ftd_count) AS ftd_count, SUM(ftc_count) AS ftc_count
    FROM tagged GROUP BY dept_group, tx_date, qual_date
    UNION ALL
    SELECT 'all', tx_date, qual_date,
           SUM(net_usd), SUM(deposit_usd), SUM(ftd_count), SUM(ftc_count)
    FROM mv_daily_kpis GROUP BY tx_date, qual_date
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_run_rate_u      ON mv_run_rate (dept_group, tx_date, COALESCE(qual_date, '1900-01-01'::date))",
    "CREATE INDEX IF NOT EXISTS idx_mv_run_rate_dept          ON mv_run_rate (dept_group)",
    "CREATE INDEX IF NOT EXISTS idx_mv_run_rate_tx_date       ON mv_run_rate (tx_date)",
    "CREATE INDEX IF NOT EXISTS idx_mv_run_rate_qual          ON mv_run_rate (qual_date) WHERE qual_date IS NOT NULL",

    # ── mv_account_stats  (new leads + live accounts — today and MTD) ─────────
    # Drop first so the definition can be updated on restart
    "DROP MATERIALIZED VIEW IF EXISTS mv_account_stats CASCADE",
    """
    CREATE MATERIALIZED VIEW mv_account_stats AS
    SELECT
        1                                                                           AS id,
        COUNT(*) FILTER (WHERE createdtime::date = CURRENT_DATE)                    AS new_leads_today,
        COUNT(*) FILTER (WHERE createdtime >= date_trunc('month', CURRENT_DATE))    AS new_leads_month,
        COUNT(*) FILTER (WHERE createdtime::date = CURRENT_DATE AND birth_date IS NOT NULL) AS new_live_today,
        COUNT(*) FILTER (WHERE createdtime >= date_trunc('month', CURRENT_DATE)
                           AND birth_date IS NOT NULL)                              AS new_live_month
    FROM accounts
    WHERE is_test_account = 0
      AND (is_demo = 0 OR is_demo IS NULL)
      AND createdtime IS NOT NULL
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_account_stats_u ON mv_account_stats (id)",

    # ── mv_std_clients  (STD — second deposit after running total hits $240) ───
    "DROP MATERIALIZED VIEW IF EXISTS mv_std_clients CASCADE",
    """
    CREATE MATERIALIZED VIEW mv_std_clients AS
    WITH ordered_tx AS (
        SELECT
            t.vtigeraccountid AS accountid,
            t.confirmation_time,
            t.usdamount,
            SUM(t.usdamount) OVER (
                PARTITION BY t.vtigeraccountid
                ORDER BY t.confirmation_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_total
        FROM transactions t
        JOIN accounts a ON t.vtigeraccountid = a.accountid
        WHERE t.transactionapproval = 'Approved'
          AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND a.is_test_account = 0
    ),
    ftd_event AS (
        SELECT
            accountid,
            MIN(confirmation_time) AS ftd_240_date
        FROM ordered_tx
        WHERE running_total >= 240
        GROUP BY accountid
    ),
    second_tx AS (
        SELECT
            f.accountid,
            MIN(t.confirmation_time) AS second_deposit_date
        FROM ftd_event f
        JOIN transactions t ON t.vtigeraccountid = f.accountid
          AND t.confirmation_time > f.ftd_240_date
          AND t.transactionapproval = 'Approved'
          AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')
          AND (t.deleted = 0 OR t.deleted IS NULL)
        GROUP BY f.accountid
    )
    SELECT
        f.accountid,
        CASE WHEN s.second_deposit_date IS NOT NULL THEN 1 ELSE 0 END AS has_second_deposit,
        s.second_deposit_date,
        a.assigned_to
    FROM ftd_event f
    LEFT JOIN second_tx s ON f.accountid = s.accountid
    LEFT JOIN accounts a ON f.accountid = a.accountid
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_std_clients_u   ON mv_std_clients (accountid)",
    "CREATE INDEX IF NOT EXISTS idx_mv_std_clients_agent      ON mv_std_clients (assigned_to, second_deposit_date) WHERE has_second_deposit = 1",
]


def ensure_materialized_views() -> None:
    """Create all 4 materialized views and their indexes if they don't exist.
    Called once at application startup (lifespan).
    """
    conn = get_connection()
    try:
        for sql in _MV_SETUP_SQL:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"[ensure_materialized_views] skipped: {e}")
    finally:
        conn.close()


_MV_ORDER = [
    "mv_daily_kpis",      # base — must be first
    "mv_volume_stats",    # independent
    "mv_sales_bonuses",   # independent
    "mv_run_rate",        # depends on mv_daily_kpis — must be last
    "mv_account_stats",   # independent — new leads + live accounts
    "mv_std_clients",     # independent — STD (second deposit after $240 running total)
]

# Module-level status dict updated by refresh_materialized_views()
_mv_refresh_status: dict = {mv: {"last_refresh": None, "last_error": None} for mv in _MV_ORDER}


def refresh_materialized_views() -> None:
    """Refresh all 4 MVs. Tries CONCURRENTLY first (non-blocking); falls back to
    plain refresh if no unique index exists. Order matters: mv_daily_kpis before mv_run_rate.
    Called by APScheduler every 2 minutes.
    """
    from datetime import datetime, timezone
    for mv in _MV_ORDER:
        conn = get_connection()
        try:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                conn.commit()
            except Exception:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
                conn.commit()
            _mv_refresh_status[mv]["last_refresh"] = datetime.now(timezone.utc).isoformat()
            _mv_refresh_status[mv]["last_error"]   = None
        except Exception as e:
            conn.rollback()
            _mv_refresh_status[mv]["last_error"] = str(e)
            print(f"[refresh_materialized_views] {mv}: {e}")
        finally:
            conn.close()


def get_mv_status() -> list:
    """Return status of each MV: last refresh time, error, and estimated row count."""
    from datetime import datetime, timezone
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT relname, reltuples::bigint
                FROM pg_class
                WHERE relname = ANY(%s) AND relkind = 'm'
            """, ([mv for mv in _MV_ORDER],))
            row_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
    except Exception:
        row_counts = {}
    finally:
        conn.close()

    now = datetime.now(timezone.utc)
    result = []
    for mv in _MV_ORDER:
        status = _mv_refresh_status[mv]
        last_refresh = status["last_refresh"]
        age_seconds = None
        if last_refresh:
            try:
                ts = datetime.fromisoformat(last_refresh)
                age_seconds = int((now - ts).total_seconds())
            except Exception:
                pass
        result.append({
            "name":         mv,
            "last_refresh": last_refresh,
            "age_seconds":  age_seconds,
            "rows":         row_counts.get(mv),
            "last_error":   status["last_error"],
            "healthy":      status["last_error"] is None and age_seconds is not None and age_seconds < 300,
        })
    return result
