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
    """Convert pandas NaT/NaN to None for PostgreSQL compatibility."""
    if val is None:
        return None
    try:
        if pd.isnull(val):
            return None
    except (TypeError, ValueError):
        pass
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
