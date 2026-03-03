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


def upsert_users(df: pd.DataFrame):
    rows = [
        (
            str(row["id"]),
            row.get("email"),
            row.get("full_name"),
            row.get("status"),
            row.get("first_name"),
            row.get("last_name"),
            str(row["role_id"]) if row.get("role_id") is not None else None,
            str(row["desk_id"]) if row.get("desk_id") is not None else None,
            row.get("language"),
            row.get("last_logon_time"),
            row.get("last_update_time"),
            row.get("desk_name"),
            row.get("team"),
            row.get("department"),
            row.get("desk"),
            row.get("type"),
            str(row["office_id"]) if row.get("office_id") is not None else None,
            row.get("office"),
            row.get("position"),
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
