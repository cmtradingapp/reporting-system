-- Minimal Postgres schema for integration tests.
-- Real prod schema lives in app/db/postgres_conn.py (the ensure_*_table functions).
-- This file is intentionally narrower: only the tables the integration tier exercises.
-- Keep it small — every column added here is a maintenance liability.

-- Mirrors app/db/postgres_conn.py::ensure_auth_table + the prod migrations
-- that added allowed_pages and extra_roles. Keep these column definitions
-- in sync if the prod DDL evolves.
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
    last_login            TIMESTAMP,
    allowed_pages         TEXT NULL,
    extra_roles           JSONB NULL
);
CREATE INDEX IF NOT EXISTS idx_auth_users_email ON auth_users(email);
CREATE INDEX IF NOT EXISTS idx_auth_users_crm_user_id ON auth_users(crm_user_id);

CREATE TABLE IF NOT EXISTS crm_users (
    id            BIGINT PRIMARY KEY,
    full_name     TEXT,
    agent_name    TEXT,
    department_   TEXT,
    department    TEXT,
    office        TEXT,
    is_active     INT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS accounts (
    accountid           TEXT PRIMARY KEY,
    is_test_account     INT DEFAULT 0,
    is_demo             INT DEFAULT 0,
    classification_int  INT,
    birth_date          DATE,
    campaign            TEXT,
    segmentation        TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
    id                     BIGINT PRIMARY KEY,
    vtigeraccountid        TEXT,
    transactionapproval    TEXT,
    transaction_type_name  TEXT,
    comment                TEXT,
    confirmation_time      TIMESTAMP,
    deleted                INT DEFAULT 0,
    original_deposit_owner BIGINT,
    amount_usd             NUMERIC(18, 2)
);

CREATE TABLE IF NOT EXISTS sync_log (
    id           SERIAL PRIMARY KEY,
    table_name   TEXT NOT NULL,
    status       TEXT NOT NULL,
    cutoff_time  TIMESTAMP,
    finished_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS holidays (
    holiday_date DATE PRIMARY KEY,
    label        TEXT
);

CREATE TABLE IF NOT EXISTS company_targets (
    id        SERIAL PRIMARY KEY,
    label     TEXT NOT NULL,
    month     DATE NOT NULL,
    target    NUMERIC(18, 2)
);
