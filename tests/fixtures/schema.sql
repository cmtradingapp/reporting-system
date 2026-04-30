-- Minimal Postgres schema for integration tests.
-- Real prod schema lives in app/db/postgres_conn.py (the ensure_*_table functions).
-- This file is intentionally narrower: only the tables the integration tier exercises.
-- Keep it small — every column added here is a maintenance liability.

CREATE TABLE IF NOT EXISTS auth_users (
    id           SERIAL PRIMARY KEY,
    username     TEXT UNIQUE NOT NULL,
    password     TEXT NOT NULL,
    role         TEXT NOT NULL,
    extra_roles  JSONB DEFAULT '[]'::jsonb,
    allowed_pages JSONB DEFAULT '[]'::jsonb,
    created_at   TIMESTAMP DEFAULT NOW()
);

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
