"""Integration tier smoke.

Auto-skipped locally when TEST_DATABASE_URL is unset (see conftest.py).
In CI, the Postgres service container provides the DB and `psql -f
tests/fixtures/schema.sql` seeds it; this test confirms both ends are wired.

Phase 3 will replace this with real ETL idempotency + MV refresh tests.
"""

from __future__ import annotations

import os

import psycopg2
import pytest


@pytest.mark.integration
def test_test_database_url_set_in_ci() -> None:
    """If we got here, TEST_DATABASE_URL must be set — otherwise conftest skipped us."""
    assert os.environ.get(
        "TEST_DATABASE_URL"
    ), "Integration tier ran without TEST_DATABASE_URL — conftest skip rule broken."


@pytest.mark.integration
def test_postgres_connection_works(test_database_url: str | None) -> None:
    assert test_database_url is not None
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.integration
def test_schema_seeded(test_database_url: str | None) -> None:
    """tests/fixtures/schema.sql must have been applied before the suite ran."""
    assert test_database_url is not None
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = {row[0] for row in cur.fetchall()}
        # Tables defined in tests/fixtures/schema.sql
        expected = {
            "auth_users",
            "crm_users",
            "accounts",
            "transactions",
            "sync_log",
            "public_holidays",
            "company_targets",
        }
        missing = expected - tables
        assert not missing, f"schema.sql tables missing: {missing}. Did the seed step run?"
    finally:
        conn.close()
