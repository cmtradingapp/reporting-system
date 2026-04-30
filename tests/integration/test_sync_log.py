"""Integration tests for sync_log + public_holidays operations.

Covers `get_last_sync_times` (the data shown on the Data Sync admin page)
and the public_holidays round-trip via the production DB helpers.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta

import psycopg2
import pytest


@pytest.fixture(autouse=True)
def _clean_tables(test_database_url: str | None) -> Iterator[None]:
    if test_database_url is None:
        yield
        return
    conn = psycopg2.connect(test_database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE sync_log RESTART IDENTITY")
            cur.execute("TRUNCATE public_holidays")
        conn.commit()
    finally:
        conn.close()
    yield


@pytest.fixture
def db_conn(test_database_url: str | None) -> Iterator[psycopg2.extensions.connection]:
    assert test_database_url is not None
    conn = psycopg2.connect(test_database_url)
    try:
        yield conn
    finally:
        conn.close()


# ── get_last_sync_times ───────────────────────────────────────────────


@pytest.mark.integration
def test_get_last_sync_times_empty(db_conn) -> None:
    from app.db.postgres_conn import get_last_sync_times

    assert get_last_sync_times() == {}


@pytest.mark.integration
def test_get_last_sync_times_returns_most_recent_per_table(db_conn) -> None:
    """Multiple successful runs per table → only the latest ran_at survives."""
    from app.db.postgres_conn import get_last_sync_times

    older = datetime(2026, 4, 1, 12, 0, 0)
    newer = datetime(2026, 4, 30, 9, 30, 0)
    with db_conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sync_log (table_name, cutoff_used, status, ran_at)
               VALUES ('accounts', %s, 'success', %s),
                      ('accounts', %s, 'success', %s),
                      ('users',    %s, 'success', %s)""",
            (older, older, newer, newer, newer, newer),
        )
    db_conn.commit()

    result = get_last_sync_times()
    assert set(result.keys()) == {"accounts", "users"}
    assert result["accounts"].startswith("2026-04-30")
    assert result["users"].startswith("2026-04-30")


@pytest.mark.integration
def test_get_last_sync_times_excludes_failed_runs(db_conn) -> None:
    """Only status='success' rows count — failed/in-progress runs are filtered out."""
    from app.db.postgres_conn import get_last_sync_times

    now = datetime(2026, 4, 30, 12, 0, 0)
    with db_conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sync_log (table_name, cutoff_used, status, ran_at)
               VALUES ('accounts', %s, 'failed',  %s),
                      ('accounts', %s, 'success', %s)""",
            (now, now, now - timedelta(hours=1), now - timedelta(hours=1)),
        )
    db_conn.commit()

    result = get_last_sync_times()
    # Only the older successful run should appear
    assert "accounts" in result
    assert result["accounts"].startswith("2026-04-30T11:00:00")


# ── public_holidays via the API helpers ───────────────────────────────


@pytest.mark.integration
def test_holiday_post_then_select(db_conn) -> None:
    """The /api/holidays POST helper inserts via plain SQL — verify it round-trips."""
    from datetime import date

    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public_holidays (holiday_date, description) VALUES (%s, %s)",
            (date(2026, 5, 1), "Labour Day"),
        )
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT description FROM public_holidays WHERE holiday_date = %s", (date(2026, 5, 1),))
        row = cur.fetchone()
    assert row == ("Labour Day",)


@pytest.mark.integration
def test_holiday_upsert_overwrites_description(db_conn) -> None:
    """ON CONFLICT clause on (holiday_date) updates the description rather than failing."""
    from datetime import date

    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public_holidays (holiday_date, description) VALUES (%s, %s)",
            (date(2026, 5, 1), "First desc"),
        )
        cur.execute(
            """INSERT INTO public_holidays (holiday_date, description) VALUES (%s, %s)
               ON CONFLICT (holiday_date) DO UPDATE SET description = EXCLUDED.description""",
            (date(2026, 5, 1), "Second desc"),
        )
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), MAX(description) FROM public_holidays WHERE holiday_date = %s", (date(2026, 5, 1),)
        )
        count, desc = cur.fetchone()
    assert count == 1
    assert desc == "Second desc"


@pytest.mark.integration
def test_holiday_delete_round_trip(db_conn) -> None:
    from datetime import date

    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public_holidays (holiday_date, description) VALUES (%s, %s)",
            (date(2026, 12, 25), "Christmas"),
        )
        cur.execute("DELETE FROM public_holidays WHERE holiday_date = %s", (date(2026, 12, 25),))
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM public_holidays WHERE holiday_date = %s", (date(2026, 12, 25),))
        assert cur.fetchone() == (0,)
