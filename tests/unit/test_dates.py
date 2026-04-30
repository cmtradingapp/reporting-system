"""Unit tests for app.utils.dates.

Pure logic — no IO, no network, no mocking. The point of extracting these
helpers from three route files was precisely so they could be tested like
this. New consumers should import from `app.utils.dates`.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.utils.dates import count_working_days, last_day_of_month

# ── last_day_of_month ───────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    ("inp", "expected"),
    [
        (date(2026, 1, 15), date(2026, 1, 31)),  # 31-day month
        (date(2024, 2, 1), date(2024, 2, 29)),  # leap February
        (date(2026, 2, 1), date(2026, 2, 28)),  # non-leap February
        (date(2026, 4, 1), date(2026, 4, 30)),  # 30-day month
        (date(2026, 12, 25), date(2026, 12, 31)),  # December edge
    ],
)
def test_last_day_of_month(inp: date, expected: date) -> None:
    assert last_day_of_month(inp) == expected


@pytest.mark.unit
def test_last_day_of_month_idempotent_on_last_day() -> None:
    """Calling it on a date that is already the last day must not advance the month."""
    assert last_day_of_month(date(2026, 4, 30)) == date(2026, 4, 30)


# ── count_working_days ──────────────────────────────────────────────────


@pytest.mark.unit
def test_count_working_days_full_business_week() -> None:
    # 2026-04-27 is Monday, 2026-05-01 is Friday → 5 working days
    assert count_working_days(date(2026, 4, 27), date(2026, 5, 1), set()) == 5


@pytest.mark.unit
def test_count_working_days_weekend_only_range() -> None:
    # 2026-05-02 (Sat) to 2026-05-03 (Sun)
    assert count_working_days(date(2026, 5, 2), date(2026, 5, 3), set()) == 0


@pytest.mark.unit
def test_count_working_days_same_day_weekday() -> None:
    assert count_working_days(date(2026, 4, 30), date(2026, 4, 30), set()) == 1


@pytest.mark.unit
def test_count_working_days_same_day_weekend() -> None:
    # 2026-05-02 is Saturday
    assert count_working_days(date(2026, 5, 2), date(2026, 5, 2), set()) == 0


@pytest.mark.unit
def test_count_working_days_excludes_holidays() -> None:
    # Mon 2026-04-27 is the only weekday in this single-day range; flag it as a holiday
    holidays = {date(2026, 4, 27)}
    assert count_working_days(date(2026, 4, 27), date(2026, 4, 27), holidays) == 0


@pytest.mark.unit
def test_count_working_days_holiday_in_middle_of_range() -> None:
    # Full Mon–Fri week with Wednesday flagged as holiday → 4 working days
    holidays = {date(2026, 4, 29)}  # Wed
    assert count_working_days(date(2026, 4, 27), date(2026, 5, 1), holidays) == 4


@pytest.mark.unit
def test_count_working_days_end_before_start_returns_zero() -> None:
    assert count_working_days(date(2026, 5, 5), date(2026, 5, 1), set()) == 0


@pytest.mark.unit
def test_count_working_days_full_calendar_month() -> None:
    # April 2026: 1st is Wednesday. 22 weekdays in the month.
    assert count_working_days(date(2026, 4, 1), date(2026, 4, 30), set()) == 22


@pytest.mark.unit
def test_count_working_days_holidays_outside_range_dont_affect_count() -> None:
    holidays = {date(2025, 12, 25), date(2027, 1, 1)}
    assert count_working_days(date(2026, 4, 27), date(2026, 5, 1), holidays) == 5
