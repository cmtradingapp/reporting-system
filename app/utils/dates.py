"""Pure-logic date helpers shared by reporting routes.

These were duplicated verbatim in `app/routes/{dashboard,scoreboard,agent_bonuses}.py`
before extraction. Single source of truth keeps working-day arithmetic consistent
across reports and makes the logic unit-testable without importing route modules.
"""

from __future__ import annotations

import calendar
from datetime import date as date_type
from datetime import timedelta


def last_day_of_month(d: date_type) -> date_type:
    """Return the last calendar day of the month containing `d`."""
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def count_working_days(start: date_type, end: date_type, holidays: set[date_type]) -> int:
    """Count Mon–Fri non-holiday days between `start` and `end` (inclusive).

    Returns 0 if `end` precedes `start`. `holidays` is matched by `date` equality.
    """
    if end < start:
        return 0
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            count += 1
        current += timedelta(days=1)
    return count
