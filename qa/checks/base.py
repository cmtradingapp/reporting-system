from dataclasses import dataclass, field
from typing import Any

STATUS = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL", "ERROR": "ERROR"}


@dataclass
class QAResult:
    report: str       # "Performance", "Agent Bonuses", "Dashboard", "FTC Date", "Sync"
    section: str      # "Sales", "Retention", "Cross-Source", etc.
    check_name: str   # e.g. "ftc_per_agent", "office_totals"
    context: str      # Agent name, office, or "Grand Total"
    expected: Any     # Value computed directly from DB / business rule
    actual: Any       # Value shown in the report (re-computed from same query)
    diff: float       # abs(expected - actual)
    pct_diff: float   # diff / expected if expected != 0, else 0
    status: str       # PASS | WARN | FAIL | ERROR
    message: str      # Human-readable explanation
