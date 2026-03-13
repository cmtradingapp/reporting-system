"""
QA Orchestrator: run all check modules, collect results, write reports.
"""
import os
from datetime import datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

from app.db.postgres_conn import get_connection
from qa.config import load_config
from qa.checks.base import QAResult, STATUS
from qa.checks.performance import run_performance_checks
from qa.checks.agent_bonuses import run_bonus_checks
from qa.checks.dashboard import run_dashboard_checks
from qa.checks.ftc_date import run_ftcdate_checks
from qa.checks.sync_sources import run_sync_checks
from qa.checks.per_agent_crosscheck import run_per_agent_crosscheck
from qa.history import update_history
from qa.reporter import write_excel, write_pdf

_TZ = ZoneInfo("Europe/Nicosia")


def run(date_to: Optional[str] = None, reports: Optional[List[str]] = None) -> dict:
    """
    Run all enabled checks.

    Args:
        date_to: End date (YYYY-MM-DD). Defaults to yesterday (Cyprus time).
        reports: List of report names to run. Defaults to all enabled in config.

    Returns:
        dict with keys: score, total, results, excel_path, pdf_path
    """
    cfg = load_config()

    if date_to is None:
        date_to = (datetime.now(_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")

    # date_from = 1st of the same month as date_to
    date_from = date_to[:8] + "01"

    conn = get_connection()
    all_results: List[QAResult] = []

    enabled = reports or [k for k, v in cfg.get("reports", {}).items() if v]

    try:
        if "performance" in enabled:
            try:
                all_results += run_performance_checks(conn, date_from, date_to, cfg)
            except Exception as e:
                all_results.append(QAResult(
                    "Performance", "Error", "module_error", "Engine",
                    None, None, 0.0, 0.0, STATUS["ERROR"],
                    f"Performance module crashed: {e}"
                ))

        if "agent_bonuses" in enabled:
            try:
                all_results += run_bonus_checks(conn, date_from, date_to, cfg)
            except Exception as e:
                all_results.append(QAResult(
                    "Agent Bonuses", "Error", "module_error", "Engine",
                    None, None, 0.0, 0.0, STATUS["ERROR"],
                    f"Agent Bonuses module crashed: {e}"
                ))

        if "dashboard" in enabled:
            try:
                all_results += run_dashboard_checks(conn, date_from, date_to, cfg)
            except Exception as e:
                all_results.append(QAResult(
                    "Dashboard", "Error", "module_error", "Engine",
                    None, None, 0.0, 0.0, STATUS["ERROR"],
                    f"Dashboard module crashed: {e}"
                ))

        if "ftc_date" in enabled:
            try:
                all_results += run_ftcdate_checks(conn, date_from, date_to, cfg)
            except Exception as e:
                all_results.append(QAResult(
                    "FTC Date", "Error", "module_error", "Engine",
                    None, None, 0.0, 0.0, STATUS["ERROR"],
                    f"FTC Date module crashed: {e}"
                ))

        # Sync checks always run
        try:
            all_results += run_sync_checks(conn, date_from, date_to, cfg)
        except Exception as e:
            all_results.append(QAResult(
                "Sync", "Error", "module_error", "Engine",
                None, None, 0.0, 0.0, STATUS["ERROR"],
                f"Sync module crashed: {e}"
            ))

        # Per-agent cross-source validation (MySQL vs PostgreSQL)
        try:
            all_results += run_per_agent_crosscheck(conn, date_from, date_to, cfg)
        except Exception as e:
            all_results.append(QAResult(
                "Sync", "Error", "module_error", "Engine",
                None, None, 0.0, 0.0, STATUS["ERROR"],
                f"Per-agent crosscheck module crashed: {e}"
            ))

    finally:
        conn.close()

    total = len(all_results)
    if total == 0:
        score = 100.0
    else:
        # Score = PASS / (PASS + FAIL + ERROR) × 100  — WARNs are informational, not failures
        denom = sum(1 for r in all_results if r.status in (STATUS["PASS"], STATUS["FAIL"], STATUS["ERROR"]))
        score = (sum(1 for r in all_results if r.status == STATUS["PASS"]) / denom * 100
                 if denom > 0 else 100.0)

    folder = cfg.get("output", {}).get("folder", "reports/qa")
    os.makedirs(folder, exist_ok=True)

    update_history(date_to, score, all_results, folder)

    excel_path = write_excel(all_results, score, folder, date_to)
    pdf_path   = write_pdf(all_results, score, folder, date_to)

    return {
        "score":      round(score, 2),
        "total":      total,
        "pass":       sum(1 for r in all_results if r.status == STATUS["PASS"]),
        "warn":       sum(1 for r in all_results if r.status == STATUS["WARN"]),
        "fail":       sum(1 for r in all_results if r.status == STATUS["FAIL"]),
        "error":      sum(1 for r in all_results if r.status == STATUS["ERROR"]),
        "date_from":  date_from,
        "date_to":    date_to,
        "excel_path": excel_path,
        "pdf_path":   pdf_path,
        "results":    all_results,
    }
