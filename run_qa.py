#!/usr/bin/env python3
"""
QA validation agent — CLI entry point + APScheduler setup.

Usage:
    python run_qa.py --now                         # Run immediately, current month
    python run_qa.py --now --date 2026-03-01       # Specific end date
    python run_qa.py --now --reports performance   # One report only
    python run_qa.py                               # Start scheduler (9 AM Cyprus daily)
"""
import argparse
import sys
import os

# Ensure project root is in PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _run(date: str = None, reports: list = None):
    from qa.engine import run
    print("▶  Starting QA run…")
    result = run(date_to=date, reports=reports)
    print(f"✅  Score: {result['score']}%  |  Total: {result['total']}  |  "
          f"Pass: {result['pass']}  Warn: {result['warn']}  "
          f"Fail: {result['fail']}  Error: {result['error']}")
    print(f"📊  Excel: {result['excel_path']}")
    print(f"📄  PDF:   {result['pdf_path']}")

    if result["fail"] > 0 or result["error"] > 0:
        print("\n⚠️  Anomalies:")
        from qa.checks.base import STATUS
        for r in result["results"]:
            if r.status in (STATUS["FAIL"], STATUS["ERROR"]):
                print(f"   [{r.status}] {r.report} / {r.section} / {r.check_name} "
                      f"({r.context}): {r.message}")
    return result


def _start_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BlockingScheduler()
    scheduler.add_job(
        _run,
        trigger=CronTrigger(hour=9, minute=0, timezone="Europe/Nicosia"),
        name="qa_daily",
        misfire_grace_time=3600,
    )
    print("⏰  QA scheduler started — runs daily at 09:00 Cyprus time.")
    print("    Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


def main():
    parser = argparse.ArgumentParser(description="Data QA Validation Agent")
    parser.add_argument("--now", action="store_true",
                        help="Run immediately instead of starting scheduler")
    parser.add_argument("--date", type=str, default=None,
                        help="End date for the QA run (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--reports", type=str, nargs="+", default=None,
                        choices=["performance", "agent_bonuses", "dashboard", "ftc_date"],
                        help="Specific reports to check (space-separated)")
    args = parser.parse_args()

    if args.now:
        _run(date=args.date, reports=args.reports)
    else:
        _start_scheduler()


if __name__ == "__main__":
    main()
