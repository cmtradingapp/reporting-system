#!/usr/bin/env python3
"""Run on server: /root/forex-agent/venv/bin/python3 fix_corrupt.py"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from app.db.postgres_conn import get_connection

conn = get_connection()
cur = conn.cursor()

# Step 1: Delete corrupt rows
cur.execute("""
    DELETE FROM dealio_daily_profits
    WHERE ABS(COALESCE(convertedfloatingpnl, 0)) >= 100000000
""")
print(f"Deleted {cur.rowcount} corrupt rows")
conn.commit()

# Step 2: Verify
cur.execute("""
    SELECT COUNT(*) FROM dealio_daily_profits
    WHERE ABS(COALESCE(convertedfloatingpnl, 0)) >= 100000000
""")
print(f"Remaining corrupt rows: {cur.fetchone()[0]}")
conn.close()

# Step 3: Re-run snapshots
from app.etl.fetch_and_store import run_daily_equity_zeroed_snapshot
for d in ['2026-04-18', '2026-04-19', '2026-04-20']:
    try:
        r = run_daily_equity_zeroed_snapshot(d)
        print(f"Snapshot {d}: {r}")
    except Exception as e:
        print(f"Snapshot {d} error: {e}")

print("\nDone! EEZ values should now be correct.")
