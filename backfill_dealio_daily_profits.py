import sys
sys.path.insert(0, '/app')
from app.etl.fetch_and_store import run_dealio_daily_profits_daterange_etl, run_daily_equity_zeroed_snapshot

# Step 1: re-sync dealio_daily_profits from Dealio replica for all affected dates
print("Syncing dealio_daily_profits from Dealio replica for 2026-02-01 to 2026-03-22...")
result = run_dealio_daily_profits_daterange_etl('2026-02-01', '2026-03-22')
print(result)

# Step 2: re-run EEZ snapshots for all affected dates
dates = [
    '2026-02-28',
    '2026-03-01', '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05',
    '2026-03-06', '2026-03-07', '2026-03-08', '2026-03-09', '2026-03-10',
    '2026-03-11', '2026-03-12', '2026-03-13', '2026-03-14', '2026-03-15',
    '2026-03-16', '2026-03-17', '2026-03-18', '2026-03-19', '2026-03-20',
    '2026-03-21', '2026-03-22',
]
print("\nRe-running EEZ snapshots...")
for d in dates:
    result = run_daily_equity_zeroed_snapshot(snapshot_date=d)
    print(result)
