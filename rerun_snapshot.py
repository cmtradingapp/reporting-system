import sys
sys.path.insert(0, '/app')
from app.etl.fetch_and_store import run_daily_equity_zeroed_snapshot

dates = ['2026-02-28', '2026-03-14', '2026-03-15', '2026-03-16', '2026-03-17', '2026-03-18']
for d in dates:
    result = run_daily_equity_zeroed_snapshot(snapshot_date=d)
    print(result)
