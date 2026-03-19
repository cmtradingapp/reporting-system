import sys
sys.path.insert(0, '/app')
from app.etl.fetch_and_store import run_daily_equity_zeroed_snapshot

dates = [
    '2026-02-28',
    '2026-03-01', '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05',
    '2026-03-06', '2026-03-07', '2026-03-08', '2026-03-09', '2026-03-10',
    '2026-03-11', '2026-03-12', '2026-03-13', '2026-03-14', '2026-03-15',
    '2026-03-16', '2026-03-17', '2026-03-18',
]
for d in dates:
    result = run_daily_equity_zeroed_snapshot(snapshot_date=d)
    print(result)
