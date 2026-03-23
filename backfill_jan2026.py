import sys
sys.path.insert(0, '/app')
from app.etl.fetch_and_store import run_dealio_daily_profits_daterange_etl

print("Backfilling dealio_daily_profits for January 2026...")
result = run_dealio_daily_profits_daterange_etl('2026-01-01', '2026-01-31')
print(result)
