import sys
sys.path.insert(0, '/app')
from app.etl.fetch_and_store import run_daily_equity_zeroed_snapshot
from app.db.postgres_conn import _pg_conn

# Delete the inflated rows (test accounts included) before re-running
conn = _pg_conn()
try:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM daily_equity_zeroed WHERE day BETWEEN '2026-03-14' AND '2026-03-17'")
        print(f"Deleted {cur.rowcount} rows for 2026-03-14 to 2026-03-17")
    conn.commit()
finally:
    conn.close()

dates = [
    '2026-03-14', '2026-03-15', '2026-03-16', '2026-03-17',  # re-insert with test filter
    '2026-03-18',  # recalc start_eez from corrected March 17
]
for d in dates:
    result = run_daily_equity_zeroed_snapshot(snapshot_date=d)
    print(result)
