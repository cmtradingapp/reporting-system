import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from app.etl.fetch_and_store import run_dealio_daily_profits_daterange_etl

# Get row counts per month from source
print("Checking source row counts per month...")
src_conn = get_dealio_connection()
src_cur = src_conn.cursor()
src_cur.execute("""
    SELECT TO_CHAR(date::date, 'YYYY-MM') AS month, COUNT(*)
    FROM dealio.daily_profits
    GROUP BY 1 ORDER BY 1
""")
source_counts = {row[0]: row[1] for row in src_cur.fetchall()}
src_conn.close()

# Get row counts per month from local
local_conn = get_connection()
local_cur = local_conn.cursor()
local_cur.execute("""
    SELECT TO_CHAR(date::date, 'YYYY-MM') AS month, COUNT(*)
    FROM dealio_daily_profits
    GROUP BY 1 ORDER BY 1
""")
local_counts = {row[0]: row[1] for row in local_cur.fetchall()}
local_conn.close()

# Find months where local < source
month_ends = {
    '02': '28', '04': '30', '06': '30', '09': '30', '11': '30',
}
def month_end(ym):
    y, m = ym.split('-')
    if m == '02' and int(y) % 4 == 0:
        return '29'
    return month_ends.get(m, '31')

to_sync = []
for month, src_count in sorted(source_counts.items()):
    local_count = local_counts.get(month, 0)
    if local_count < src_count:
        to_sync.append((month, src_count, local_count))

print(f"\n{len(to_sync)} months need syncing:")
for month, src, local in to_sync:
    print(f"  {month}: local={local}, source={src}, missing={src-local}")

print("\nStarting sync...")
for month, src_count, local_count in to_sync:
    y, m = month.split('-')
    date_from = f"{y}-{m}-01"
    date_to = f"{y}-{m}-{month_end(month)}"
    print(f"Syncing {date_from} to {date_to} (missing {src_count - local_count} rows)...")
    result = run_dealio_daily_profits_daterange_etl(date_from, date_to)
    print(result)

print("\nAll done!")
