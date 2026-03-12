"""
Break down ALL missing transactions by transactiontype.
Run:
  docker cp scripts/check_missing_breakdown.py reporting-system-app-1:/tmp/breakdown.py
  docker exec reporting-system-app-1 python3 /tmp/breakdown.py
"""
import pymssql
import psycopg2

MSSQL = dict(server='cmtmainserver.database.windows.net', port='1433',
             user='clawreadonly', password='1231!#ASDF!a', database='cmt_main',
             tds_version='7.4', conn_properties='')
PG    = dict(host='127.0.0.1', port=5432, user='postgres',
             password='8PpVuUasBVR85T7WuAec', dbname='datawarehouse')

print("Fetching IDs from PostgreSQL...")
pg = psycopg2.connect(**PG)
cur = pg.cursor()
cur.execute("SELECT mttransactionsid FROM transactions")
pg_ids = set(int(r[0]) for r in cur.fetchall() if r[0] is not None)
pg.close()
print(f"PostgreSQL rows: {len(pg_ids):,}")

print("Fetching IDs from MSSQL...")
mc = pymssql.connect(**MSSQL)
cur = mc.cursor()
cur.execute("SELECT mttransactionsid FROM report.vtiger_mttransactions")
mssql_ids = set(int(r[0]) for r in cur.fetchall() if r[0] is not None)
mc.close()

missing = mssql_ids - pg_ids
print(f"Missing from PostgreSQL: {len(missing):,}\n")

# Query MSSQL for full breakdown of ALL missing IDs
print("Getting full breakdown by transactiontype from MSSQL...")
mc = pymssql.connect(**MSSQL)
cur = mc.cursor()

# Insert missing IDs into a temp table for efficient querying
cur.execute("CREATE TABLE #missing_ids (id BIGINT)")
batch_size = 1000
missing_list = list(missing)
for i in range(0, len(missing_list), batch_size):
    batch = missing_list[i:i+batch_size]
    values = ','.join(f'({v})' for v in batch)
    cur.execute(f"INSERT INTO #missing_ids VALUES {values}")

print("\n── By transactiontype ──")
cur.execute("""
    SELECT v.transactiontype, COUNT(*) AS cnt,
           SUM(CASE WHEN v.transactionapproval = 'Approved' THEN 1 ELSE 0 END) AS approved_cnt,
           SUM(CASE WHEN v.transactionapproval = 'Approved' THEN v.usdamount ELSE 0 END) AS approved_usd
    FROM report.vtiger_mttransactions v
    JOIN #missing_ids m ON m.id = v.mttransactionsid
    GROUP BY v.transactiontype
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):25} count={r[1]:>8,}  approved={r[2]:>8,}  approved_usd=${r[3]:>15,.0f}")

print("\n── By transactionapproval ──")
cur.execute("""
    SELECT v.transactionapproval, COUNT(*) AS cnt
    FROM report.vtiger_mttransactions v
    JOIN #missing_ids m ON m.id = v.mttransactionsid
    GROUP BY v.transactionapproval
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):25} count={r[1]:>8,}")

mc.close()
print("\nDone.")
