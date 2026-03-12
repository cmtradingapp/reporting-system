"""
Diagnose missing transactions between MSSQL report.vtiger_mttransactions and PostgreSQL transactions.
Run inside the docker container:
  docker cp scripts/diagnose_missing_transactions.py reporting-system-app-1:/tmp/diag.py
  docker exec reporting-system-app-1 python3 /tmp/diag.py
"""
import pymssql
import psycopg2

MSSQL = dict(server='cmtmainserver.database.windows.net', port='1433',
             user='clawreadonly', password='1231!#ASDF!a', database='cmt_main',
             tds_version='7.4', conn_properties='')
PG    = dict(host='127.0.0.1', port=5432, user='postgres',
             password='8PpVuUasBVR85T7WuAec', dbname='datawarehouse')

# ── 1. Get IDs from MSSQL ─────────────────────────────────────────────────────
print("Fetching MSSQL IDs...")
mc = pymssql.connect(**MSSQL)
cur = mc.cursor()

# Check available columns first
cur.execute("SELECT TOP 1 * FROM report.vtiger_mttransactions")
mssql_cols = [d[0].lower() for d in cur.description]
print(f"MSSQL columns: {mssql_cols}\n")

cur.execute("SELECT mttransactionsid FROM report.vtiger_mttransactions")
mssql_ids = set(int(r[0]) for r in cur.fetchall() if r[0] is not None)
mc.close()
print(f"MSSQL total rows: {len(mssql_ids)}")

# ── 2. Get IDs from PostgreSQL ────────────────────────────────────────────────
print("Fetching PostgreSQL IDs...")
pg = psycopg2.connect(**PG)
cur2 = pg.cursor()
cur2.execute("SELECT mttransactionsid FROM transactions")
pg_ids = set(int(r[0]) for r in cur2.fetchall() if r[0] is not None)
pg.close()
print(f"PostgreSQL total rows: {len(pg_ids)}")

# ── 3. Find differences ───────────────────────────────────────────────────────
missing  = mssql_ids - pg_ids   # in MSSQL but not in our DB
extra    = pg_ids - mssql_ids   # in our DB but not in MSSQL

print(f"\n{'='*55}")
print(f"In MSSQL but MISSING from PostgreSQL : {len(missing):,}")
print(f"In PostgreSQL but not in MSSQL       : {len(extra):,}")
print(f"{'='*55}\n")

if not missing:
    print("No missing transactions. Done.")
    exit()

# ── 4. Sample missing rows from MSSQL to understand why they're missing ───────
print("Sampling 30 missing rows from MSSQL to analyse...")
mc = pymssql.connect(**MSSQL)
cur = mc.cursor()

sample_ids = ','.join(str(i) for i in list(missing)[:200])

# Build SELECT dynamically based on available columns
select_cols = ['mttransactionsid', 'transactionapproval', 'transactiontype']
for col in ['server_id', 'usdamount', 'created_time', 'deleted']:
    if col in mssql_cols:
        select_cols.append(col)

col_str = ', '.join(select_cols)
cur.execute(f"""
    SELECT TOP 30 {col_str}
    FROM report.vtiger_mttransactions
    WHERE mttransactionsid IN ({sample_ids})
    ORDER BY mttransactionsid DESC
""")
rows = cur.fetchall()
headers = select_cols
print(' | '.join(f'{h:20}' for h in headers))
print('-' * (23 * len(headers)))
for r in rows:
    print(' | '.join(f'{str(v):20}' for v in r))

# ── 5. Breakdown by server_id if column exists ────────────────────────────────
if 'server_id' in mssql_cols:
    print("\n── Breakdown of missing rows by server_id ──")
    cur.execute(f"""
        SELECT server_id, COUNT(*) AS cnt
        FROM report.vtiger_mttransactions
        WHERE mttransactionsid IN ({','.join(str(i) for i in list(missing)[:5000])})
        GROUP BY server_id
        ORDER BY cnt DESC
    """)
    for r in cur.fetchall():
        print(f"  server_id={r[0]}  →  {r[1]:,} rows")

# ── 6. Breakdown by transactionapproval ──────────────────────────────────────
print("\n── Breakdown of missing rows by transactionapproval ──")
cur.execute(f"""
    SELECT transactionapproval, COUNT(*) AS cnt
    FROM report.vtiger_mttransactions
    WHERE mttransactionsid IN ({','.join(str(i) for i in list(missing)[:5000])})
    GROUP BY transactionapproval
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):20}  →  {r[1]:,} rows")

# ── 7. Check if missing IDs exist in MySQL broker_banking ────────────────────
print("\n── Checking if missing IDs exist in MySQL broker_banking ──")
import pymysql
try:
    my = pymysql.connect(host='cmtrading-replica-db.cllx9icdmhvp.eu-west-1.rds.amazonaws.com',
                         port=3306, user='db_readonly', password='wmFZBKH4E5j9m8Ax', database='crmdb')
    cur3 = my.cursor()
    sample_missing = list(missing)[:500]
    id_list = ','.join(str(i) for i in sample_missing)
    cur3.execute(f"SELECT COUNT(*) FROM crmdb.broker_banking WHERE id IN ({id_list})")
    found_in_mysql = cur3.fetchone()[0]
    print(f"  Of first 500 missing: {found_in_mysql} exist in MySQL broker_banking")
    print(f"  → {500 - found_in_mysql} do NOT exist in MySQL (truly missing from Antelope)")
    my.close()
except Exception as e:
    print(f"  Could not connect to MySQL: {e}")

mc.close()
print("\nDone.")
