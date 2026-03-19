import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection

print("=== DEALIO REPLICA ===")
conn = get_dealio_connection()
cur = conn.cursor()
cur.execute("SELECT MIN(date::date), MAX(date::date), COUNT(*) FROM dealio.daily_profits")
print(cur.fetchone())
conn.close()

print("=== LOCAL TABLE ===")
conn2 = get_connection()
cur2 = conn2.cursor()
cur2.execute("SELECT MIN(date::date), MAX(date::date), COUNT(*) FROM dealio_daily_profits")
print(cur2.fetchone())
conn2.close()
