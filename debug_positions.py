import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection

conn = get_dealio_connection()
cur = conn.cursor()
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='dealio'
    ORDER BY table_name
""")
for r in cur.fetchall():
    print(r[0])
conn.close()
