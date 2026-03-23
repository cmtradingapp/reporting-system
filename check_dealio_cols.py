import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection

conn = get_dealio_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dealio'
              AND table_name = 'daily_profits'
            ORDER BY ordinal_position
        """)
        for r in cur.fetchall():
            print(f"{r[0]:35s} {r[1]}")
finally:
    conn.close()
