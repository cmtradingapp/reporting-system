#!/usr/bin/env python3
import sys; sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SET statement_timeout = 300000")
cur.execute("SET lock_timeout = 0")

# Kill any queries touching dealio_trades_mt4
cur.execute("""
    SELECT pid, left(query, 80)
    FROM pg_stat_activity
    WHERE query ILIKE '%%dealio_trades_mt4%%' AND pid <> pg_backend_pid()
""")
for r in cur.fetchall():
    print(f"Killing pid={r[0]}: {r[1]}")
    cur.execute("SELECT pg_terminate_backend(%s)", (r[0],))

conn.commit()
import time
time.sleep(2)

cur.execute("DROP TABLE IF EXISTS dealio_trades_mt4")
print("Table dropped (was 11 GB)")
conn.commit()
conn.close()
