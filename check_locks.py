#!/usr/bin/env python3
import sys; sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SET statement_timeout = 30000")
cur.execute("""
    SELECT pid, state, wait_event_type, wait_event,
           now()-query_start as duration, left(query, 120)
    FROM pg_stat_activity
    WHERE state <> 'idle' AND pid <> pg_backend_pid()
    ORDER BY query_start
""")
for r in cur.fetchall():
    print(f"pid={r[0]} state={r[1]} wait={r[2]}/{r[3]} dur={r[4]}")
    print(f"  q={r[5]}")
if cur.rowcount == 0:
    print("No active queries")

# Also check for long-running MV refreshes
cur.execute("""
    SELECT pid, state, now()-query_start as duration, left(query, 120)
    FROM pg_stat_activity
    WHERE query ILIKE '%%REFRESH%%' OR query ILIKE '%%materializ%%'
""")
rows = cur.fetchall()
if rows:
    print("\nMV refreshes:")
    for r in rows:
        print(f"  pid={r[0]} state={r[1]} dur={r[2]} q={r[3]}")

conn.close()
