"""
One-time script: Convert dealio_trades_mt5 open_time and close_time from EET/EEST to UTC.

The dealio source stores timestamps in MT5 server time (EET = UTC+2 / EEST = UTC+3).
This script converts them to UTC to match the MSSQL/PBI convention.

Usage (on server):
    cd /opt/reporting-system
    source venv/bin/activate
    python3 scripts/convert_mt5_timestamps_to_utc.py
"""
import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1", port=5432, dbname="datawarehouse",
    user="postgres", password="8PpVuUasBVR85T7WuAec",
    options="-c statement_timeout=0",
)
conn.autocommit = False
cur = conn.cursor()

# Check current state
cur.execute("""
    SELECT ticket, open_time, close_time
    FROM dealio_trades_mt5
    WHERE ticket = 5556348
""")
row = cur.fetchone()
print(f"Before: ticket={row[0]} open_time={row[1]} close_time={row[2]}")

# Convert open_time (skip 1970 placeholders)
print("Converting open_time from EET to UTC...")
cur.execute("""
    UPDATE dealio_trades_mt5
    SET open_time = (open_time AT TIME ZONE 'EET') AT TIME ZONE 'UTC'
    WHERE open_time > '2000-01-01'
""")
print(f"  Updated {cur.rowcount:,} rows")

# Convert close_time (skip 1970 placeholders)
print("Converting close_time from EET to UTC...")
cur.execute("""
    UPDATE dealio_trades_mt5
    SET close_time = (close_time AT TIME ZONE 'EET') AT TIME ZONE 'UTC'
    WHERE close_time > '2000-01-01'
""")
print(f"  Updated {cur.rowcount:,} rows")

conn.commit()

# Verify
cur.execute("""
    SELECT ticket, open_time, close_time
    FROM dealio_trades_mt5
    WHERE ticket = 5556348
""")
row = cur.fetchone()
print(f"After:  ticket={row[0]} open_time={row[1]} close_time={row[2]}")
print("Expected: open_time=2026-03-31 21:34:49 (was 2026-04-01 00:34:49)")

cur.close()
conn.close()
print("Done.")
