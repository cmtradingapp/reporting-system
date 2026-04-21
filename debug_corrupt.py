#!/usr/bin/env python3
"""Run directly on server: python3 debug_corrupt.py"""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))
from app.db.postgres_conn import get_connection

conn = get_connection()
cur = conn.cursor()

# 1. All corrupt rows in local DB
cur.execute("""
    SELECT login, date, sourceid, convertedfloatingpnl, convertedbalance
    FROM dealio_daily_profits
    WHERE ABS(COALESCE(convertedfloatingpnl, 0)) >= 100000000
    ORDER BY ABS(convertedfloatingpnl) DESC
""")
rows = cur.fetchall()
print(f"\n=== CORRUPT LOCAL ROWS ({len(rows)}) ===")
for r in rows:
    print(f"  login={r[0]}  date={r[1]}  sourceid={r[2]}  floatingpnl={r[3]:,.2f}  balance={r[4]:,.2f}")

# 2. Check login 141727130 local data
login = 141727130
cur.execute("""
    SELECT date, sourceid, convertedbalance, convertedfloatingpnl
    FROM dealio_daily_profits
    WHERE login = %s ORDER BY date DESC LIMIT 10
""", (login,))
rows = cur.fetchall()
print(f"\n=== LOCAL dealio_daily_profits for {login} (last 10) ===")
for r in rows:
    print(f"  date={r[0]}  sourceid={r[1]}  balance={r[2]:,.2f}  floatingpnl={r[3]:,.2f}")

# 3. Check remote dealio for same login
try:
    from app.db.dealio_conn import get_dealio_connection
    dconn = get_dealio_connection()
    dcur = dconn.cursor()
    dcur.execute("""
        SELECT date, sourceid, convertedbalance, convertedfloatingpnl
        FROM dealio.daily_profits
        WHERE login = %s ORDER BY date DESC LIMIT 10
    """, (login,))
    rows = dcur.fetchall()
    print(f"\n=== REMOTE dealio.daily_profits for {login} (last 10) ===")
    for r in rows:
        print(f"  date={r[0]}  sourceid={r[1]}  balance={r[2]:,.2f}  floatingpnl={r[3]:,.2f}")
    dconn.close()
except Exception as e:
    print(f"\nREMOTE dealio error: {e}")

conn.close()
print("\nDone.")
