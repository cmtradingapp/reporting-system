#!/usr/bin/env python3
"""Fix corrupt dealio_users compbalance using clean dealio_daily_profits data."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from app.db.postgres_conn import get_connection

conn = get_connection()
cur = conn.cursor()

# 1. Show current state
cur.execute("SELECT COUNT(*), ROUND(SUM(compbalance)::numeric,2) FROM dealio_users WHERE compbalance > 100000")
r = cur.fetchone()
print(f"Logins with compbalance > 100K: {r[0]}, total: ${r[1]:,.2f}")

# 2. Fix compbalance using latest clean dealio_daily_profits (CMTrading-S1)
cur.execute("""
    UPDATE dealio_users du
    SET compbalance = ddp.convertedbalance
    FROM (
        SELECT DISTINCT ON (login) login, convertedbalance
        FROM dealio_daily_profits
        WHERE sourcename = 'CMTrading-S1'
        ORDER BY login, date DESC
    ) ddp
    WHERE du.login = ddp.login
      AND du.compbalance > 100000
      AND du.compbalance <> ddp.convertedbalance
""")
print(f"Fixed compbalance for {cur.rowcount} logins")
conn.commit()

# 3. Verify
cur.execute("""
    SELECT COUNT(*), ROUND(SUM(du.compbalance)::numeric,2)
    FROM dealio_users du
    JOIN trading_accounts ta ON ta.login::bigint = du.login
    WHERE ta.equity > 0 AND (ta.deleted=0 OR ta.deleted IS NULL)
""")
r = cur.fetchone()
print(f"\nActive logins: {r[0]}, total compbalance: ${r[1]:,.2f}")

cur.execute("SELECT login, compbalance FROM dealio_users ORDER BY compbalance DESC LIMIT 10")
print("\nTop 10 compbalance after fix:")
for r in cur.fetchall():
    print(f"  login={r[0]}  compbalance=${r[1]:,.2f}")

conn.close()
print("\nDone.")
