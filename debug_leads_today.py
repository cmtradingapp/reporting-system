"""Run on server: python debug_leads_today.py"""
from app.db.postgres_conn import get_connection

conn = get_connection()
try:
    with conn.cursor() as cur:

        cur.execute("SELECT CURRENT_DATE, NOW(), current_setting('TIMEZONE')")
        row = cur.fetchone()
        print(f"DB CURRENT_DATE : {row[0]}")
        print(f"DB NOW()        : {row[1]}")
        print(f"DB timezone     : {row[2]}")
        print()

        cur.execute("SELECT MAX(createdtime), MIN(createdtime) FROM accounts WHERE is_test_account = 0")
        row = cur.fetchone()
        print(f"Latest createdtime : {row[0]}")
        print(f"Oldest createdtime : {row[1]}")
        print()

        cur.execute("""
            SELECT createdtime::date AS day, COUNT(*)
            FROM accounts
            WHERE is_test_account = 0
              AND createdtime >= NOW() - INTERVAL '7 days'
            GROUP BY 1 ORDER BY 1 DESC
        """)
        rows = cur.fetchall()
        print("Accounts per day (last 7 days):")
        for r in rows:
            print(f"  {r[0]}  →  {r[1]}")
        print()

        cur.execute("""
            SELECT COUNT(*)
            FROM accounts
            WHERE is_test_account = 0
              AND createdtime::date = CURRENT_DATE
        """)
        print(f"Accounts where createdtime::date = CURRENT_DATE : {cur.fetchone()[0]}")

        cur.execute("""
            SELECT COUNT(*)
            FROM accounts
            WHERE is_test_account = 0
              AND createdtime >= NOW() - INTERVAL '24 hours'
        """)
        print(f"Accounts in last 24h (NOW()-24h)               : {cur.fetchone()[0]}")
        print()

        cur.execute("SELECT * FROM mv_account_stats")
        row = cur.fetchone()
        print(f"mv_account_stats row: {row}")

finally:
    conn.close()
