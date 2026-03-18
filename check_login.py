import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGIN = 140189016
conn = get_connection()
try:
    with conn.cursor() as cur:
        # trading_accounts info
        cur.execute("""
            SELECT ta.login, ta.vtigeraccountid, ta.balance, ta.equity, a.is_test_account
            FROM trading_accounts ta
            LEFT JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE ta.login = %s
        """, (str(LOGIN),))
        rows = cur.fetchall()
        print("=== trading_accounts ===")
        for r in rows:
            print(f"  login={r[0]}, vtigeraccountid={r[1]}, balance={r[2]}, equity={r[3]}, is_test={r[4]}")

        # latest dealio_daily_profits
        cur.execute("""
            SELECT DISTINCT ON (login) login, date, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE login = %s
            ORDER BY login, date DESC
        """, (str(LOGIN),))
        r = cur.fetchone()
        print(f"\n=== dealio_daily_profits (latest) ===")
        print(f"  {r}")

        # bonus
        cur.execute("SELECT SUM(net_amount) FROM bonus_transactions WHERE login = %s", (str(LOGIN),))
        print(f"\n=== bonus_transactions total ===")
        print(f"  {cur.fetchone()[0]}")

        # daily_equity_zeroed
        cur.execute("SELECT day, end_equity_zeroed, start_equity_zeroed FROM daily_equity_zeroed WHERE login = %s ORDER BY day", (str(LOGIN),))
        rows = cur.fetchall()
        print(f"\n=== daily_equity_zeroed ===")
        for r in rows:
            print(f"  {r}")
finally:
    conn.close()
