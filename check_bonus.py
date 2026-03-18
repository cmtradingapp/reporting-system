import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGIN = 141300595
conn = get_connection()
try:
    with conn.cursor() as cur:
        # Show bonus_transactions columns
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bonus_transactions'
            ORDER BY ordinal_position
        """)
        print("=== bonus_transactions columns ===")
        for r in cur.fetchall():
            print(f"  {r[0]} ({r[1]})")

        # Source 1: transactions table
        cur.execute("""
            SELECT SUM(CASE
                WHEN transactiontype IN ('Deposit', 'Credit in')     THEN  usdamount
                WHEN transactiontype IN ('Withdrawal', 'Credit out') THEN -usdamount
                ELSE 0
            END)
            FROM transactions
            WHERE login = %s
              AND transactionapproval = 'Approved'
              AND LOWER(comment) LIKE '%%bonus%%'
              AND (deleted = 0 OR deleted IS NULL)
        """, (str(LOGIN),))
        print(f"\ntransactions bonus total: {cur.fetchone()[0]}")

        # Source 2: bonus_transactions - sum net_amount
        cur.execute("SELECT SUM(net_amount) FROM bonus_transactions WHERE login = %s", (str(LOGIN),))
        print(f"bonus_transactions net_amount total: {cur.fetchone()[0]}")

        # Show all rows for this login
        cur.execute("SELECT * FROM bonus_transactions WHERE login = %s ORDER BY confirmation_time", (str(LOGIN),))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print(f"\nbonus_transactions rows ({len(rows)}):")
        for r in rows:
            print(dict(zip(cols, r)))
finally:
    conn.close()
