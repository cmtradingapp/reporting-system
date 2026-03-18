import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGIN = 140189016
conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), SUM(net_amount) FROM bonus_transactions WHERE login = %s", (str(LOGIN),))
        row = cur.fetchone()
        print(f"Before: {row[0]} rows, total={row[1]}")

        cur.execute("""
            UPDATE bonus_transactions
            SET net_amount = 0, manual_override = TRUE
            WHERE login = %s
        """, (str(LOGIN),))
        print(f"Updated {cur.rowcount} rows to net_amount=0, manual_override=TRUE")

        cur.execute("SELECT COUNT(*), SUM(net_amount) FROM bonus_transactions WHERE login = %s", (str(LOGIN),))
        row = cur.fetchone()
        print(f"After: {row[0]} rows, total={row[1]}")
    conn.commit()
finally:
    conn.close()
