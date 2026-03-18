import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGIN = 141300595
conn = get_connection()
try:
    with conn.cursor() as cur:
        # Source 1: transactions table (used in _live_calc)
        cur.execute("""
            SELECT
                t.transactiontype,
                t.usdamount,
                t.comment,
                t.transactionapproval,
                t.deleted,
                t.confirmation_time,
                CASE
                    WHEN t.transactiontype IN ('Deposit', 'Credit in')     THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Credit out') THEN -t.usdamount
                    ELSE 0
                END AS contribution
            FROM transactions t
            WHERE t.login = %(login)s
              AND t.transactionapproval = 'Approved'
              AND LOWER(t.comment) LIKE '%%bonus%%'
              AND (t.deleted = 0 OR t.deleted IS NULL)
            ORDER BY t.confirmation_time
        """, {"login": str(LOGIN)})
        rows = cur.fetchall()
        print("=== transactions (bonus rows) ===")
        total = 0.0
        for r in rows:
            print(f"  type={r[0]}, amount={r[1]}, comment={r[2][:50] if r[2] else None}, contrib={r[5]}")
            total += float(r[5] or 0)
        print(f"  TOTAL from transactions: {total}")

        # Source 2: bonus_transactions table (used in snapshot ETL)
        cur.execute("""
            SELECT
                transactiontype,
                net_amount,
                comment,
                confirmation_time
            FROM bonus_transactions
            WHERE login = %(login)s
            ORDER BY confirmation_time
        """, {"login": str(LOGIN)})
        rows2 = cur.fetchall()
        print("\n=== bonus_transactions ===")
        total2 = 0.0
        for r in rows2:
            print(f"  type={r[0]}, net_amount={r[1]}, comment={r[2][:50] if r[2] else None}")
            total2 += float(r[1] or 0)
        print(f"  TOTAL from bonus_transactions: {total2}")
finally:
    conn.close()
