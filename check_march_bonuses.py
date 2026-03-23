import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

conn = get_connection()
try:
    with conn.cursor() as cur:
        # Total bonuses per day in March from bonus_transactions
        cur.execute("""
            SELECT confirmation_time::date AS day,
                   COUNT(*)               AS count,
                   SUM(net_amount)        AS total
            FROM bonus_transactions
            WHERE confirmation_time >= '2026-03-01'
              AND confirmation_time <  '2026-03-24'
            GROUP BY confirmation_time::date
            ORDER BY day
        """)
        rows = cur.fetchall()

        print(f"{'Date':<12} {'Count':>8} {'Total Bonus':>14}")
        print("-" * 38)
        grand = 0.0
        for r in rows:
            grand += float(r[2] or 0)
            print(f"{str(r[0]):<12} {r[1]:>8} ${float(r[2] or 0):>13,.2f}")
        print("-" * 38)
        print(f"{'TOTAL':<12} {'':>8} ${grand:>13,.2f}")
finally:
    conn.close()
