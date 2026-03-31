import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection

dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        # Show all columns in dealio.positions
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'dealio' AND table_name = 'positions'
            ORDER BY ordinal_position
        """)
        print("positions columns:", [r[0] for r in cur.fetchall()])

        # Compare profit vs computedprofit for ZAR account (non-USD)
        # If computedprofit != profit -> it's USD-converted
        cur.execute("""
            SELECT login, profit, computedprofit, computedcommission, computedswap
            FROM dealio.positions
            WHERE login = 141319759
            LIMIT 5
        """)
        rows = cur.fetchall()
        print("\nZAR account 141319759 positions (profit vs computedprofit):")
        if rows:
            print(f"  {'profit':>14} {'computedprofit':>16} {'computedcomm':>14} {'computedswap':>14}")
            for r in rows:
                print(f"  {str(r[1]):>14} {str(r[2]):>16} {str(r[3]):>14} {str(r[4]):>14}")
        else:
            print("  No open positions for this login")

        # Also check a USD account for comparison
        cur.execute("""
            SELECT login, profit, computedprofit
            FROM dealio.positions
            WHERE login IN (141932329, 141896222, 141635360)
            LIMIT 5
        """)
        rows = cur.fetchall()
        print("\nUSD accounts positions (profit vs computedprofit):")
        if rows:
            print(f"  {'login':>12} {'profit':>14} {'computedprofit':>16}")
            for r in rows:
                print(f"  {r[0]:>12} {str(r[1]):>14} {str(r[2]):>16}")
        else:
            print("  No open positions")
finally:
    dc.close()
