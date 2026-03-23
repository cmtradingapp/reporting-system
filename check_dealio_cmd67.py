import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection

conn = get_dealio_connection()
try:
    with conn.cursor() as cur:
        # Check ticket 37696713
        cur.execute("SELECT ticket, login, cmd, symbol, open_time, close_time, profit FROM dealio.trades_mt4 WHERE ticket = 37696713")
        row = cur.fetchone()
        print("=== Ticket 37696713 ===")
        if row:
            print(f"  ticket={row[0]}, login={row[1]}, cmd={row[2]}, symbol={row[3]}, open_time={row[4]}, close_time={row[5]}, profit={row[6]}")
        else:
            print("  NOT FOUND in trades_mt4")

        # Check if cmd 6 or 7 exist at all
        cur.execute("SELECT cmd, COUNT(*) FROM dealio.trades_mt4 WHERE cmd IN (6, 7) GROUP BY cmd ORDER BY cmd")
        rows = cur.fetchall()
        print("\n=== CMD 6 / 7 counts ===")
        if rows:
            for r in rows:
                print(f"  cmd={r[0]}: {r[1]:,} rows")
        else:
            print("  None found")

        # Sample a few if they exist
        cur.execute("SELECT ticket, login, cmd, symbol, open_time, close_time, profit FROM dealio.trades_mt4 WHERE cmd IN (6, 7) LIMIT 5")
        samples = cur.fetchall()
        if samples:
            print("\n=== Sample rows cmd 6/7 ===")
            for r in samples:
                print(f"  ticket={r[0]}, login={r[1]}, cmd={r[2]}, symbol={r[3]}, open={r[4]}, close={r[5]}, profit={r[6]}")
finally:
    conn.close()
