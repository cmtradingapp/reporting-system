import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection

logins = [141635360, 141319759, 141932329, 141896222]

dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        # Show all columns in dealio.users
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dealio' AND table_name = 'users'
            ORDER BY ordinal_position
        """)
        cols = cur.fetchall()
        print("=== dealio.users columns ===")
        for c in cols:
            print(f"  {c[0]:35s} {c[1]}")

        # Show all 'comp*' + currency/conversion fields for our test logins
        interesting = [c[0] for c in cols if
                       c[0].startswith('comp') or
                       'equity' in c[0].lower() or
                       'conver' in c[0].lower() or
                       'currency' in c[0].lower() or
                       'rate' in c[0].lower()]
        interesting = list(dict.fromkeys(interesting))  # deduplicate, preserve order
        if interesting:
            col_str = ', '.join(interesting)
            cur.execute(f"SELECT login, {col_str} FROM dealio.users WHERE login = ANY(%s)", (logins,))
            rows = cur.fetchall()
            print(f"\n=== equity/comp/conversion fields for test logins ===")
            headers = ['login'] + interesting
            print('  ' + '  '.join(f"{h:>22}" for h in headers))
            for r in rows:
                print('  ' + '  '.join(f"{str(v):>22}" for v in r))

        # Also show groupcurrency + balance/credit in native vs comp
        cur.execute("""
            SELECT login, groupcurrency, balance, credit, compbalance, compcredit,
                   compprevequity, compprevbalance
            FROM dealio.users WHERE login = ANY(%s)
        """, (logins,))
        rows = cur.fetchall()
        print("\n=== currency + balance comparison ===")
        print(f"  {'login':>12} {'currency':>10} {'balance':>14} {'credit':>14} {'compbalance':>14} {'compcredit':>14} {'compprevequity':>16} {'compprevbalance':>16}")
        for r in rows:
            print(f"  {r[0]:>12} {str(r[1]):>10} {str(r[2]):>14} {str(r[3]):>14} {str(r[4]):>14} {str(r[5]):>14} {str(r[6]):>16} {str(r[7]):>16}")
finally:
    dc.close()
