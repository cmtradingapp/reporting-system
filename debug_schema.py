"""
debug_schema.py — explore dealio.users columns to find USD SOD values

Run:
    docker exec reporting-system-app-1 python debug_schema.py
"""
from app.db.dealio_conn import get_dealio_connection

dc = get_dealio_connection()
with dc.cursor() as cur:
    # All columns in dealio.users
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'dealio' AND table_name = 'users'
        ORDER BY ordinal_position
    """)
    cols = cur.fetchall()
    print("dealio.users columns:")
    for name, dtype in cols:
        print(f"  {name:<35} {dtype}")

    print()

    # Sample row for one active login to see actual values
    cur.execute("""
        SELECT * FROM dealio.users
        WHERE compbalance > 0
        LIMIT 1
    """)
    row = cur.fetchone()
    col_names = [d[0] for d in cur.description]
    if row:
        print("Sample row (compbalance > 0):")
        for name, val in zip(col_names, row):
            if val is not None and val != 0 and val != '':
                print(f"  {name:<35} {val}")

    print()

    # Also check daily_profits columns
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'dealio' AND table_name = 'daily_profits'
        ORDER BY ordinal_position
    """)
    cols2 = cur.fetchall()
    print("dealio.daily_profits columns:")
    for name, dtype in cols2:
        print(f"  {name:<35} {dtype}")

dc.close()
