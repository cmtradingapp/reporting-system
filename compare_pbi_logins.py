import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGINS_FILE = '/tmp/logins_clean.txt'
SNAPSHOT_DATE = '2026-03-22'

with open(LOGINS_FILE) as f:
    pbi_logins = set(int(x.strip()) for x in f.read().split(',') if x.strip().isdigit())

conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT login FROM daily_equity_zeroed WHERE day = %s", (SNAPSHOT_DATE,))
        our_logins = set(row[0] for row in cur.fetchall())

        in_ours_not_pbi = list(our_logins - pbi_logins)
        in_pbi_not_ours = list(pbi_logins - our_logins)

        print(f"=== ACCOUNTS IN OUR SNAPSHOT BUT NOT PBI ({len(in_ours_not_pbi)}) ===")
        cur.execute("""
            SELECT
                COALESCE(ta.serverid::text, 'NO_TA') as serverid,
                COALESCE(ta.broker, 'NO_TA') as broker,
                COALESCE(ta.currency, '?') as currency,
                COUNT(DISTINCT d.login) as logins,
                ROUND(SUM(GREATEST(0, d.convertedbalance + d.convertedfloatingpnl))::numeric, 2) as raw_eez,
                ROUND(SUM(ez.end_equity_zeroed)::numeric, 2) as snapshot_eez
            FROM dealio_daily_profits d
            LEFT JOIN trading_accounts ta ON ta.login::bigint = d.login
                AND (ta.deleted = 0 OR ta.deleted IS NULL)
            LEFT JOIN daily_equity_zeroed ez ON ez.login = d.login AND ez.day = %s
            WHERE d.date::date = %s AND d.login = ANY(%s)
            GROUP BY ta.serverid, ta.broker, ta.currency
            ORDER BY logins DESC
        """, (SNAPSHOT_DATE, SNAPSHOT_DATE, in_ours_not_pbi))
        rows = cur.fetchall()
        print(f"{'serverid':<12} {'broker':<20} {'currency':<10} {'logins':>8} {'raw_eez':>14} {'snapshot_eez':>14}")
        print("-" * 80)
        for r in rows:
            print(f"{str(r[0]):<12} {str(r[1]):<20} {str(r[2]):<10} {r[3]:>8} {str(r[4]):>14} {str(r[5]):>14}")

        print(f"\n=== ACCOUNTS IN PBI BUT NOT OUR SNAPSHOT ({len(in_pbi_not_ours)}) ===")
        cur.execute("""
            SELECT
                COALESCE(ta.serverid::text, 'NO_TA') as serverid,
                COALESCE(ta.broker, 'NO_TA') as broker,
                COUNT(DISTINCT ta.login) as logins_in_ta
            FROM trading_accounts ta
            WHERE ta.login::bigint = ANY(%s)
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
            GROUP BY ta.serverid, ta.broker
            ORDER BY logins_in_ta DESC
        """, (in_pbi_not_ours,))
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"serverid={r[0]}, broker={r[1]}, logins={r[2]}")
        else:
            print("None found in trading_accounts (no trading account records)")

finally:
    conn.close()
