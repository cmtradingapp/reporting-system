import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

LOGINS_FILE = '/tmp/logins_clean.txt'
SNAPSHOT_DATE = '2026-03-22'

with open(LOGINS_FILE) as f:
    pbi_logins = set(int(x.strip()) for x in f.read().split(',') if x.strip().isdigit())

print(f"Power BI logins: {len(pbi_logins)}")

conn = get_connection()
try:
    with conn.cursor() as cur:
        # Our snapshot logins for that date
        cur.execute("SELECT login FROM daily_equity_zeroed WHERE day = %s", (SNAPSHOT_DATE,))
        our_logins = set(row[0] for row in cur.fetchall())
        print(f"Our snapshot logins ({SNAPSHOT_DATE}): {len(our_logins)}")

        in_pbi_not_ours = pbi_logins - our_logins
        in_ours_not_pbi = our_logins - pbi_logins
        in_both = pbi_logins & our_logins
        print(f"In both: {len(in_both)}")
        print(f"In PBI but NOT our snapshot: {len(in_pbi_not_ours)}")
        print(f"In our snapshot but NOT PBI: {len(in_ours_not_pbi)}")

        if in_pbi_not_ours:
            ids = tuple(in_pbi_not_ours)
            # Check if they exist in dealio_daily_profits
            cur.execute("""
                SELECT COUNT(DISTINCT login) as in_dealio,
                       SUM(GREATEST(0, convertedbalance + convertedfloatingpnl)) as raw_eez
                FROM dealio_daily_profits
                WHERE date::date = %s AND login = ANY(%s)
            """, (SNAPSHOT_DATE, list(in_pbi_not_ours)))
            row = cur.fetchone()
            print(f"\nPBI-only logins in dealio_daily_profits on {SNAPSHOT_DATE}: {row[0]}, raw EEZ: {row[1]}")

            # Check test flag
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN a.is_test_account = 0 THEN 1 ELSE 0 END) as non_test,
                    SUM(CASE WHEN a.is_test_account = 1 THEN 1 ELSE 0 END) as test,
                    SUM(CASE WHEN ta.login IS NULL THEN 1 ELSE 0 END) as no_trading_account
                FROM dealio_daily_profits d
                LEFT JOIN trading_accounts ta ON ta.login::bigint = d.login AND (ta.deleted = 0 OR ta.deleted IS NULL)
                LEFT JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE d.date::date = %s AND d.login = ANY(%s)
            """, (SNAPSHOT_DATE, list(in_pbi_not_ours)))
            row = cur.fetchone()
            print(f"Of those: non_test={row[1]}, test={row[2]}, no_trading_account={row[3]}")

        if in_ours_not_pbi:
            cur.execute("""
                SELECT SUM(end_equity_zeroed) as eez_excluded_by_pbi
                FROM daily_equity_zeroed
                WHERE day = %s AND login = ANY(%s)
            """, (SNAPSHOT_DATE, list(in_ours_not_pbi)))
            row = cur.fetchone()
            print(f"\nOur-only logins EEZ (excluded by PBI): {row[0]}")

        # EEZ comparison for matching logins
        cur.execute("""
            SELECT SUM(end_equity_zeroed) as our_eez
            FROM daily_equity_zeroed
            WHERE day = %s AND login = ANY(%s)
        """, (SNAPSHOT_DATE, list(in_both)))
        our_shared = cur.fetchone()[0]
        print(f"\nEEZ for shared logins — ours: {our_shared}")
        print("Compare this to Power BI total for same date to isolate bonus difference")

finally:
    conn.close()
