#!/usr/bin/env python3
"""Re-run EEZ snapshots using pure SQL (no mysql dependency)."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from app.db.postgres_conn import get_connection

sql_eez = """
    WITH latest_equity AS (
        SELECT DISTINCT ON (login)
            login, convertedbalance, convertedfloatingpnl
        FROM dealio_daily_profits
        WHERE date::date = %(d)s
        ORDER BY login, date DESC
    ),
    bonus_bal AS (
        SELECT login, SUM(net_amount) AS bonus_balance
        FROM bonus_transactions
        WHERE confirmation_time::date <= %(d)s
        GROUP BY login
    ),
    test_flags AS (
        SELECT ta.login::bigint AS login, MAX(a.is_test_account) AS is_test
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
        GROUP BY ta.login::bigint
    )
    SELECT
        le.login,
        ROUND(CASE
            WHEN COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0) <= 0 THEN 0
            ELSE GREATEST(
                COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0)
                    - COALESCE(b.bonus_balance, 0),
                0
            )
        END::numeric, 2) AS eez
    FROM latest_equity le
    LEFT JOIN bonus_bal b  ON b.login = le.login
    JOIN test_flags tf ON tf.login = le.login
    WHERE tf.is_test = 0
"""

for snapshot_date in ['2026-04-18', '2026-04-19', '2026-04-20']:
    from datetime import datetime, timedelta
    prev_date = (datetime.strptime(snapshot_date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")

    conn = get_connection()
    cur = conn.cursor()

    # End EEZ
    cur.execute(sql_eez, {"d": snapshot_date})
    end_rows = cur.fetchall()

    # Start EEZ
    cur.execute(sql_eez, {"d": prev_date})
    start_rows = cur.fetchall()

    start_map = {login: eez for login, eez in start_rows}

    # Upsert
    for login, end_eez in end_rows:
        start_eez = start_map.get(login)
        cur.execute("""
            INSERT INTO daily_equity_zeroed (login, day, end_equity_zeroed, start_equity_zeroed)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (login, day) DO UPDATE SET
                end_equity_zeroed = EXCLUDED.end_equity_zeroed,
                start_equity_zeroed = EXCLUDED.start_equity_zeroed
        """, (login, snapshot_date, end_eez, start_eez))

    conn.commit()
    conn.close()
    print(f"Snapshot {snapshot_date}: {len(end_rows)} logins")

print("\nDone! EEZ snapshots rebuilt.")
