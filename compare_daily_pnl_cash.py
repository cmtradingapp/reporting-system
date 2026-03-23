import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

# Dealio's numbers for reference
DEALIO = {
    '2026-03-23': -155844.47,
    '2026-03-22': -14424.70,
    '2026-03-21': -44154.84,
    '2026-03-20': -270655.16,
    '2026-03-19': -422664.80,
    '2026-03-18': -654360.68,
    '2026-03-17': -175503.58,
    '2026-03-16': -191888.22,
    '2026-03-15': -1527.01,
    '2026-03-14': -57922.57,
    '2026-03-13': -306280.28,
    '2026-03-12': -389207.58,
    '2026-03-11': -191393.41,
    '2026-03-10': -94138.67,
    '2026-03-09': -606834.62,
    '2026-03-08': -9572.73,
    '2026-03-07': -56967.27,
    '2026-03-06': 361107.87,
    '2026-03-05': -279956.67,
    '2026-03-04': 370.40,
    '2026-03-03': -708348.23,
    '2026-03-02': -301158.91,
    '2026-03-01': -10417.25,
}

conn = get_connection()
try:
    with conn.cursor() as cur:
        # End EEZ per day from snapshot (non-test accounts only, already filtered in snapshot)
        cur.execute("""
            SELECT day, SUM(end_equity_zeroed) AS end_eez
            FROM daily_equity_zeroed
            WHERE day BETWEEN '2026-02-28' AND '2026-03-23'
              AND login IN (
                  SELECT ta.login::bigint
                  FROM trading_accounts ta
                  JOIN accounts a ON a.accountid = ta.vtigeraccountid
                  WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
                    AND a.is_test_account = 0
                    AND ta.vtigeraccountid IS NOT NULL
              )
            GROUP BY day
            ORDER BY day
        """)
        eez_rows = {str(r[0]): float(r[1]) for r in cur.fetchall()}

        # Net deposits per day (same logic as live_equity)
        cur.execute("""
            SELECT t.confirmation_time::date AS day,
                   SUM(CASE
                       WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                       WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                   END) AS net_dep
            FROM transactions t
            JOIN crm_users u ON u.id = t.original_deposit_owner
            JOIN accounts a  ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.confirmation_time >= '2026-03-01'
              AND t.confirmation_time <  '2026-03-24'
              AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
              AND t.vtigeraccountid IS NOT NULL
              AND a.is_test_account = 0
              AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%bonus%'
            GROUP BY t.confirmation_time::date
            ORDER BY day
        """)
        dep_rows = {str(r[0]): float(r[1]) for r in cur.fetchall()}
finally:
    conn.close()

dates = sorted(DEALIO.keys())

print(f"{'Date':<12} {'End EEZ':>14} {'Prev EEZ':>14} {'Net Dep':>12} {'Ours (CEO)':>14} {'Dealio':>14} {'Diff':>12}")
print("-" * 96)

our_total    = 0.0
dealio_total = 0.0

for d in dates:
    end_eez  = eez_rows.get(d, None)
    prev_day = str((
        __import__('datetime').date.fromisoformat(d) -
        __import__('datetime').timedelta(days=1)
    ))
    prev_eez = eez_rows.get(prev_day, None)
    net_dep  = dep_rows.get(d, 0.0)
    dealio_v = DEALIO[d]

    if end_eez is not None and prev_eez is not None:
        ours = round(end_eez - prev_eez - net_dep, 2)
    else:
        ours = None

    diff = round(ours - dealio_v, 2) if ours is not None else None

    if ours is not None:
        our_total += ours
    dealio_total += dealio_v

    end_str  = f"${end_eez:,.0f}"  if end_eez  is not None else "N/A"
    prev_str = f"${prev_eez:,.0f}" if prev_eez is not None else "N/A"
    dep_str  = f"${net_dep:,.0f}"
    our_str  = f"${ours:,.2f}"     if ours is not None else "N/A"
    dea_str  = f"${dealio_v:,.2f}"
    dif_str  = f"${diff:,.2f}"     if diff is not None else "N/A"

    print(f"{d:<12} {end_str:>14} {prev_str:>14} {dep_str:>12} {our_str:>14} {dea_str:>14} {dif_str:>12}")

print("-" * 96)
print(f"{'TOTAL':<12} {'':>14} {'':>14} {'':>12} ${our_total:>13,.2f} ${dealio_total:>13,.2f} ${our_total-dealio_total:>11,.2f}")
