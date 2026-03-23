import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from datetime import date, timedelta

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
        # End EEZ per day from snapshot
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

        # Net deposits per day
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

        # Dealio formula per account per day:
        # derived_credit = convertedequity - convertedbalance - convertedfloatingpnl
        # end_eez_i = MAX(0, convertedfloatingpnl_i - derived_credit_i)
        # dealio_formula = SUM(end_eez_i) - SUM(start_eez_i) - net_deposits
        # Using DISTINCT ON to get one row per login per day (latest)
        cur.execute("""
            WITH daily AS (
                SELECT DISTINCT ON (date::date, login)
                    date::date AS day,
                    login,
                    convertedfloatingpnl,
                    convertedequity,
                    convertedbalance
                FROM dealio_daily_profits
                WHERE date >= '2026-02-28' AND date < '2026-03-24'
                ORDER BY date::date, login, date DESC
            )
            SELECT
                day,
                SUM(GREATEST(0,
                    convertedfloatingpnl
                    - GREATEST(0, convertedequity - convertedbalance - convertedfloatingpnl)
                )) AS dealio_eez
            FROM daily
            GROUP BY day
            ORDER BY day
        """)
        dealio_eez_rows = {str(r[0]): float(r[1]) for r in cur.fetchall()}

finally:
    conn.close()

dates = sorted(DEALIO.keys())

print(f"{'Date':<12} {'CEO Ours':>14} {'Dealio Ref':>14} {'Dealio Formula':>16} {'Diff CEO':>12} {'Diff Formula':>14}")
print("-" * 86)

ceo_total     = 0.0
dealio_total  = 0.0
formula_total = 0.0

for d in dates:
    end_eez  = eez_rows.get(d)
    prev_day = str(date.fromisoformat(d) - timedelta(days=1))
    prev_eez = eez_rows.get(prev_day)
    net_dep  = dep_rows.get(d, 0.0)
    dealio_v = DEALIO[d]

    # CEO formula: end_eez - prev_eez - net_deposits
    ceo = round(end_eez - prev_eez - net_dep, 2) if (end_eez and prev_eez) else None

    # Dealio formula: SUM(MAX(0, float - credit)) today - SUM(MAX(0, float - credit)) yesterday - net_dep
    d_end  = dealio_eez_rows.get(d)
    d_prev = dealio_eez_rows.get(prev_day)
    formula = round(d_end - d_prev - net_dep, 2) if (d_end is not None and d_prev is not None) else None

    diff_ceo     = round(ceo - dealio_v, 2)     if ceo     is not None else None
    diff_formula = round(formula - dealio_v, 2) if formula is not None else None

    if ceo     is not None: ceo_total     += ceo
    if formula is not None: formula_total += formula
    dealio_total += dealio_v

    def fmt(v): return f"${v:,.2f}" if v is not None else "N/A"

    print(f"{d:<12} {fmt(ceo):>14} {fmt(dealio_v):>14} {fmt(formula):>16} {fmt(diff_ceo):>12} {fmt(diff_formula):>14}")

print("-" * 86)
print(f"{'TOTAL':<12} {fmt(ceo_total):>14} {fmt(dealio_total):>14} {fmt(formula_total):>16} {fmt(ceo_total-dealio_total):>12} {fmt(formula_total-dealio_total):>14}")
