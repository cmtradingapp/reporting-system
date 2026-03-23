import sys
sys.path.insert(0, '/app')
import psycopg2
from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from app.config import (
    DEALIO_PG_HOST, DEALIO_PG_PORT, DEALIO_PG_USER,
    DEALIO_PG_PASSWORD, DEALIO_PG_DB,
    DEALIO_PG_SSLCERT, DEALIO_PG_SSLKEY, DEALIO_PG_SSLROOTCERT,
)
from datetime import date, timedelta
from collections import defaultdict

def get_dealio_long_conn():
    """Dealio connection with extended timeout + TCP keepalives for long queries."""
    return psycopg2.connect(
        host=DEALIO_PG_HOST, port=DEALIO_PG_PORT,
        user=DEALIO_PG_USER, password=DEALIO_PG_PASSWORD,
        dbname=DEALIO_PG_DB,
        connect_timeout=30,
        options="-c statement_timeout=600000",  # 10 minutes
        sslmode="require",
        sslcert=DEALIO_PG_SSLCERT, sslkey=DEALIO_PG_SSLKEY, sslrootcert=DEALIO_PG_SSLROOTCERT,
        client_encoding="utf8",
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )

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

pg_conn = get_connection()
dealio_conn = get_dealio_connection()

try:
    with pg_conn.cursor() as cur:
        # Step 1: Get valid (non-test) logins
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
              AND ta.vtigeraccountid IS NOT NULL
        """)
        valid_logins = [r[0] for r in cur.fetchall()]
        print(f"Valid logins: {len(valid_logins)}")

        valid_logins_set = set(valid_logins)

        # Step 2a: Floating PnL — filtered to valid CRM logins
        cur.execute("""
            SELECT DISTINCT ON (date::date, login)
                date::date AS day,
                login,
                convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date >= '2026-02-28' AND date < '2026-03-24'
              AND login = ANY(%s)
            ORDER BY date::date, login, date DESC
        """, (valid_logins,))
        floating = {}
        for r in cur.fetchall():
            floating[(int(r[1]), str(r[0]))] = float(r[2]) if r[2] else 0.0

        # Step 2b: Floating PnL — ALL logins (no filter)
        cur.execute("""
            SELECT DISTINCT ON (date::date, login)
                date::date AS day,
                login,
                convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date >= '2026-02-28' AND date < '2026-03-24'
            ORDER BY date::date, login, date DESC
        """)
        floating_all = {}
        for r in cur.fetchall():
            floating_all[(int(r[1]), str(r[0]))] = float(r[2]) if r[2] else 0.0
        print(f"Filtered logins in floating: {len(set(k[0] for k in floating))}")
        print(f"All logins in floating:      {len(set(k[0] for k in floating_all))}")

        # Step 3: Net deposits per day
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
    pg_conn.close()
    dealio_conn.close()

# Login sets
all_logins     = set(k[0] for k in floating.keys())
all_logins_all = set(k[0] for k in floating_all.keys())

dates = sorted(DEALIO.keys())

def fmt(v): return f"${v:,.2f}" if v is not None else "N/A"

print(f"\n{'Date':<12} {'Dealio Ref':>14} {'CEO filtered':>14} {'CEO all logins':>16} {'Diff filtered':>14} {'Diff all':>12}")
print("-" * 90)

dealio_total  = 0.0
ceo_total     = 0.0
ceo_all_total = 0.0

for d in dates:
    dealio_v = DEALIO[d]
    prev     = str(date.fromisoformat(d) - timedelta(days=1))
    net_dep  = dep_rows.get(d, 0.0)

    # CEO filtered (CRM non-test accounts only)
    end_f   = sum(max(0.0, floating.get((l, d),    0.0)) for l in all_logins if (l, d)    in floating)
    start_f = sum(max(0.0, floating.get((l, prev), 0.0)) for l in all_logins if (l, prev) in floating)
    ceo = round(end_f - start_f - net_dep, 2) if (end_f or start_f) else None

    # CEO all logins (no filter)
    end_a   = sum(max(0.0, floating_all.get((l, d),    0.0)) for l in all_logins_all if (l, d)    in floating_all)
    start_a = sum(max(0.0, floating_all.get((l, prev), 0.0)) for l in all_logins_all if (l, prev) in floating_all)
    ceo_all = round(end_a - start_a - net_dep, 2) if (end_a or start_a) else None

    diff_ceo     = round(ceo     - dealio_v, 2) if ceo     is not None else None
    diff_ceo_all = round(ceo_all - dealio_v, 2) if ceo_all is not None else None

    if ceo     is not None: ceo_total     += ceo
    if ceo_all is not None: ceo_all_total += ceo_all
    dealio_total += dealio_v

    print(f"{d:<12} {fmt(dealio_v):>14} {fmt(ceo):>14} {fmt(ceo_all):>16} {fmt(diff_ceo):>14} {fmt(diff_ceo_all):>12}")

print("-" * 90)
print(f"{'TOTAL':<12} {fmt(dealio_total):>14} {fmt(ceo_total):>14} {fmt(ceo_all_total):>16} {fmt(ceo_total-dealio_total):>14} {fmt(ceo_all_total-dealio_total):>12}")
