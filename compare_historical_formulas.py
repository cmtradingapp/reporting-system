"""
compare_historical_formulas.py

For the last N days, compute daily_pnl_cash using:
  OURS:   SUM(MAX(0, balance+floating)) EOD  - SUM(MAX(0, balance+floating)) SOD  - net_deposits - bonuses
  DEALIO: SUM(MAX(0, floating)) EOD          - SUM(MAX(0, floating)) SOD          - net_deposits

Both computed from dealio_daily_profits (our synced snapshot), so timing is identical.
This isolates the formula difference from any timing/sync gap.

Run:
    docker exec reporting-system-app-1 python compare_historical_formulas.py
"""

from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ   = ZoneInfo("Europe/Nicosia")
today = datetime.now(_TZ).date()
DAYS  = 10  # how many past days to check

pg = get_connection()
with pg.cursor() as cur:

    # ── equity_logins ────────────────────────────────────────────────────────
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]
    print(f"equity_logins: {len(equity_logins):,}")
    print()

    print(f"{'Date':<12} {'Ours PnL$':>12} {'Dealio PnL$':>12} {'Gap':>10}  {'SOD ours':>12}  {'SOD dealio':>12}")
    print("─" * 80)

    for i in range(1, DAYS + 1):
        d      = today - timedelta(days=i)
        d_prev = d     - timedelta(days=1)

        # EOD for day d: convertedbalance + convertedfloatingpnl
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb + cf <= 0 THEN 0 ELSE cb + cf END), 0) AS eod_equity,
                COALESCE(SUM(CASE WHEN cf     <= 0 THEN 0 ELSE cf         END), 0) AS eod_floating
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio_daily_profits
                WHERE date::date = %s
                  AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d), equity_logins))
        row = cur.fetchone()
        eod_equity, eod_floating = float(row[0]), float(row[1])

        # SOD = EOD of prior day
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb + cf <= 0 THEN 0 ELSE cb + cf END), 0) AS sod_equity,
                COALESCE(SUM(CASE WHEN cf     <= 0 THEN 0 ELSE cf         END), 0) AS sod_floating
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio_daily_profits
                WHERE date::date = %s
                  AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev), equity_logins))
        row = cur.fetchone()
        sod_equity, sod_floating = float(row[0]), float(row[1])

        # net deposits for day d
        cur.execute("""
            SELECT COALESCE(SUM(net_usd), 0)
            FROM mv_daily_kpis
            WHERE tx_date = %s
        """, (str(d),))
        net_dep = float(cur.fetchone()[0] or 0)

        # bonuses for day d
        cur.execute("""
            SELECT COALESCE(SUM(net_amount), 0)
            FROM bonus_transactions
            WHERE confirmation_time::date = %s
        """, (str(d),))
        bonuses = float(cur.fetchone()[0] or 0)

        pnl_ours   = round(eod_equity   - sod_equity   - net_dep - bonuses)
        pnl_dealio = round(eod_floating - sod_floating - net_dep)
        gap        = pnl_ours - pnl_dealio

        print(f"{str(d):<12} {pnl_ours:>12,}  {pnl_dealio:>12,}  {gap:>+10,}   {sod_equity:>12,.0f}  {sod_floating:>12,.0f}")

pg.close()
