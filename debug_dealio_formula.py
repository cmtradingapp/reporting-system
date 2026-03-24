"""
debug_dealio_formula.py

Implements Dealio's exact daily_pnl_cash formula per login:

  EEZ_end   = floating per login (live from positions, excl symbols)
  EEZ_start = yesterday's convertedfloatingpnl per login (from daily_profits)
  net_dep   = cmd=6 from trades_mt4 per login for today

  Per login:
    if EEZ_end > 0 and EEZ_start > 0: pnl = EEZ_end - EEZ_start - net_dep
    if EEZ_end > 0 and EEZ_start <= 0: pnl = EEZ_end - net_dep
    if EEZ_end <= 0 and EEZ_start > 0: pnl = -EEZ_start - net_dep
    else:                               pnl = -net_dep

  == MAX(0, EEZ_end) - MAX(0, EEZ_start) - net_dep per login, summed.

Also tests on historical dates using daily_profits EOD floating as EEZ_end proxy.

Run:
    docker exec reporting-system-app-1 python debug_dealio_formula.py
"""

from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ   = ZoneInfo("Europe/Nicosia")
today = datetime.now(_TZ).date()

KNOWN = {
    # date: known Dealio daily_pnl_cash
    today.__class__(2026,3,23): -285438,
    today.__class__(2026,3,22):  -14425,
    today.__class__(2026,3,21):  -44155,
    today.__class__(2026,3,20): -270655,
    today.__class__(2026,3,19): -422665,
    today.__class__(2026,3,18): -654361,
    today.__class__(2026,3,17): -175504,
    today.__class__(2026,3,16): -191888,
}

pg = get_connection()
with pg.cursor() as cur:
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]
pg.close()

print(f"equity_logins: {len(equity_logins):,}")
print()

# ── PART 1: Historical (using daily_profits EOD floating as EEZ_end proxy) ──
print("=" * 70)
print("PART 1: Historical — EEZ_end from daily_profits, EEZ_start from d-1")
print("=" * 70)
print(f"{'Date':<12} {'Dealio':>10} {'New formula':>12} {'Gap':>10}")
print("─" * 50)

dc = get_dealio_connection()
with dc.cursor() as cur:
    for d in sorted(KNOWN.keys(), reverse=True):
        d_prev = d - timedelta(days=1)

        # EEZ_end per login: convertedfloatingpnl from date d
        cur.execute("""
            SELECT login, COALESCE(convertedfloatingpnl, 0)
            FROM (
                SELECT DISTINCT ON (login) login, convertedfloatingpnl
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        eez_end_map = {int(r[0]): float(r[1]) for r in cur.fetchall()}

        # EEZ_start per login: convertedfloatingpnl from d_prev
        cur.execute("""
            SELECT login, COALESCE(convertedfloatingpnl, 0)
            FROM (
                SELECT DISTINCT ON (login) login, convertedfloatingpnl
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev),))
        eez_start_map = {int(r[0]): float(r[1]) for r in cur.fetchall()}

        # net_dep per login: trades_mt4 cmd=6 for date d
        cur.execute("""
            SELECT login, SUM(COALESCE(computed_profit, 0))
            FROM dealio.trades_mt4
            WHERE close_time::date = %s
              AND cmd = 6
            GROUP BY login
        """, (str(d),))
        net_dep_map = {int(r[0]): float(r[1]) for r in cur.fetchall()}

        # Apply formula per login
        total = 0.0
        all_logins = set(eez_end_map) | set(eez_start_map)
        for login in all_logins:
            eez_e = eez_end_map.get(login, 0.0)
            eez_s = eez_start_map.get(login, 0.0)
            nd    = net_dep_map.get(login, 0.0)
            total += max(0.0, eez_e) - max(0.0, eez_s) - nd

        result = round(total)
        known  = KNOWN[d]
        print(f"{str(d):<12} {known:>10,} {result:>12,} {known-result:>+10,}")

print()

# ── PART 2: Live (today) ─────────────────────────────────────────────────────
print("=" * 70)
print(f"PART 2: Live — today ({today})")
print("=" * 70)

# EEZ_start from yesterday's daily_profits per login (Postgres)
pg2 = get_connection()
with pg2.cursor() as cur:
    cur.execute("""
        SELECT login, COALESCE(convertedfloatingpnl, 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s::date - INTERVAL '1 day'
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    """, (str(today), equity_logins))
    eez_start_map = {int(r[0]): float(r[1]) for r in cur.fetchall()}

    cur.execute("""
        SELECT COALESCE(SUM(net_usd), 0)
        FROM mv_daily_kpis
        WHERE tx_date = %s
    """, (str(today),))
    net_dep_total = float(cur.fetchone()[0] or 0)
pg2.close()

# Live floating + net_dep from Dealio
with dc.cursor() as cur:
    cur.execute("""
        SELECT login,
               SUM(COALESCE(computedcommission,0)
                 + COALESCE(computedprofit,0)
                 + COALESCE(computedswap,0))
        FROM dealio.positions
        WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
        GROUP BY login
    """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
    floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    cur.execute("""
        SELECT login, SUM(COALESCE(computed_profit, 0))
        FROM dealio.trades_mt4
        WHERE login = ANY(%s)
          AND cmd = 6
          AND close_time >= %s::date
          AND close_time <  %s::date + INTERVAL '1 day'
        GROUP BY login
    """, (equity_logins, str(today), str(today)))
    net_dep_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

dc.close()

# Apply Dealio formula per login (live)
total_live = 0.0
for login in equity_logins:
    eez_e = floating_map.get(login, 0.0)
    eez_s = eez_start_map.get(login, 0.0)
    nd    = net_dep_map.get(login, 0.0)
    total_live += max(0.0, eez_e) - max(0.0, eez_s) - nd

print(f"Dealio formula (live):   ${round(total_live):>12,}")
print(f"net_dep (trades_mt4):    ${sum(net_dep_map.values()):>12,.0f}")
print(f"net_dep (mv_daily_kpis): ${net_dep_total:>12,.0f}")
print()
print("Compare against Dealio's current value manually.")
