"""
debug_daily_pnl_cash.py

Tests whether using compprevequity from dealio.users (Dealio's own SOD value)
gives a daily_pnl_cash that matches Dealio's export.

Compares four approaches:
  A) Current:     SUM(MAX(0, convertedbalance + convertedfloatingpnl)) from dealio_daily_profits yesterday
  B) compprevequity:  SUM(MAX(0, compprevequity)) from dealio.users  (Dealio's own SOD)
  C) equityprevday:   SUM(equityprevday) from today's dealio_daily_profits records
  D) compprevbalance: SUM(compprevbalance) from dealio.users  (SOD balance without floating)

Dealio export total (11:15 CY 24/3/2026): -15,810
Our system at 11:13:                       -15,181

Run:
    docker exec reporting-system-app-1 python debug_daily_pnl_cash.py
"""

from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = str(datetime.now(_TZ).date())
print(f"today (Cyprus): {today}")
print()

# ── Get equity_logins ─────────────────────────────────────────────────────
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

    # A) Current approach: yesterday's convertedbalance + convertedfloatingpnl
    cur.execute("""
        SELECT COALESCE(SUM(CASE
            WHEN COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0) <= 0 THEN 0
            ELSE COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0)
        END), 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s::date - INTERVAL '1 day'
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    """, (today, equity_logins))
    start_a = float(cur.fetchone()[0] or 0)

    # C) equityprevday from today's daily_profits records (Dealio's own SOD stored in today's rows)
    cur.execute("""
        SELECT COALESCE(SUM(d.equityprevday), 0)
        FROM (
            SELECT DISTINCT ON (login) login, equityprevday
            FROM dealio_daily_profits
            WHERE date::date = %s::date
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
          AND d.equityprevday IS NOT NULL
    """, (today, equity_logins))
    start_c = float(cur.fetchone()[0] or 0)

    # Net deposits today
    cur.execute("""
        SELECT COALESCE(SUM(net_usd), 0)
        FROM mv_daily_kpis
        WHERE tx_date = %s::date
    """, (today,))
    net_deposits_today = float(cur.fetchone()[0] or 0)

    # Today bonuses
    cur.execute("""
        SELECT COALESCE(SUM(net_amount), 0)
        FROM bonus_transactions
        WHERE confirmation_time::date = %s
    """, (today,))
    today_bonuses = float(cur.fetchone()[0] or 0)

pg.close()

# ── Live from Dealio ─────────────────────────────────────────────────────
dc = get_dealio_connection()
with dc.cursor() as cur:
    # compbalance, compprevequity, compprevbalance, compcredit
    cur.execute("""
        SELECT login, compbalance, compprevequity, compprevbalance, compcredit
        FROM dealio.users
        WHERE login = ANY(%s)
    """, (equity_logins,))
    rows = cur.fetchall()
    bal_map          = {int(r[0]): float(r[1] or 0) for r in rows}
    prev_equity_map  = {int(r[0]): float(r[2] or 0) for r in rows}
    prev_bal_map     = {int(r[0]): float(r[3] or 0) for r in rows}
    credit_map       = {int(r[0]): float(r[4] or 0) for r in rows}

    # floating per login
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

dc.close()

# ── Compute daily_end_net_equity ─────────────────────────────────────────
daily_end_net_equity = sum(
    max(0.0, bal_map.get(l, 0) + floating_map.get(l, 0))
    for l in equity_logins
    if l in bal_map
)

# ── B) compprevequity from dealio.users ──────────────────────────────────
start_b = sum(max(0.0, prev_equity_map.get(l, 0)) for l in equity_logins if l in prev_equity_map)

# ── D) compprevbalance (SOD balance, no floating) ────────────────────────
start_d = sum(max(0.0, prev_bal_map.get(l, 0)) for l in equity_logins if l in prev_bal_map)

# ── Results ───────────────────────────────────────────────────────────────
pnl_a = round(daily_end_net_equity - start_a - net_deposits_today - today_bonuses)
pnl_b = round(daily_end_net_equity - start_b - net_deposits_today - today_bonuses)
pnl_c = round(daily_end_net_equity - start_c - net_deposits_today - today_bonuses)
pnl_d = round(daily_end_net_equity - start_d - net_deposits_today - today_bonuses)

print(f"equity_logins:             {len(equity_logins):>10,}")
print(f"daily_end_net_equity:      ${daily_end_net_equity:>12,.0f}")
print(f"net_deposits_today:        ${net_deposits_today:>12,.0f}")
print(f"today_bonuses:             ${today_bonuses:>12,.0f}")
print()
print("─" * 60)
print(f"{'Approach':<45} {'SOD':>10}  {'RESULT':>10}")
print("─" * 60)
print(f"A) current (dealio_daily_profits yesterday): ${start_a:>10,.0f}  ${pnl_a:>10,}")
print(f"B) compprevequity  (dealio.users):           ${start_b:>10,.0f}  ${pnl_b:>10,}")
print(f"C) equityprevday   (today's daily_profits):  ${start_c:>10,.0f}  ${pnl_c:>10,}")
print(f"D) compprevbalance (dealio.users, no float): ${start_d:>10,.0f}  ${pnl_d:>10,}")
print("─" * 60)
print(f"Dealio export (11:15 CY):                                ${-15810:>10,}")
print()

# ── Which approach is closest to Dealio? ─────────────────────────────────
dealio_ref = -15810
gaps = {
    "A (current)":      abs(pnl_a - dealio_ref),
    "B (compprevequity)": abs(pnl_b - dealio_ref),
    "C (equityprevday)":  abs(pnl_c - dealio_ref),
    "D (compprevbalance)": abs(pnl_d - dealio_ref),
}
best = min(gaps, key=gaps.get)
print("Gap vs Dealio (-$15,810):")
for name, gap in gaps.items():
    marker = "  <-- BEST" if name == best else ""
    print(f"  {name:<30} ${gap:>8,.0f}{marker}")

# ── E) Read today's daily_profits directly from Dealio replica ────────────
# Uses the same pre-computed columns Dealio's UI reads:
#   convertedclosedpnl + converteddeltafloatingpnl - convertednetdeposit
# Tested with two filters:
#   E1 = all logins (no test filter) — matches Dealio's unfiltered total
#   E2 = our equity_logins only
print()
print("─" * 60)
print("E) Read dealio.daily_profits LIVE from Dealio replica (today)")
print("─" * 60)
dc2 = get_dealio_connection()
with dc2.cursor() as cur:
    # E1: all logins (no filter) — should match Dealio export exactly
    cur.execute("""
        SELECT COALESCE(SUM(
            COALESCE(d.convertedclosedpnl, 0)
            + COALESCE(d.converteddeltafloatingpnl, 0)
            - COALESCE(d.convertednetdeposit, 0)
        ), 0)
        FROM (
            SELECT DISTINCT ON (login)
                login, convertedclosedpnl, converteddeltafloatingpnl, convertednetdeposit
            FROM dealio.daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d
    """, (today,))
    e1 = float(cur.fetchone()[0])

    # E2: filtered by our equity_logins (non-test accounts)
    cur.execute("""
        SELECT COALESCE(SUM(
            COALESCE(d.convertedclosedpnl, 0)
            + COALESCE(d.converteddeltafloatingpnl, 0)
            - COALESCE(d.convertednetdeposit, 0)
        ), 0)
        FROM (
            SELECT DISTINCT ON (login)
                login, convertedclosedpnl, converteddeltafloatingpnl, convertednetdeposit
            FROM dealio.daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    """, (today, equity_logins))
    e2 = float(cur.fetchone()[0])
dc2.close()

print(f"  E1 (ALL logins, no filter):      ${e1:>12,.0f}")
print(f"  E2 (equity_logins filter):        ${e2:>12,.0f}")
print()
print("  NOTE: E1 should match Dealio's download if run at same time.")
print(f"  Dealio at 11:15 CY was:          ${-15810:>12,}")
