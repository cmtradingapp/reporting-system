"""
compare_pnl.py  — run on the server at the SAME time as a Dealio CSV export

Compares:
  A) Dealio's live Daily PnL Cash per login  (from dealio.positions + trades_mt4)
  B) Our system's equity_logins set          (from postgres trading_accounts/accounts)

Prints:
  - Login set comparison  (in both / only Dealio / only our system)
  - PnL totals broken down by login set
  - Our system's daily_pnl (same formula as check_pnl.py)
"""

from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = str(datetime.now(_TZ).date())
print(f"today (Cyprus): {today}")
print()

# ── Step 1: our equity_logins ──────────────────────────────────────────────
pg = get_connection()
with pg.cursor() as cur:
    cur.execute('''
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
    ''')
    our_logins = set(r[0] for r in cur.fetchall())
pg.close()
print(f"Our equity_logins:  {len(our_logins):,}")

# ── Step 2: Dealio — ALL logins with a position or closed trade today ───────
dc = get_dealio_connection()
with dc.cursor() as cur:
    # Floating PnL per login (open positions)
    cur.execute('''
        SELECT login,
               SUM(COALESCE(computedcommission,0)+COALESCE(computedprofit,0)+COALESCE(computedswap,0))
        FROM dealio.positions
        WHERE cmd < 2 AND symbol NOT IN %s
        GROUP BY login
    ''', (_EXCLUDED_SYMBOLS_TUPLE,))
    floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # Closed PnL today per login
    cur.execute('''
        SELECT login,
               COALESCE(SUM(COALESCE(computed_commission,0)+COALESCE(computed_profit,0)+COALESCE(computed_swap,0)),0)
        FROM dealio.trades_mt4
        WHERE close_time >= %s::date
          AND close_time <  %s::date + INTERVAL '1 day'
          AND cmd < 2
          AND symbol NOT IN %s
        GROUP BY login
    ''', (today, today, _EXCLUDED_SYMBOLS_TUPLE))
    closed_today_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

dc.close()

dealio_logins = set(floating_map.keys()) | set(closed_today_map.keys())
print(f"Dealio logins (pos or closed today): {len(dealio_logins):,}")

# Combine per login: floating + closed_today (approximate daily_pnl per login)
# Note: this is NOT exactly Dealio's "Daily PnL Cash" formula (which deducts SOD)
# but it's the closest we can compute live per login.
all_logins = our_logins | dealio_logins
in_both       = our_logins & dealio_logins
only_dealio   = dealio_logins - our_logins
only_ours     = our_logins - dealio_logins

def pnl_for_logins(login_set):
    total_f = sum(floating_map.get(l, 0) for l in login_set)
    total_c = sum(closed_today_map.get(l, 0) for l in login_set)
    return total_f + total_c

pnl_both        = pnl_for_logins(in_both)
pnl_only_dealio = pnl_for_logins(only_dealio)
pnl_only_ours   = pnl_for_logins(only_ours)  # will be 0 (no Dealio data for these)
pnl_all_dealio  = pnl_for_logins(dealio_logins)

print()
print("─" * 55)
print("LOGIN SET COMPARISON")
print("─" * 55)
print(f"  In both (our logins ∩ Dealio):  {len(in_both):>7,}  PnL = ${pnl_both:>12,.0f}")
print(f"  Only in Dealio (not our list):  {len(only_dealio):>7,}  PnL = ${pnl_only_dealio:>12,.0f}")
print(f"  Only ours   (not in Dealio):    {len(only_ours):>7,}  PnL = ${'0':>12}")
print()
print(f"  All Dealio logins total PnL:    {' ':>7}  PnL = ${pnl_all_dealio:>12,.0f}")
print(f"  Our system scope PnL (in_both): {' ':>7}  PnL = ${pnl_both:>12,.0f}")
print()

# ── Step 3: run the same formula as check_pnl.py ──────────────────────────
pg2 = get_connection()
with pg2.cursor() as cur:
    cur.execute('''
        SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl,0)),0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s::date - INTERVAL '1 day'
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    ''', (today, list(in_both)))
    eod_yesterday = float(cur.fetchone()[0])
pg2.close()

dc2 = get_dealio_connection()
with dc2.cursor() as cur:
    cur.execute('''
        SELECT COALESCE(SUM(COALESCE(computed_commission,0)+COALESCE(computed_profit,0)+COALESCE(computed_swap,0)),0)
        FROM dealio.trades_mt4
        WHERE login = ANY(%s)
          AND close_time >= %s::date
          AND close_time <  %s::date + INTERVAL '1 day'
          AND cmd < 2 AND symbol NOT IN %s
    ''', (list(in_both), today, today, _EXCLUDED_SYMBOLS_TUPLE))
    closed_in_both = float(cur.fetchone()[0])
dc2.close()

current_float_in_both = sum(floating_map.get(l, 0) for l in in_both)
delta = current_float_in_both - eod_yesterday
our_daily_pnl = round(delta + closed_in_both)

print("─" * 55)
print("OUR SYSTEM daily_pnl (for in_both logins)")
print("─" * 55)
print(f"  current_floating:       ${current_float_in_both:>12,.0f}")
print(f"  eod_floating_yesterday: ${eod_yesterday:>12,.0f}")
print(f"  delta_floating:         ${delta:>12,.0f}")
print(f"  closed_today:           ${closed_in_both:>12,.0f}")
print(f"  DAILY PNL:              ${our_daily_pnl:>12,}")
print()

# ── Top 10 logins in Dealio that we exclude ────────────────────────────────
if only_dealio:
    top_excluded = sorted(
        [(l, floating_map.get(l,0) + closed_today_map.get(l,0)) for l in only_dealio],
        key=lambda x: abs(x[1]), reverse=True
    )[:10]
    print("─" * 55)
    print("TOP 10 Dealio logins EXCLUDED from our system:")
    print("─" * 55)
    for login, pnl in top_excluded:
        print(f"  {login}: ${pnl:,.2f}")
