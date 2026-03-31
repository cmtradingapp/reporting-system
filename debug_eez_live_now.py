"""
Run live EEZ formula right now using dealio replica.
Formula: MAX(0, compbalance + live_floating - bonus)
Compare to: yesterday snapshot, CEO formula estimate.
"""
import sys, time
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today     = datetime.now(_TZ).date()
yesterday = today - timedelta(days=1)
d         = str(today)
print(f"Live EEZ check at: {datetime.now(_TZ).strftime('%H:%M:%S')}  ({d})\n")

# ── 1. Local postgres ──────────────────────────────────────────────────────────
conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE ta.equity > 0
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
        """)
        equity_logins = [int(r[0]) for r in cur.fetchall()]

        cur.execute("""
            SELECT login, SUM(net_amount)
            FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
              AND login = ANY(%s)
            GROUP BY login
        """, (d, equity_logins))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        cur.execute("""
            SELECT COALESCE(SUM(end_equity_zeroed), 0)
            FROM daily_equity_zeroed
            WHERE day = %s::date
              AND login IN (
                  SELECT login::bigint FROM trading_accounts
                  WHERE vtigeraccountid IS NOT NULL
                    AND (deleted = 0 OR deleted IS NULL)
              )
        """, (str(yesterday),))
        start_eez = float(cur.fetchone()[0] or 0)

        cur.execute("""
            SELECT COALESCE(SUM(CASE
                WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
            END), 0)
            FROM transactions t
            JOIN crm_users u ON u.id = t.original_deposit_owner
            JOIN accounts a  ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.confirmation_time >= %s::date
              AND t.confirmation_time <  %s::date + INTERVAL '1 day'
              AND a.is_test_account = 0
              AND LOWER(COALESCE(t.comment,'')) NOT LIKE '%%bonus%%'
        """, (d, d))
        net_deposits = float(cur.fetchone()[0] or 0)
finally:
    conn.close()

print(f"  Equity logins (ta.equity > 0): {len(equity_logins):,}")

# ── 2. Dealio live (with retry) ────────────────────────────────────────────────
def _fetch_dealio(logins):
    dc = get_dealio_connection()
    try:
        with dc.cursor() as cur:
            cur.execute("""
                SELECT login,
                       SUM(COALESCE(computedcommission,0)
                         + COALESCE(computedprofit,0)
                         + COALESCE(computedswap,0))
                FROM dealio.positions
                WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
                GROUP BY login
            """, (logins, _EXCLUDED_SYMBOLS_TUPLE))
            floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

            cur.execute(
                "SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)",
                (logins,)
            )
            bal_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
        return floating_map, bal_map
    finally:
        dc.close()

_RETRYABLE = ("conflict with recovery", "ssl syscall", "eof detected", "timeout expired")
floating_map = bal_map = None
for attempt in range(5):
    try:
        floating_map, bal_map = _fetch_dealio(equity_logins)
        print(f"  Dealio connected (attempt {attempt+1})")
        break
    except Exception as e:
        print(f"  Retry {attempt+1}: {e}")
        if attempt < 4 and any(s in str(e).lower() for s in _RETRYABLE):
            time.sleep(3)
        else:
            raise

# ── 3. Calculate ───────────────────────────────────────────────────────────────
live_eez = 0.0
for login, bal in bal_map.items():
    flt   = floating_map.get(login, 0.0)
    bonus = max(0.0, bonus_map.get(login, 0.0))
    live_eez += max(0.0, bal + flt - bonus)

total_floating = sum(floating_map.values())

# ── 4. Output ──────────────────────────────────────────────────────────────────
print(f"\n{'─'*60}")
print(f"  Yesterday snapshot:   {start_eez:>14,.0f}")
print(f"  Net deposits today:   {net_deposits:>14,.0f}")
print(f"  CEO base (no PnL):    {start_eez + net_deposits:>14,.0f}")
print(f"{'─'*60}")
print(f"  Live EEZ (now):       {live_eez:>14,.0f}")
print(f"{'─'*60}")
print(f"  Total live floating:  {total_floating:>14,.0f}")
print(f"  Accounts w/ balance:  {len(bal_map):>14,}")
print(f"  Accounts w/ positions:{len(floating_map):>14,}")
