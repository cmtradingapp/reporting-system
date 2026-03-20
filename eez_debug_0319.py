"""
EEZ Debug: Compare snapshot vs broken live vs FIXED live formula for 2026-03-19.

Run on server:
  docker cp /opt/reporting-system/reporting-system/eez_debug_0319.py reporting-system-app-1:/tmp/eez_debug_0319.py
  docker exec reporting-system-app-1 python3 /tmp/eez_debug_0319.py
"""
import psycopg2

TARGET_DATE = "2026-03-19"
PREV_DATE   = "2026-03-18"

conn = psycopg2.connect(
    host="127.0.0.1", port=5432,
    user="postgres", password="8PpVuUasBVR85T7WuAec",
    dbname="datawarehouse"
)

with conn.cursor() as cur:

    # 1. Snapshot for 2026-03-19
    cur.execute("SELECT login, end_equity_zeroed FROM daily_equity_zeroed WHERE day = %s", (TARGET_DATE,))
    snapshot = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # 2. Start EEZ (2026-03-18 snapshot)
    cur.execute("SELECT login, end_equity_zeroed FROM daily_equity_zeroed WHERE day = %s", (PREV_DATE,))
    start_eez = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # 3. Net deposits on 2026-03-19 per login
    cur.execute("""
        SELECT ta.login::bigint,
               COALESCE(SUM(CASE
                   WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                   WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
               END), 0)
        FROM transactions t
        JOIN accounts a           ON a.accountid = t.vtigeraccountid
        JOIN trading_accounts ta  ON ta.vtigeraccountid = t.vtigeraccountid
        JOIN crm_users u          ON u.id = t.original_deposit_owner
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND t.confirmation_time::date = %s
          AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
        GROUP BY ta.login::bigint
    """, (TARGET_DATE,))
    net_deposits = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # 4. Balance + floating PnL from dealio_daily_profits for 2026-03-19
    cur.execute("""
        SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
        FROM dealio_daily_profits
        WHERE date::date = %s
        ORDER BY login, date DESC
    """, (TARGET_DATE,))
    raw_equity = {}
    for r in cur.fetchall():
        raw_equity[int(r[0])] = {"balance": float(r[1] or 0), "fpnl": float(r[2] or 0)}

    # 5. Cumulative bonus up to 2026-03-19
    cur.execute("""
        SELECT login, SUM(net_amount) FROM bonus_transactions
        WHERE confirmation_time::date <= %s GROUP BY login
    """, (TARGET_DATE,))
    bonus_cum = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # 6. Today's new bonuses only (2026-03-19)
    cur.execute("""
        SELECT login, SUM(net_amount) FROM bonus_transactions
        WHERE confirmation_time::date = %s GROUP BY login
    """, (TARGET_DATE,))
    bonus_today = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # 7. Valid logins
    cur.execute("""
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0 AND ta.vtigeraccountid IS NOT NULL
    """)
    valid_logins = {int(r[0]) for r in cur.fetchall()}

conn.close()

# ── Per-login computation ─────────────────────────────────────────────────────
THRESHOLD = 50.0
rows = []

all_logins = valid_logins & (set(snapshot) | set(start_eez) | set(raw_equity))

for login in sorted(all_logins):
    snap    = snapshot.get(login, 0.0)
    start   = start_eez.get(login, 0.0)
    dep     = net_deposits.get(login, 0.0)
    eq      = raw_equity.get(login)
    bal     = eq["balance"] if eq else 0.0
    fpnl    = eq["fpnl"]    if eq else 0.0
    equity  = bal + fpnl
    bon_cum = max(0.0, bonus_cum.get(login, 0.0))
    bon_new = bonus_today.get(login, 0.0)   # today's bonuses only

    # ── BROKEN live formula (current code) ──────────────────────────────────
    # Bug 1: uses total cumulative bonus (already baked into start_eez)
    # Bug 2: uses total floating PnL instead of delta
    broken_live = max(0.0, start + dep + fpnl - bon_cum)

    # ── FIXED live formula ───────────────────────────────────────────────────
    # Same shape as historical: use actual equity (balance+fpnl) - cumulative bonus
    # (In production this will use live dealio equity instead of daily_profits)
    if equity <= 0:
        fixed_live = 0.0
    else:
        fixed_live = max(0.0, equity - bon_cum)

    rows.append({
        "login":       login,
        "snapshot":    snap,
        "broken_live": round(broken_live, 2),
        "fixed_live":  round(fixed_live, 2),
        "diff_broken": round(broken_live - snap, 2),
        "diff_fixed":  round(fixed_live  - snap, 2),
        "start_eez":   start,
        "net_dep":     dep,
        "balance":     bal,
        "fpnl":        fpnl,
        "bon_cum":     bon_cum,
        "bon_new":     bon_new,
    })

# ── Print logins where fixed differs from snapshot by > threshold ─────────────
print(f"\n{'='*150}")
print(f"LOGIN COMPARISON: snapshot vs broken live vs FIXED live  ({TARGET_DATE})")
print(f"Showing logins where |fixed - snapshot| > ${THRESHOLD}")
print(f"{'='*150}")
hdr = (f"{'LOGIN':>12}  {'SNAPSHOT':>12}  {'BROKEN_LIVE':>12}  {'FIXED_LIVE':>12}  "
       f"{'DIFF_BRK':>10}  {'DIFF_FIX':>10}  {'START_EEZ':>12}  "
       f"{'BALANCE':>12}  {'FPNL':>12}  {'BON_CUM':>10}  {'BON_NEW':>8}")
print(hdr)
print("-"*150)

filtered = [r for r in rows if abs(r["diff_fixed"]) > THRESHOLD]
filtered.sort(key=lambda x: abs(x["diff_fixed"]), reverse=True)
for r in filtered:
    print(
        f"{r['login']:>12}  {r['snapshot']:>12,.2f}  {r['broken_live']:>12,.2f}  "
        f"{r['fixed_live']:>12,.2f}  {r['diff_broken']:>10,.2f}  {r['diff_fixed']:>10,.2f}  "
        f"{r['start_eez']:>12,.2f}  {r['balance']:>12,.2f}  "
        f"{r['fpnl']:>12,.2f}  {r['bon_cum']:>10,.2f}  {r['bon_new']:>8,.2f}"
    )

# ── Totals ────────────────────────────────────────────────────────────────────
snap_total   = sum(r["snapshot"]    for r in rows)
broken_total = sum(r["broken_live"] for r in rows)
fixed_total  = sum(r["fixed_live"]  for r in rows)

print(f"\n{'='*80}")
print(f"TOTALS")
print(f"{'='*80}")
print(f"  Stored snapshot          : ${snap_total:>15,.2f}")
print(f"  Broken live formula      : ${broken_total:>15,.2f}   diff: ${broken_total - snap_total:>12,.2f}")
print(f"  FIXED live formula       : ${fixed_total:>15,.2f}   diff: ${fixed_total  - snap_total:>12,.2f}")
print(f"\n  Total logins compared    : {len(rows)}")
print(f"  Logins |fixed-snap| > $50: {len(filtered)}")

# ── Breakdown of remaining fixed vs snapshot gap ──────────────────────────────
still_off = [(r["login"], r["diff_fixed"]) for r in rows if abs(r["diff_fixed"]) > THRESHOLD]
if still_off:
    over  = sum(d for _, d in still_off if d > 0)
    under = sum(d for _, d in still_off if d < 0)
    print(f"\n  Fixed over  snapshot (live > snap): ${over:>12,.2f}")
    print(f"  Fixed under snapshot (live < snap): ${under:>12,.2f}")
