import psycopg2, openpyxl
from collections import defaultdict

EXCLUDED = (
    'Cashback','CFDRollover','CommEUR','CommUSD','CommGBP','CommJPY',
    'CorrectiEUR','CorrectiGBP','CorrectiJPY','Correction',
    'CredExp','CredExpEUR','CredExpGBP','CredExpJPY',
    'Dividend','DividendEUR','DividendGBP','DividendJPY',
    'Dormant','EarnedCr','EarnedCrEUR','FEE','INACT-FEE',
    'Inactivity','Rollover','SPREAD',
    'ZeroingEUR','ZeroingGBP','ZeroingJPY','ZeroingKES',
    'ZeroingNGN','ZeroingUSD','ZeroingZAR')
COUNTRIES = ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')
CN = {'CM':'Cameroon','KE':'Kenya','SE':'Sweden','ZM':'Zambia','DK':'Denmark','NL':'Netherlands','ES':'Spain','FI':'Finland','NO':'Norway'}

# Load PBI
wb = openpyxl.load_workbook(r'C:\Users\elise.i\Downloads\data (3).xlsx', read_only=True)
rows = list(wb['Export'].iter_rows(values_only=True))
pbi = {}
for r in rows[1:]:
    pbi[str(r[1])] = {'country': r[0], 'total': r[3] or 0, 'open': r[4] or 0, 'close': r[5] or 0}

conn = psycopg2.connect(host='109.199.112.72', port=5432, dbname='datawarehouse',
                        user='postgres', password='8PpVuUasBVR85T7WuAec', connect_timeout=10)
cur = conn.cursor()

# Get our volumes per login
cur.execute("""
    SELECT t.login, a.country_iso, COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.open_time >= '2026-01-01' AND t.open_time < '2026-04-01'
      AND t.entry = 0 AND t.cmd IN (0, 1) AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
    GROUP BY t.login, a.country_iso
""", (EXCLUDED, COUNTRIES))
ours = {}
for login, country, vol in cur.fetchall():
    ours[str(login)] = {'country': CN.get(country, country), 'open': float(vol), 'close': 0.0}

cur.execute("""
    SELECT t.login, a.country_iso, COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.close_time >= '2026-01-01' AND t.close_time < '2026-04-01'
      AND t.entry = 1 AND t.close_time > '1971-01-01'
      AND t.cmd IN (0, 1) AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
    GROUP BY t.login, a.country_iso
""", (EXCLUDED, COUNTRIES))
for login, country, vol in cur.fetchall():
    login = str(login)
    if login in ours:
        ours[login]['close'] = float(vol)
    else:
        ours[login] = {'country': CN.get(country, country), 'open': 0.0, 'close': float(vol)}

# Find logins only in PBI / only in ours
only_pbi = set(pbi) - set(ours)
only_ours = set(ours) - set(pbi)

print("=== Logins only in PBI ===")
for login in sorted(only_pbi):
    d = pbi[login]
    print(f"  {login}: {d['country']}, total=${d['total']:,.2f}")
    # Check why missing - account filters?
    cur.execute("""
        SELECT a.funded, a.is_test_account, a.sales_rep_id, a.country_iso, a.compliance_status
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.login::bigint = %s
    """, (int(login),))
    r = cur.fetchone()
    if r:
        print(f"    funded={r[0]}, is_test={r[1]}, sales_rep={r[2]}, country={r[3]}, compliance={r[4]}")
    else:
        print(f"    NOT FOUND in trading_accounts/accounts join")
    # Check trades exist
    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(notional_value), 0)
        FROM dealio_trades_mt5
        WHERE login = %s AND cmd IN (0,1) AND symbol NOT IN %s
          AND ((entry=0 AND open_time >= '2026-01-01' AND open_time < '2026-04-01')
               OR (entry=1 AND close_time >= '2026-01-01' AND close_time < '2026-04-01' AND close_time > '1971-01-01'))
    """, (int(login), EXCLUDED))
    r = cur.fetchone()
    print(f"    Trades in Q1: {r[0]}, vol=${float(r[1]):,.2f}")

print("\n=== Logins only in Ours ===")
for login in sorted(only_ours):
    d = ours[login]
    total = d['open'] + d['close']
    print(f"  {login}: {d['country']}, total=${total:,.2f}")

# Deep dive: top 5 biggest diffs - check open/close separately
print("\n=== Top 10 login diffs (open vs close breakdown) ===")
diffs = []
for login in set(pbi) & set(ours):
    p = pbi[login]
    o = ours[login]
    o_total = o['open'] + o['close']
    if abs(p['total'] - o_total) > 100:
        diffs.append((login, p['country'], p['open'], o['open'], p['close'], o['close'], p['total'], o_total))

diffs.sort(key=lambda x: abs(x[6] - x[7]), reverse=True)
print(f"{'Login':<12} {'Country':<10} {'PBI Open':>14} {'Our Open':>14} {'Open Diff':>12} {'PBI Close':>14} {'Our Close':>14} {'Close Diff':>12}")
print("-" * 110)
for login, country, p_open, o_open, p_close, o_close, p_total, o_total in diffs[:10]:
    print(f"{login:<12} {country:<10} {p_open:>14,.2f} {o_open:>14,.2f} {p_open-o_open:>12,.2f} {p_close:>14,.2f} {o_close:>14,.2f} {p_close-o_close:>12,.2f}")

# Check boundary trades for top diff login
top_login = int(diffs[0][0])
print(f"\n=== Boundary analysis for login {top_login} ===")

# Trades with open_time near Q1 start/end
print("Open trades near boundaries:")
cur.execute("""
    SELECT open_time, close_time, entry, notional_value, symbol
    FROM dealio_trades_mt5
    WHERE login = %s AND cmd IN (0,1) AND entry = 0
      AND symbol NOT IN %s
      AND (open_time BETWEEN '2025-12-31 21:00' AND '2026-01-01 03:00'
           OR open_time BETWEEN '2026-03-31 21:00' AND '2026-04-01 03:00')
    ORDER BY open_time
""", (top_login, EXCLUDED))
for row in cur.fetchall():
    print(f"  open={row[0]} close={row[1]} entry={row[2]} val={float(row[3]):,.2f} sym={row[4]}")

print("Close trades near boundaries:")
cur.execute("""
    SELECT open_time, close_time, entry, notional_value, symbol
    FROM dealio_trades_mt5
    WHERE login = %s AND cmd IN (0,1) AND entry = 1
      AND symbol NOT IN %s
      AND close_time > '1971-01-01'
      AND (close_time BETWEEN '2025-12-31 21:00' AND '2026-01-01 03:00'
           OR close_time BETWEEN '2026-03-31 21:00' AND '2026-04-01 03:00')
    ORDER BY close_time
""", (top_login, EXCLUDED))
for row in cur.fetchall():
    print(f"  open={row[0]} close={row[1]} entry={row[2]} val={float(row[3]):,.2f} sym={row[4]}")

# Check if our timestamps are UTC+2 vs PBI using UTC
# Sum open vol with UTC boundaries (2h earlier)
print(f"\n=== Timezone test for ALL logins: UTC vs UTC+2 ===")
cur.execute("""
    SELECT COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.open_time >= '2025-12-31 22:00' AND t.open_time < '2026-03-31 22:00'
      AND t.entry = 0 AND t.cmd IN (0, 1) AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
""", (EXCLUDED, COUNTRIES))
utc_open = float(cur.fetchone()[0])

cur.execute("""
    SELECT COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.close_time >= '2025-12-31 22:00' AND t.close_time < '2026-03-31 22:00'
      AND t.entry = 1 AND t.close_time > '1971-01-01'
      AND t.cmd IN (0, 1) AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
""", (EXCLUDED, COUNTRIES))
utc_close = float(cur.fetchone()[0])

print(f"If timestamps are UTC+2 (shift -2h): open={utc_open:,.2f} + close={utc_close:,.2f} = {utc_open+utc_close:,.2f}")
print(f"Current (as-is UTC+2 boundaries):     our total = {sum(d['open']+d['close'] for d in ours.values()):,.2f}")
print(f"PBI total:                             {sum(d['total'] for d in pbi.values()):,.2f}")

cur.close()
conn.close()
