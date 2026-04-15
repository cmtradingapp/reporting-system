"""
Temporary script: run on the server to export per-login trading volumes for Q1 2026.
Usage: python3 compare_s4_volume.py > our_volumes.csv

Then download our_volumes.csv and compare with PBI export locally.
"""
import psycopg2, csv, sys

EXCLUDED = (
    'Cashback','CFDRollover','CommEUR','CommUSD','CommGBP','CommJPY',
    'CorrectiEUR','CorrectiGBP','CorrectiJPY','Correction',
    'CredExp','CredExpEUR','CredExpGBP','CredExpJPY',
    'Dividend','DividendEUR','DividendGBP','DividendJPY',
    'Dormant','EarnedCr','EarnedCrEUR','FEE','INACT-FEE',
    'Inactivity','Rollover','SPREAD',
    'ZeroingEUR','ZeroingGBP','ZeroingJPY','ZeroingKES',
    'ZeroingNGN','ZeroingUSD','ZeroingZAR',
)
COUNTRIES = ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')

conn = psycopg2.connect(
    host="127.0.0.1", port=5432, dbname="reporting_db",
    user="reporting_user", password="Rep0rt!ng2025#Secure"
)
cur = conn.cursor()

# Open volume per login
cur.execute("""
    SELECT t.login, a.country_iso, COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.open_time >= '2026-01-01' AND t.open_time < '2026-04-01'
      AND t.entry = 0
      AND t.cmd IN (0, 1)
      AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
    GROUP BY t.login, a.country_iso
""", (EXCLUDED, COUNTRIES))

data = {}
for login, country, vol in cur.fetchall():
    data[str(login)] = {'country': country, 'open': float(vol), 'close': 0.0}

# Close volume per login
cur.execute("""
    SELECT t.login, a.country_iso, COALESCE(SUM(t.notional_value), 0)
    FROM dealio_trades_mt5 t
    JOIN trading_accounts ta ON ta.login::bigint = t.login
    JOIN accounts a ON a.accountid = ta.vtigeraccountid
    WHERE t.close_time >= '2026-01-01' AND t.close_time < '2026-04-01'
      AND t.entry = 1
      AND t.close_time > '1971-01-01'
      AND t.cmd IN (0, 1)
      AND t.symbol NOT IN %s
      AND a.funded = 1 AND a.is_test_account = 0
      AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
      AND a.country_iso IN %s
    GROUP BY t.login, a.country_iso
""", (EXCLUDED, COUNTRIES))

for login, country, vol in cur.fetchall():
    login = str(login)
    if login in data:
        data[login]['close'] = float(vol)
    else:
        data[login] = {'country': country, 'open': 0.0, 'close': float(vol)}

cur.close()
conn.close()

w = csv.writer(sys.stdout)
w.writerow(['login', 'country_iso', 'open_vol', 'close_vol', 'total_vol'])
for login in sorted(data, key=lambda x: int(x)):
    d = data[login]
    w.writerow([login, d['country'], f"{d['open']:.2f}", f"{d['close']:.2f}", f"{d['open']+d['close']:.2f}"])

print(f"# Total logins: {len(data)}", file=sys.stderr)
print(f"# Grand total: {sum(d['open']+d['close'] for d in data.values()):,.2f}", file=sys.stderr)
