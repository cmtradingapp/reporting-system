from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()

# 1. Traders per month per quality (single query)
print("Querying traders...")
cur.execute("""
SELECT date_trunc('month', rt.day)::date AS mth,
    CASE
        WHEN COALESCE(a.classification_int,0) BETWEEN 6 AND 10 THEN 'High'
        WHEN COALESCE(a.classification_int,0) BETWEEN 1 AND 5 THEN 'Low'
        WHEN a.birth_date IS NOT NULL AND DATE_PART('year',AGE(rt.day,a.birth_date::date)) >= 30 THEN 'High'
        WHEN a.birth_date IS NOT NULL THEN 'Low'
        ELSE 'N/A'
    END AS quality,
    COUNT(DISTINCT rt.accountid) AS traders
FROM mv_retention_traders rt
JOIN accounts a ON a.accountid=rt.accountid
LEFT JOIN crm_users u ON u.id=rt.assigned_to
WHERE rt.accountid IS NOT NULL AND rt.accountid::text!=''
    AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'
    AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'
    AND u.department_='Retention'
    AND rt.day >= '2025-10-01' AND rt.day < '2026-05-01'
GROUP BY 1, 2
""")
traders = {}
for r in cur.fetchall():
    traders[(r[0].strftime('%Y-%m'), r[1])] = r[2]

# 2. Depositors + NET per month per quality (single query)
print("Querying deposits/net...")
cur.execute("""
SELECT date_trunc('month', t.confirmation_time)::date AS mth,
    CASE
        WHEN COALESCE(a.classification_int,0) BETWEEN 6 AND 10 THEN 'High'
        WHEN COALESCE(a.classification_int,0) BETWEEN 1 AND 5 THEN 'Low'
        WHEN a.birth_date IS NOT NULL AND DATE_PART('year',AGE(t.confirmation_time::date,a.birth_date::date)) >= 30 THEN 'High'
        WHEN a.birth_date IS NOT NULL THEN 'Low'
        ELSE 'N/A'
    END AS quality,
    COUNT(DISTINCT CASE WHEN t.transaction_type_name='Deposit' THEN a.accountid END) AS depositors,
    COALESCE(SUM(CASE
        WHEN t.transaction_type_name IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount
        WHEN t.transaction_type_name IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
    END),0) AS net_usd
FROM transactions t
JOIN accounts a ON a.accountid=t.vtigeraccountid
LEFT JOIN crm_users u ON u.id=t.original_deposit_owner
WHERE t.transactionapproval='Approved'
    AND (t.deleted=0 OR t.deleted IS NULL)
    AND t.transaction_type_name IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
    AND t.vtigeraccountid IS NOT NULL
    AND a.is_test_account=0 AND (a.is_demo=0 OR a.is_demo IS NULL)
    AND a.accountid IS NOT NULL AND a.accountid::text!=''
    AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'
    AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'
    AND u.department_='Retention'
    AND t.confirmation_time >= '2025-10-01'::date AND t.confirmation_time < '2026-05-01'::date
GROUP BY 1, 2
""")
deps = {}
nets = {}
for r in cur.fetchall():
    k = (r[0].strftime('%Y-%m'), r[1])
    deps[k] = r[2]
    nets[k] = float(r[3])

conn.close()

def fs(n):
    return f"${n:,.0f}" if n >= 0 else f"-${abs(n):,.0f}"

months = [('2025-10','Oct 25'),('2025-11','Nov 25'),('2025-12','Dec 25'),('2026-01','Jan 26'),('2026-02','Feb 26'),('2026-03','Mar 26'),('2026-04','Apr 26')]

print(f"\n{'Month':<8}|{'Quality':<6}|{'Traders':>8}|{'Deps':>6}|{'NET $':>14}|{'NET/Trader':>11}")
print('-' * 60)
for mk, ml in months:
    tot_t, tot_d, tot_n = 0, 0, 0
    for q in ['High', 'Low']:
        t = traders.get((mk, q), 0)
        d = deps.get((mk, q), 0)
        n = nets.get((mk, q), 0)
        tot_t += t; tot_d += d; tot_n += n
        npt = n / t if t else 0
        print(f"{ml:<8}|{q:<6}|{t:>8,}|{d:>6,}|{fs(n):>14}|{fs(npt):>11}")
    tnpt = tot_n / tot_t if tot_t else 0
    print(f"{'':>8}|{'TOTAL':<6}|{tot_t:>8,}|{tot_d:>6,}|{fs(tot_n):>14}|{fs(tnpt):>11}")
    print('-' * 60)
