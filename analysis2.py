from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()

AGE_CASE = """CASE
    WHEN COALESCE(a.classification_int,0) BETWEEN 6 AND 10 THEN 'High'
    WHEN COALESCE(a.classification_int,0) BETWEEN 1 AND 5 THEN 'Low'
    WHEN a.birth_date IS NOT NULL AND DATE_PART('year',AGE(rt.day,a.birth_date::date)) >= 30 THEN 'High'
    WHEN a.birth_date IS NOT NULL THEN 'Low'
    ELSE 'N/A'
END"""

# Distinct traders across entire 7-month period, split by quality
cur.execute(f"""
SELECT {AGE_CASE} AS quality, COUNT(DISTINCT rt.accountid) AS traders
FROM mv_retention_traders rt
JOIN accounts a ON a.accountid=rt.accountid
LEFT JOIN crm_users u ON u.id=rt.assigned_to
WHERE rt.accountid IS NOT NULL AND rt.accountid::text!=''
    AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'
    AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'
    AND u.department_='Retention'
    AND rt.day >= '2025-10-01' AND rt.day < '2026-05-01'
GROUP BY 1
""")
print("=== DISTINCT TRADERS (Oct 25 - Apr 26) ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

AGE_CASE2 = AGE_CASE.replace("rt.day", "t.confirmation_time::date")

# Distinct depositors across entire period, split by quality
cur.execute(f"""
SELECT {AGE_CASE2} AS quality,
    COUNT(DISTINCT CASE WHEN t.transaction_type_name='Deposit' THEN a.accountid END) AS depositors
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
GROUP BY 1
""")
print("\n=== DISTINCT DEPOSITORS (Oct 25 - Apr 26) ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

conn.close()
