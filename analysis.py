from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()
months = [('2025-10','Oct 25'),('2025-11','Nov 25'),('2025-12','Dec 25'),('2026-01','Jan 26'),('2026-02','Feb 26'),('2026-03','Mar 26'),('2026-04','Apr 26')]
results = {}
AGE_Q = ("CASE WHEN DATE_PART('year',AGE(rt.day,a.birth_date::date))<25 THEN 3"
         " WHEN DATE_PART('year',AGE(rt.day,a.birth_date::date))<30 THEN 5"
         " WHEN DATE_PART('year',AGE(rt.day,a.birth_date::date))<40 THEN 6"
         " ELSE 7 END")
AGE_Q2 = AGE_Q.replace("rt.day", "t.confirmation_time::date")
for mk, ml in months:
    y, m = int(mk[:4]), int(mk[5:])
    d = mk + '-01'
    e = f"{y+1}-01-01" if m == 12 else f"{y}-{m+1:02d}-01"
    for q, qf in [('High','BETWEEN 6 AND 10'),('Low','BETWEEN 1 AND 5')]:
        sc = f"CASE WHEN COALESCE(a.classification_int,0)>0 THEN a.classification_int WHEN a.birth_date IS NOT NULL THEN {AGE_Q} ELSE NULL END"
        cur.execute(
            f"SELECT COUNT(DISTINCT rt.accountid)"
            f" FROM mv_retention_traders rt"
            f" JOIN accounts a ON a.accountid=rt.accountid"
            f" LEFT JOIN crm_users u ON u.id=rt.assigned_to"
            f" WHERE rt.accountid IS NOT NULL AND rt.accountid::text!=''"
            f" AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'"
            f" AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'"
            f" AND u.department_='Retention'"
            f" AND rt.day>='{d}' AND rt.day<'{e}'"
            f" AND ({sc}) {qf}", {})
        traders = cur.fetchone()[0] or 0
        sc2 = f"CASE WHEN COALESCE(a.classification_int,0)>0 THEN a.classification_int WHEN a.birth_date IS NOT NULL THEN {AGE_Q2} ELSE NULL END"
        base = (
            f" FROM transactions t"
            f" JOIN accounts a ON a.accountid=t.vtigeraccountid"
            f" LEFT JOIN crm_users u ON u.id=t.original_deposit_owner"
            f" WHERE t.transactionapproval='Approved'"
            f" AND (t.deleted=0 OR t.deleted IS NULL)"
            f" AND t.vtigeraccountid IS NOT NULL"
            f" AND a.is_test_account=0 AND (a.is_demo=0 OR a.is_demo IS NULL)"
            f" AND a.accountid IS NOT NULL AND a.accountid::text!=''"
            f" AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'"
            f" AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'"
            f" AND u.department_='Retention'"
            f" AND t.confirmation_time>='{d}'::date AND t.confirmation_time<'{e}'::date"
            f" AND ({sc2}) {qf}")
        cur.execute(f"SELECT COUNT(DISTINCT a.accountid) {base} AND t.transaction_type_name='Deposit'", {})
        deps = cur.fetchone()[0] or 0
        cur.execute(
            f"SELECT COALESCE(SUM(CASE"
            f" WHEN t.transaction_type_name IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount"
            f" WHEN t.transaction_type_name IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount"
            f" END),0)"
            f" {base}"
            f" AND t.transaction_type_name IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')", {})
        net_val = float(cur.fetchone()[0] or 0)
        results[(ml, q)] = (traders, deps, net_val)
    # Also get the TOTAL net (no quality split) to verify against Total Traders page
    cur.execute(
        f"SELECT COALESCE(SUM(CASE"
        f" WHEN t.transaction_type_name IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount"
        f" WHEN t.transaction_type_name IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount"
        f" END),0)"
        f" FROM transactions t"
        f" JOIN accounts a ON a.accountid=t.vtigeraccountid"
        f" LEFT JOIN crm_users u ON u.id=t.original_deposit_owner"
        f" WHERE t.transactionapproval='Approved'"
        f" AND (t.deleted=0 OR t.deleted IS NULL)"
        f" AND t.vtigeraccountid IS NOT NULL"
        f" AND a.is_test_account=0 AND (a.is_demo=0 OR a.is_demo IS NULL)"
        f" AND a.accountid IS NOT NULL AND a.accountid::text!=''"
        f" AND TRIM(COALESCE(u.agent_name,u.full_name,'')) NOT ILIKE 'test%%'"
        f" AND TRIM(COALESCE(u.full_name,'')) NOT ILIKE 'test%%'"
        f" AND u.department_='Retention'"
        f" AND t.confirmation_time>='{d}'::date AND t.confirmation_time<'{e}'::date"
        f" AND t.transaction_type_name IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')", {})
    results[(ml, 'ALL')] = float(cur.fetchone()[0] or 0)
conn.close()

def fs(n):
    return f"${n:,.0f}" if n >= 0 else f"-${abs(n):,.0f}"

print(f"{'Month':<8}|{'Quality':<6}|{'Traders':>8}|{'Deps':>6}|{'NET $':>14}|{'NET/Trader':>11}")
print('-' * 60)
for mk, ml in months:
    for q_label in ['High', 'Low']:
        t, d, n = results.get((ml, q_label), (0, 0, 0))
        npt = n / t if t else 0
        print(f"{ml:<8}|{q_label:<6}|{t:>8,}|{d:>6,}|{fs(n):>14}|{fs(npt):>11}")
    h = results.get((ml, 'High'), (0, 0, 0))
    l = results.get((ml, 'Low'), (0, 0, 0))
    tt, td, tn = h[0]+l[0], h[1]+l[1], h[2]+l[2]
    real_total = results.get((ml, 'ALL'), 0)
    tnpt = tn / tt if tt else 0
    print(f"{'':>8}|{'H+L':<6}|{tt:>8,}|{td:>6,}|{fs(tn):>14}|{fs(tnpt):>11}")
    print(f"{'':>8}|{'PAGE':<6}|{'':>8}|{'':>6}|{fs(real_total):>14}|{'':>11}")
    print('-' * 60)
