import pymssql
mc = pymssql.connect(server='cmtmainserver.database.windows.net', port='1433',
    user='clawreadonly', password='1231!#ASDF!a', database='cmt_main',
    tds_version='7.4', conn_properties='')
cur = mc.cursor()
cur.execute("""
    SELECT transaction_type_name, COUNT(*) AS cnt,
           SUM(CASE WHEN transactionapproval='Approved' THEN 1 ELSE 0 END) AS approved_cnt,
           SUM(CASE WHEN transactionapproval='Approved' THEN usdamount ELSE 0 END) AS approved_usd
    FROM report.vtiger_mttransactions
    WHERE transactiontype IN ('Deposit', 'Withdraw', 'Credit in', 'Credit out')
    GROUP BY transaction_type_name
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(r)
mc.close()
