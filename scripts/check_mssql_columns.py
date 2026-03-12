import pymssql
mc = pymssql.connect(server='cmtmainserver.database.windows.net', port='1433',
    user='clawreadonly', password='1231!#ASDF!a', database='cmt_main',
    tds_version='7.4', conn_properties='')
cur = mc.cursor()
cur.execute("SELECT TOP 1 * FROM report.vtiger_mttransactions")
for i, d in enumerate(cur.description):
    print(f"{i:3}. {d[0]}")
mc.close()
