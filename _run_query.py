import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/elise.i/reporting-system/.env")

import pymysql

host     = os.getenv("MYSQL_HOST", "cmtrading-replica-db.cllx9icdmhvp.eu-west-1.rds.amazonaws.com")
port     = int(os.getenv("MYSQL_PORT", 3306))
user     = os.getenv("MYSQL_USER", "db_readonly")
password = os.getenv("MYSQL_PASSWORD", "")
database = os.getenv("MYSQL_DB", "crmdb")

conn = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database,
    connect_timeout=15,
    ssl={"ssl": True},
    cursorclass=pymysql.cursors.DictCursor,
)

sql = """
SELECT op.id as agent_id,
op.full_name as agent_name,
op.email as agent_email,
o.name as office_name,
e.voip_extension,
case when ro.display_name = 'Sales Agent' then 'Conversion' else ro.display_name end as position
FROM crmdb.operator_voip_extension e
LEFT JOIN crmdb.operators op on e.operator_id= op.id
LEFT JOIN crmdb.operator_desk_rel r on r.operator_id= op.id
LEFT JOIN crmdb.desk d on d.id = r.desk_id
LEFT JOIN crmdb.office o on o.id = d.office_id
LEFT JOIN crmdb.operator_role ro on ro.id = op.role_id
WHERE e.integration_id = 389 and op.role_id in (1076,1077,1080,1081) and o.name is not null
group by op.full_name
"""

try:
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
finally:
    conn.close()

TARGET_IDS = {6743, 6744}
found = [r for r in rows if r.get('agent_id') in TARGET_IDS]
print(f"\n--- Searching for IDs {TARGET_IDS} ---")
if found:
    for r in found:
        print(f"FOUND: id={r['agent_id']} | {r['agent_name']} | {r['agent_email']} | {r['office_name']} | ext={r['voip_extension']} | {r['position']}")
else:
    print("NOT FOUND — IDs 6743 and 6744 are not in the results.")

if not rows:
    print("Query returned 0 rows.")
else:
    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            widths[c] = max(widths[c], len(str(row[c]) if row[c] is not None else "NULL"))

    sep = "+-" + "-+-".join("-" * widths[c] for c in cols) + "-+"
    header = "| " + " | ".join(c.ljust(widths[c]) for c in cols) + " |"

    print(sep)
    print(header)
    print(sep)
    for row in rows:
        line = "| " + " | ".join((str(row[c]) if row[c] is not None else "NULL").ljust(widths[c]) for c in cols) + " |"
        print(line)
    print(sep)
    print(f"\nTotal rows: {len(rows)}")
