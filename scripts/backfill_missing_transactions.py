"""
One-time script to backfill missing transactions from MSSQL report.vtiger_mttransactions
into PostgreSQL transactions table.

Uses ON CONFLICT DO NOTHING — existing Antelope/MySQL rows are never overwritten.

Run:
  docker cp scripts/backfill_missing_transactions.py reporting-system-app-1:/tmp/backfill.py
  docker exec reporting-system-app-1 python3 /tmp/backfill.py
"""
import pymssql
import psycopg2
from psycopg2.extras import execute_values
import time

MSSQL = dict(server='cmtmainserver.database.windows.net', port='1433',
             user='clawreadonly', password='1231!#ASDF!a', database='cmt_main',
             tds_version='7.4', conn_properties='')
PG    = dict(host='127.0.0.1', port=5432, user='postgres',
             password='8PpVuUasBVR85T7WuAec', dbname='datawarehouse')

CHUNK_SIZE  = 5000
INSERT_COLS = [
    "mttransactionsid", "tradingaccountsid", "transaction_no", "vtigeraccountid",
    "manualorauto", "paymenttype", "transactionapproval", "amount", "creditcardlast",
    "transactiontype", "login", "platform", "cardtype", "cvv2pin", "expmon", "expyear",
    "server", "comment", "transactionid", "receipt", "bank_name", "bank_acccount_holder",
    "bank_acccount_number", "referencenum", "expiration", "actionok", "cleared_by",
    "mtorder_id", "approved_by", "ewalletid", "transaction_source", "currency_id",
    "bank_country_id", "bank_state", "bank_city", "bank_address", "swift", "need_revise",
    "original_deposit_owner", "decline_reason", "ftd", "usdamount", "chb_type",
    "chb_status", "chb_date", "cellexpert", "client_source", "iban", "deposifromip",
    "cardownername", "server_id", "ticket", "payment_method_id", "confirmation_time",
    "payment_processor", "withdrawal_reason", "deposit_ip", "expiration_card",
    "original_owner_department", "dod", "granted_by", "destination_wallet",
    "payment_method", "compliance_status", "ftd_owner", "email", "created_time",
    "modifiedtime", "psp_transaction_id", "finance_status", "session_id", "gateway_name",
    "payment_subtype", "legacy_mtt", "fee_type", "fee", "fee_included",
    "transaction_promo", "assisted_by", "deleted", "is_frd",
]

INSERT_SQL = f"""
    INSERT INTO transactions ({', '.join(INSERT_COLS)})
    VALUES %s
    ON CONFLICT (mttransactionsid) DO NOTHING
"""

# ── 1. Get existing IDs from PostgreSQL ───────────────────────────────────────
print("Loading existing transaction IDs from PostgreSQL...")
pg = psycopg2.connect(**PG)
cur = pg.cursor()
cur.execute("SELECT mttransactionsid FROM transactions")
existing_ids = set(int(r[0]) for r in cur.fetchall() if r[0] is not None)
pg.close()
print(f"  Existing rows: {len(existing_ids):,}")

# ── 2. Stream missing rows from MSSQL in chunks ───────────────────────────────
print("Streaming missing transactions from MSSQL...")
total_inserted = 0
last_id = 0
start = time.time()

while True:
    mc = pymssql.connect(**MSSQL)
    cur_m = mc.cursor(as_dict=True)
    cur_m.execute(f"""
        SELECT TOP {CHUNK_SIZE}
            mttransactionsid, tradingaccountsid, transaction_no, vtigeraccountid,
            NULL AS manualorauto, NULL AS paymenttype,
            transactionapproval, amount, creditcardlast,
            transaction_type_name AS transactiontype,
            login, NULL AS platform, cardtype,
            NULL AS cvv2pin, NULL AS expmon, NULL AS expyear, NULL AS server,
            comment, transactionid, NULL AS receipt,
            bank_name, bank_acccount_holder, bank_acccount_number,
            NULL AS referencenum, expiration, NULL AS actionok, cleared_by,
            mtorder_id, approved_by, destination_wallet AS ewalletid,
            transaction_source, currency_id, bank_country_id, bank_state,
            bank_city, bank_address, swift, need_revise, original_deposit_owner,
            decline_reason, ftd, usdamount, chb_type, chb_status, chb_date,
            cellexpert, client_source, iban, deposifromip, cardownername,
            server_id, ticket, payment_method_id, confirmation_time,
            payment_processor, withdrawal_reason, deposit_ip, expiration_card,
            original_owner_department, dod, granted_by, destination_wallet,
            payment_method, compliance_status, ftd_owner, NULL AS email,
            created_time, modifiedtime, psp_transaction_id, finance_status,
            session_id, gateway_name, payment_subtype, legacy_mtt, fee_type,
            fee, fee_included, transaction_promo, assisted_by, deleted, is_frd
        FROM report.vtiger_mttransactions
        WHERE mttransactionsid > {last_id}
          AND server_id = 2
          AND usdamount < 10000000
        ORDER BY mttransactionsid
    """)
    chunk = cur_m.fetchall()
    mc.close()

    if not chunk:
        break

    last_id = int(chunk[-1]['mttransactionsid'])

    # Filter to only missing rows
    missing = [r for r in chunk if int(r['mttransactionsid']) not in existing_ids]

    if missing:
        def _val(r, c):
            v = r.get(c)
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == '':
                return None
            if isinstance(v, bytes) and v.strip() == b'':
                return None
            if c in ('ftd', 'is_frd') and isinstance(v, bool):
                return int(v)
            return v
        rows = [tuple(_val(r, c) for c in INSERT_COLS) for r in missing]
        pg = psycopg2.connect(**PG)
        with pg.cursor() as cur_pg:
            execute_values(cur_pg, INSERT_SQL, rows)
        pg.commit()
        pg.close()
        total_inserted += len(missing)

    elapsed = int(time.time() - start)
    print(f"  Processed up to id={last_id:,} | inserted={total_inserted:,} | {elapsed}s elapsed")

print(f"\nDone. Total inserted: {total_inserted:,}")
