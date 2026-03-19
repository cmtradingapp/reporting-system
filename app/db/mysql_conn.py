import pymysql
import pymysql.cursors
import pandas as pd
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

CHUNK_SIZE = 50_000


def _get_connection(streaming: bool = False):
    kwargs = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB if MYSQL_DB else None,
        connect_timeout=10,
        read_timeout=3600,
        ssl={"ssl": True},
    )
    if streaming:
        kwargs["cursorclass"] = pymysql.cursors.SSDictCursor
    return pymysql.connect(**kwargs)


def get_operators() -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = "SELECT o.id, o.full_name FROM v_ant_operators o"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def get_accounts(hours: int = 24) -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = f"""
            SELECT
                u.id                                                        AS accountid,
                u.is_test + 0                                               AS is_test_account,
                u.first_name,
                u.last_name,
                u.full_name,
                u.email,
                IF((u.gender = 0), 'M', IF((u.gender = 1), 'F', u.gender)) AS gender,
                u.language_iso                                              AS customer_language,
                u.country_iso,
                uair.tracking_campaign_id                                   AS campaign,
                u.original_affiliate_id                                     AS campaign_code_legacy,
                u.source                                                    AS client_source,
                u.original_affiliate,
                u.is_trading_active,
                u.is_demo,
                u.kyc_status                                                AS compliance_status,
                IF(u.acquisition_status = 0, 'Sales', 'Retention')         AS accountstatus,
                u.sales_status,
                u.retention_status,
                u.kyc_workflow_status,
                CASE
                    WHEN u.acquisition_status = 0 AND u.sales_rep != 0     THEN u.sales_rep
                    WHEN u.acquisition_status = 0 AND u.sales_rep = 0      THEN u.sales_desk_id
                    WHEN u.acquisition_status = 1 AND u.retention_rep != 0 THEN u.retention_rep
                    ELSE u.retention_desk_id
                END                                                         AS assigned_to,
                u.sales_rep                                                 AS sales_rep_id,
                u.sales_desk_id,
                u.retention_rep                                             AS retention_rep_id,
                u.retention_desk_id,
                u.first_sales_desk_id,
                u.first_retention_rep_id,
                u.kyc_rep                                                   AS compliance_agent,
                u.last_agent_assignment_time,
                u.last_trade_opened_time,
                u.has_notes,
                u.last_action_time,
                u.source,
                u.has_frd,
                u.frd_time,
                u.last_trade_closed_time                                    AS last_trade_date,
                u.ftd_time                                                  AS first_deposit_date,
                u.deposits_count                                            AS countdeposits,
                u.ldt_time                                                  AS last_deposit_date,
                uair.last_communication_time                                AS last_interaction_date,
                aud.balance,
                aud.net_deposit_usd                                         AS net_deposit,
                uair.first_trade_opened_time                                AS first_trade_date,
                aud.ftd_amount,
                u.has_ftd                                                   AS funded,
                u.last_logon_time                                           AS login_date,
                aud.total_deposit_amount                                    AS total_deposit,
                aud.total_withdrawal_amount                                 AS total_withdrawal,
                u.creation_time                                             AS createdtime,
                u.last_update_time                                          AS modifiedtime,
                u.fns_status                                                AS questionnaire_completed,
                u.user_type                                                 AS client_category,
                uair.qualification_time                                     AS client_qualification_date,
                u.client_potential                                          AS segmentation,
                u.google_uid,
                IF(u.date_of_birth < '1900-01-01', NULL, u.date_of_birth)  AS birth_date,
                IFNULL(uair.cf_customer_id, u.id)                          AS customer_id,
                UPPER(crmdb.app.display_name)                              AS regulation,
                uair.sales_client_potential
            FROM crmdb.users u
            LEFT JOIN crmdb.user_additional_info_rel uair ON (u.id = uair.user_id)
            LEFT JOIN crmdb.aggregated_user_data aud ON (u.id = aud.user_id AND 0 <> aud.latest)
            LEFT JOIN crmdb.app ON u.registration_app = crmdb.app.id
            WHERE (uair.last_communication_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
               OR u.last_update_time           >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR))
              AND u.id IS NOT NULL
              AND u.id != ''
              AND u.is_test = 0
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def get_accounts_full():
    """Yields DataFrames in chunks to avoid loading 3M+ rows into memory at once."""
    conn = _get_connection(streaming=True)
    try:
        query = """
            SELECT
                u.id                                                        AS accountid,
                u.is_test + 0                                               AS is_test_account,
                u.first_name,
                u.last_name,
                u.full_name,
                u.email,
                IF((u.gender = 0), 'M', IF((u.gender = 1), 'F', u.gender)) AS gender,
                u.language_iso                                              AS customer_language,
                u.country_iso,
                uair.tracking_campaign_id                                   AS campaign,
                u.original_affiliate_id                                     AS campaign_code_legacy,
                u.source                                                    AS client_source,
                u.original_affiliate,
                u.is_trading_active,
                u.is_demo,
                u.kyc_status                                                AS compliance_status,
                IF(u.acquisition_status = 0, 'Sales', 'Retention')         AS accountstatus,
                u.sales_status,
                u.retention_status,
                u.kyc_workflow_status,
                CASE
                    WHEN u.acquisition_status = 0 AND u.sales_rep != 0     THEN u.sales_rep
                    WHEN u.acquisition_status = 0 AND u.sales_rep = 0      THEN u.sales_desk_id
                    WHEN u.acquisition_status = 1 AND u.retention_rep != 0 THEN u.retention_rep
                    ELSE u.retention_desk_id
                END                                                         AS assigned_to,
                u.sales_rep                                                 AS sales_rep_id,
                u.sales_desk_id,
                u.retention_rep                                             AS retention_rep_id,
                u.retention_desk_id,
                u.first_sales_desk_id,
                u.first_retention_rep_id,
                u.kyc_rep                                                   AS compliance_agent,
                u.last_agent_assignment_time,
                u.last_trade_opened_time,
                u.has_notes,
                u.last_action_time,
                u.source,
                u.has_frd,
                u.frd_time,
                u.last_trade_closed_time                                    AS last_trade_date,
                u.ftd_time                                                  AS first_deposit_date,
                u.deposits_count                                            AS countdeposits,
                u.ldt_time                                                  AS last_deposit_date,
                uair.last_communication_time                                AS last_interaction_date,
                aud.balance,
                aud.net_deposit_usd                                         AS net_deposit,
                uair.first_trade_opened_time                                AS first_trade_date,
                aud.ftd_amount,
                u.has_ftd                                                   AS funded,
                u.last_logon_time                                           AS login_date,
                aud.total_deposit_amount                                    AS total_deposit,
                aud.total_withdrawal_amount                                 AS total_withdrawal,
                u.creation_time                                             AS createdtime,
                u.last_update_time                                          AS modifiedtime,
                u.fns_status                                                AS questionnaire_completed,
                u.user_type                                                 AS client_category,
                uair.qualification_time                                     AS client_qualification_date,
                u.client_potential                                          AS segmentation,
                u.google_uid,
                IF(u.date_of_birth < '1900-01-01', NULL, u.date_of_birth)  AS birth_date,
                IFNULL(uair.cf_customer_id, u.id)                          AS customer_id,
                UPPER(crmdb.app.display_name)                              AS regulation,
                uair.sales_client_potential
            FROM crmdb.users u
            LEFT JOIN crmdb.user_additional_info_rel uair ON (u.id = uair.user_id)
            LEFT JOIN crmdb.aggregated_user_data aud ON (u.id = aud.user_id AND 0 <> aud.latest)
            LEFT JOIN crmdb.app ON u.registration_app = crmdb.app.id
            WHERE u.id IS NOT NULL
              AND u.id != ''
              AND u.is_test = 0
        """
        with conn.cursor() as cur:
            cur.execute(query)
            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                yield pd.DataFrame(rows)
    finally:
        conn.close()


def get_transactions(hours: int = 24) -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = f"""
            SELECT * FROM (
                SELECT
                    bb.id                                                           AS mttransactionsid,
                    bb.broker_user_id                                               AS tradingaccountsid,
                    NULL                                                            AS transaction_no,
                    bb.user_id                                                      AS vtigeraccountid,
                    bb.is_manual                                                    AS manualorauto,
                    NULL                                                            AS paymenttype,
                    IF(l1.value = 'Success', 'Approved', l1.value)                 AS transactionapproval,
                    (bb.amount / 100)                                               AS amount,
                    bb.card_number                                                  AS creditcardlast,
                    l2.value                                                        AS transactiontype,
                    bu.external_id                                                  AS login,
                    NULL                                                            AS platform,
                    bb.card_type                                                    AS cardtype,
                    NULL                                                            AS cvv2pin,
                    bb.card_expiry                                                  AS expmon,
                    bb.card_expiry                                                  AS expyear,
                    NULL                                                            AS server,
                    bb.comment                                                      AS comment,
                    bb.psp_transaction_id                                           AS transactionid,
                    NULL                                                            AS receipt,
                    bb.bank_name                                                    AS bank_name,
                    bb.bank_account_name                                            AS bank_acccount_holder,
                    bb.bank_account_number                                          AS bank_acccount_number,
                    NULL                                                            AS referencenum,
                    NULL                                                            AS expiration,
                    NULL                                                            AS actionok,
                    NULL                                                            AS cleared_by,
                    bb.broker_external_id                                           AS mtorder_id,
                    bb.decision_by_id                                               AS approved_by,
                    bb.user_withdrawal_wallet                                       AS ewalletid,
                    NULL                                                            AS transaction_source,
                    bb.currency                                                     AS currency_id,
                    bb.bank_country                                                 AS bank_country_id,
                    NULL                                                            AS bank_state,
                    NULL                                                            AS bank_city,
                    bb.bank_address                                                 AS bank_address,
                    NULL                                                            AS swift,
                    NULL                                                            AS need_revise,
                    CASE
                        WHEN bb.acquisition_status = 1 OR bb.sales_rep_id = 0 THEN bb.retention_rep_id
                        WHEN bb.acquisition_status = 0 OR bb.sales_rep_id != 0 THEN bb.sales_rep_id
                    END                                                             AS original_deposit_owner,
                    bb.decline_reason                                               AS decline_reason,
                    bb.is_ftd                                                       AS ftd,
                    (bb.normalized_amount / 100)                                    AS usdamount,
                    NULL                                                            AS chb_type,
                    NULL                                                            AS chb_status,
                    NULL                                                            AS chb_date,
                    NULL                                                            AS cellexpert,
                    NULL                                                            AS client_source,
                    NULL                                                            AS iban,
                    bb.ip                                                           AS deposifromip,
                    bb.card_holder_name                                             AS cardownername,
                    IF(bu.is_demo, 1, 2)                                            AS server_id,
                    NULL                                                            AS ticket,
                    bb.transaction_method                                           AS payment_method_id,
                    bb.decision_time                                                AS confirmation_time,
                    bb.sub_psp_name                                                 AS payment_processor,
                    bb.withdrawal_reason                                            AS withdrawal_reason,
                    bb.ip                                                           AS deposit_ip,
                    bb.card_expiry                                                  AS expiration_card,
                    bb.acquisition_status                                           AS original_owner_department,
                    NULL                                                            AS dod,
                    NULL                                                            AS granted_by,
                    bb.user_withdrawal_wallet                                       AS destination_wallet,
                    l.value                                                         AS payment_method,
                    bb.kyc_status                                                   AS compliance_status,
                    NULL                                                            AS ftd_owner,
                    bb.registered_email                                             AS email,
                    bb.creation_time                                                AS created_time,
                    bb.last_update_time                                             AS modifiedtime,
                    bb.sub_psp_transaction_id                                       AS psp_transaction_id,
                    NULL                                                            AS finance_status,
                    NULL                                                            AS session_id,
                    NULL                                                            AS gateway_name,
                    NULL                                                            AS payment_subtype,
                    NULL                                                            AS legacy_mtt,
                    NULL                                                            AS fee_type,
                    bb.fee                                                          AS fee,
                    NULL                                                            AS fee_included,
                    NULL                                                            AS transaction_promo,
                    NULL                                                            AS assisted_by,
                    NULL                                                            AS deleted,
                    bb.is_frd,
                    NULL                                                            AS transactiontypename
                FROM crmdb.broker_banking bb
                JOIN crmdb.v_ant_broker_user bu ON bb.broker_user_id = bu.id
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'TransactionMethod') l
                    ON l.`key` = bb.transaction_method
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'TransactionStatus') l1
                    ON l1.`key` = bb.status
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'BrokerBankingType') l2
                    ON l2.`key` = bb.type
            ) t
            WHERE usdamount < 10000000
              AND server_id = 2
              AND (t.modifiedtime       >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
                OR t.confirmation_time  >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR))
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_transactions_full():
    """Yields DataFrames in chunks to avoid loading all transactions into memory at once."""
    conn = _get_connection(streaming=True)
    try:
        query = """
            SELECT * FROM (
                SELECT
                    bb.id                                                           AS mttransactionsid,
                    bb.broker_user_id                                               AS tradingaccountsid,
                    NULL                                                            AS transaction_no,
                    bb.user_id                                                      AS vtigeraccountid,
                    bb.is_manual                                                    AS manualorauto,
                    NULL                                                            AS paymenttype,
                    IF(l1.value = 'Success', 'Approved', l1.value)                 AS transactionapproval,
                    (bb.amount / 100)                                               AS amount,
                    bb.card_number                                                  AS creditcardlast,
                    l2.value                                                        AS transactiontype,
                    bu.external_id                                                  AS login,
                    NULL                                                            AS platform,
                    bb.card_type                                                    AS cardtype,
                    NULL                                                            AS cvv2pin,
                    bb.card_expiry                                                  AS expmon,
                    bb.card_expiry                                                  AS expyear,
                    NULL                                                            AS server,
                    bb.comment                                                      AS comment,
                    bb.psp_transaction_id                                           AS transactionid,
                    NULL                                                            AS receipt,
                    bb.bank_name                                                    AS bank_name,
                    bb.bank_account_name                                            AS bank_acccount_holder,
                    bb.bank_account_number                                          AS bank_acccount_number,
                    NULL                                                            AS referencenum,
                    NULL                                                            AS expiration,
                    NULL                                                            AS actionok,
                    NULL                                                            AS cleared_by,
                    bb.broker_external_id                                           AS mtorder_id,
                    bb.decision_by_id                                               AS approved_by,
                    bb.user_withdrawal_wallet                                       AS ewalletid,
                    NULL                                                            AS transaction_source,
                    bb.currency                                                     AS currency_id,
                    bb.bank_country                                                 AS bank_country_id,
                    NULL                                                            AS bank_state,
                    NULL                                                            AS bank_city,
                    bb.bank_address                                                 AS bank_address,
                    NULL                                                            AS swift,
                    NULL                                                            AS need_revise,
                    CASE
                        WHEN bb.acquisition_status = 1 OR bb.sales_rep_id = 0 THEN bb.retention_rep_id
                        WHEN bb.acquisition_status = 0 OR bb.sales_rep_id != 0 THEN bb.sales_rep_id
                    END                                                             AS original_deposit_owner,
                    bb.decline_reason                                               AS decline_reason,
                    bb.is_ftd                                                       AS ftd,
                    (bb.normalized_amount / 100)                                    AS usdamount,
                    NULL                                                            AS chb_type,
                    NULL                                                            AS chb_status,
                    NULL                                                            AS chb_date,
                    NULL                                                            AS cellexpert,
                    NULL                                                            AS client_source,
                    NULL                                                            AS iban,
                    bb.ip                                                           AS deposifromip,
                    bb.card_holder_name                                             AS cardownername,
                    IF(bu.is_demo, 1, 2)                                            AS server_id,
                    NULL                                                            AS ticket,
                    bb.transaction_method                                           AS payment_method_id,
                    bb.decision_time                                                AS confirmation_time,
                    bb.sub_psp_name                                                 AS payment_processor,
                    bb.withdrawal_reason                                            AS withdrawal_reason,
                    bb.ip                                                           AS deposit_ip,
                    bb.card_expiry                                                  AS expiration_card,
                    bb.acquisition_status                                           AS original_owner_department,
                    NULL                                                            AS dod,
                    NULL                                                            AS granted_by,
                    bb.user_withdrawal_wallet                                       AS destination_wallet,
                    l.value                                                         AS payment_method,
                    bb.kyc_status                                                   AS compliance_status,
                    NULL                                                            AS ftd_owner,
                    bb.registered_email                                             AS email,
                    bb.creation_time                                                AS created_time,
                    bb.last_update_time                                             AS modifiedtime,
                    bb.sub_psp_transaction_id                                       AS psp_transaction_id,
                    NULL                                                            AS finance_status,
                    NULL                                                            AS session_id,
                    NULL                                                            AS gateway_name,
                    NULL                                                            AS payment_subtype,
                    NULL                                                            AS legacy_mtt,
                    NULL                                                            AS fee_type,
                    bb.fee                                                          AS fee,
                    NULL                                                            AS fee_included,
                    NULL                                                            AS transaction_promo,
                    NULL                                                            AS assisted_by,
                    NULL                                                            AS deleted,
                    bb.is_frd,
                    NULL                                                            AS transactiontypename
                FROM crmdb.broker_banking bb
                JOIN crmdb.v_ant_broker_user bu ON bb.broker_user_id = bu.id
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'TransactionMethod') l
                    ON l.`key` = bb.transaction_method
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'TransactionStatus') l1
                    ON l1.`key` = bb.status
                LEFT JOIN (SELECT * FROM crmdb.autolut WHERE type = 'BrokerBankingType') l2
                    ON l2.`key` = bb.type
            ) t
            WHERE usdamount < 10000000
              AND server_id = 2
        """
        with conn.cursor() as cur:
            cur.execute(query)
            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                yield pd.DataFrame(rows)
    finally:
        conn.close()


def get_crm_users(hours: int = 24) -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = f"""
            SELECT
                o.id,
                MAX(email)                                                                        AS email,
                MAX(o.full_name)                                                                  AS full_name,
                MAX(IF(o.is_active, 'Active', 'Inactive'))                                        AS status,
                MAX(SUBSTRING_INDEX(REPLACE(o.full_name, '.', ' '), ' ', 1))                     AS first_name,
                MAX(SUBSTRING_INDEX(REPLACE(o.full_name, '.', ' '), ' ', -1))                    AS last_name,
                MAX(o.role_id)                                                                    AS role_id,
                MAX(od.desk_id)                                                                   AS desk_id,
                MAX(o.language_iso)                                                               AS language,
                MAX(o.last_logon_time)                                                            AS last_logon_time,
                MAX(o.last_update_time)                                                           AS last_update_time,
                MAX(d.name)                                                                       AS desk_name,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM', '',
                    TRIM(SUBSTRING_INDEX(d.name, '-', 1))))                                       AS team,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM', '',
                    TRIM(SUBSTRING_INDEX(d.name, '-', -1))))                                      AS department,
                MAX(SUBSTRING_INDEX(SUBSTRING_INDEX(d.name, '-', 2), '-', -1))                   AS desk,
                MAX(d.type)                                                                       AS type,
                MAX(d.office_id)                                                                  AS office_id,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM',
                    'General', ofc.name))                                                         AS office,
                MAX(IF(opr.display_name LIKE '%Agent%' OR opr.display_name = 'BDM',
                    'Agent', opr.display_name))                                                   AS position,
                MAX(CASE
                    WHEN o.id = 61 OR ofc.name IS NULL OR ofc.name = '' THEN 'General'
                    WHEN d.name = 'Laila Desk' THEN 'Laila'
                    ELSE CASE ofc.name
                        WHEN 'IN'          THEN 'India'
                        WHEN 'UY'          THEN 'Uruguay'
                        WHEN 'SA'          THEN 'South Africa'
                        WHEN 'LAG-NG'      THEN 'LAG Nigeria'
                        WHEN 'IL'          THEN 'Israel'
                        WHEN 'GMT'         THEN 'GMT'
                        WHEN 'General'     THEN 'General'
                        WHEN 'Global'      THEN 'General'
                        WHEN 'DU'          THEN 'Dubai'
                        WHEN 'CO'          THEN 'Columbia'
                        WHEN 'CY'          THEN 'Cyprus'
                        WHEN 'BG'          THEN 'Bulgaria'
                        WHEN 'ABJ-NG'      THEN 'ABJ Nigeria'
                        WHEN 'WL-BG'       THEN 'WL Bulgaria'
                        WHEN 'WL-PK'       THEN 'WL Pakistan'
                        WHEN 'WL-SL'       THEN 'WL Sri Lanka'
                        WHEN 'WL-IL'       THEN 'WL IL'
                        WHEN 'VN'          THEN 'Vietnam'
                        WHEN 'WL-Belgrad'  THEN 'WL Belgrad'
                        WHEN 'WL-SNS-UAW'  THEN 'WL UAE'
                        WHEN 'WL-ABUKING'  THEN 'WL ABUKING'
                        ELSE ofc.name
                    END
                END)                                                                              AS office_name,
                MAX(o.full_name)                                                                  AS agent_name,
                MAX(IF(LOWER(TRIM(SUBSTRING_INDEX(d.name, '-', 1))) LIKE '%conversion%',
                    'Sales', 'Retention'))                                                        AS department_
            FROM v_ant_operators o
            LEFT JOIN operator_desk_rel od ON o.id = od.operator_id
            LEFT JOIN desk d ON od.desk_id = d.id
            LEFT JOIN office ofc ON d.office_id = ofc.id
            LEFT JOIN operator_role opr ON o.role_id = opr.id
            WHERE (opr.display_name IS NULL
               OR (opr.display_name != 'Affiliate'
                   AND opr.display_name NOT LIKE '%Admin'
                   AND opr.display_name != 'Dialer'))
              AND o.last_update_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
            GROUP BY o.id

            UNION

            SELECT
                d.id,
                NULL                                                                              AS email,
                d.name                                                                            AS full_name,
                'Active'                                                                          AS status,
                d.name                                                                            AS first_name,
                NULL                                                                              AS last_name,
                NULL                                                                              AS role_id,
                d.id                                                                              AS desk_id,
                NULL                                                                              AS language,
                CURRENT_TIMESTAMP()                                                               AS last_logon_time,
                d.last_update_time,
                d.name                                                                            AS desk_name,
                TRIM(SUBSTRING_INDEX(d.name, '-', -1))                                           AS team,
                TRIM(SUBSTRING_INDEX(d.name, '-', 1))                                            AS department,
                SUBSTRING_INDEX(SUBSTRING_INDEX(d.name, '-', 2), '-', -1)                       AS desk,
                d.type,
                d.office_id,
                ofc.name                                                                          AS office,
                'Agent'                                                                           AS position,
                CASE
                    WHEN d.name = 'Laila Desk'              THEN 'Laila'
                    WHEN ofc.name IS NULL OR ofc.name = ''  THEN 'General'
                    ELSE CASE ofc.name
                        WHEN 'IN'          THEN 'India'
                        WHEN 'UY'          THEN 'Uruguay'
                        WHEN 'SA'          THEN 'South Africa'
                        WHEN 'LAG-NG'      THEN 'LAG Nigeria'
                        WHEN 'IL'          THEN 'Israel'
                        WHEN 'GMT'         THEN 'GMT'
                        WHEN 'General'     THEN 'General'
                        WHEN 'Global'      THEN 'General'
                        WHEN 'DU'          THEN 'Dubai'
                        WHEN 'CO'          THEN 'Columbia'
                        WHEN 'CY'          THEN 'Cyprus'
                        WHEN 'BG'          THEN 'Bulgaria'
                        WHEN 'ABJ-NG'      THEN 'ABJ Nigeria'
                        WHEN 'WL-BG'       THEN 'WL Bulgaria'
                        WHEN 'WL-PK'       THEN 'WL Pakistan'
                        WHEN 'WL-SL'       THEN 'WL Sri Lanka'
                        WHEN 'WL-IL'       THEN 'WL IL'
                        WHEN 'VN'          THEN 'Vietnam'
                        WHEN 'WL-Belgrad'  THEN 'WL Belgrad'
                        WHEN 'WL-SNS-UAW'  THEN 'WL UAE'
                        WHEN 'WL-ABUKING'  THEN 'WL ABUKING'
                        ELSE ofc.name
                    END
                END                                                                               AS office_name,
                d.name                                                                            AS agent_name,
                IF(LOWER(TRIM(SUBSTRING_INDEX(d.name, '-', 1))) LIKE '%conversion%',
                    'Sales', 'Retention')                                                         AS department_
            FROM desk d
            JOIN office ofc ON d.office_id = ofc.id
            WHERE d.last_update_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_crm_users_full() -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = """
            SELECT
                o.id,
                MAX(email)                                                                        AS email,
                MAX(o.full_name)                                                                  AS full_name,
                MAX(IF(o.is_active, 'Active', 'Inactive'))                                        AS status,
                MAX(SUBSTRING_INDEX(REPLACE(o.full_name, '.', ' '), ' ', 1))                     AS first_name,
                MAX(SUBSTRING_INDEX(REPLACE(o.full_name, '.', ' '), ' ', -1))                    AS last_name,
                MAX(o.role_id)                                                                    AS role_id,
                MAX(od.desk_id)                                                                   AS desk_id,
                MAX(o.language_iso)                                                               AS language,
                MAX(o.last_logon_time)                                                            AS last_logon_time,
                MAX(o.last_update_time)                                                           AS last_update_time,
                MAX(d.name)                                                                       AS desk_name,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM', '',
                    TRIM(SUBSTRING_INDEX(d.name, '-', 1))))                                       AS team,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM', '',
                    TRIM(SUBSTRING_INDEX(d.name, '-', -1))))                                      AS department,
                MAX(SUBSTRING_INDEX(SUBSTRING_INDEX(d.name, '-', 2), '-', -1))                   AS desk,
                MAX(d.type)                                                                       AS type,
                MAX(d.office_id)                                                                  AS office_id,
                MAX(IF(opr.display_name NOT LIKE '%Agent%' AND opr.display_name != 'BDM',
                    'General', ofc.name))                                                         AS office,
                MAX(IF(opr.display_name LIKE '%Agent%' OR opr.display_name = 'BDM',
                    'Agent', opr.display_name))                                                   AS position,
                MAX(CASE
                    WHEN o.id = 61 OR ofc.name IS NULL OR ofc.name = '' THEN 'General'
                    WHEN d.name = 'Laila Desk' THEN 'Laila'
                    ELSE CASE ofc.name
                        WHEN 'IN'          THEN 'India'
                        WHEN 'UY'          THEN 'Uruguay'
                        WHEN 'SA'          THEN 'South Africa'
                        WHEN 'LAG-NG'      THEN 'LAG Nigeria'
                        WHEN 'IL'          THEN 'Israel'
                        WHEN 'GMT'         THEN 'GMT'
                        WHEN 'General'     THEN 'General'
                        WHEN 'Global'      THEN 'General'
                        WHEN 'DU'          THEN 'Dubai'
                        WHEN 'CO'          THEN 'Columbia'
                        WHEN 'CY'          THEN 'Cyprus'
                        WHEN 'BG'          THEN 'Bulgaria'
                        WHEN 'ABJ-NG'      THEN 'ABJ Nigeria'
                        WHEN 'WL-BG'       THEN 'WL Bulgaria'
                        WHEN 'WL-PK'       THEN 'WL Pakistan'
                        WHEN 'WL-SL'       THEN 'WL Sri Lanka'
                        WHEN 'WL-IL'       THEN 'WL IL'
                        WHEN 'VN'          THEN 'Vietnam'
                        WHEN 'WL-Belgrad'  THEN 'WL Belgrad'
                        WHEN 'WL-SNS-UAW'  THEN 'WL UAE'
                        WHEN 'WL-ABUKING'  THEN 'WL ABUKING'
                        ELSE ofc.name
                    END
                END)                                                                              AS office_name,
                MAX(o.full_name)                                                                  AS agent_name,
                MAX(IF(LOWER(TRIM(SUBSTRING_INDEX(d.name, '-', 1))) LIKE '%conversion%',
                    'Sales', 'Retention'))                                                        AS department_
            FROM operators o
            LEFT JOIN operator_desk_rel od ON o.id = od.operator_id
            LEFT JOIN desk d ON od.desk_id = d.id
            LEFT JOIN office ofc ON d.office_id = ofc.id
            LEFT JOIN operator_role opr ON o.role_id = opr.id
            WHERE (opr.display_name IS NULL
               OR (opr.display_name != 'Affiliate'
                   AND opr.display_name NOT LIKE '%Admin'
                   AND opr.display_name != 'Dialer'))
            GROUP BY o.id

            UNION

            SELECT
                d.id,
                NULL                                                                              AS email,
                d.name                                                                            AS full_name,
                'Active'                                                                          AS status,
                d.name                                                                            AS first_name,
                NULL                                                                              AS last_name,
                NULL                                                                              AS role_id,
                d.id                                                                              AS desk_id,
                NULL                                                                              AS language,
                CURRENT_TIMESTAMP()                                                               AS last_logon_time,
                d.last_update_time,
                d.name                                                                            AS desk_name,
                TRIM(SUBSTRING_INDEX(d.name, '-', -1))                                           AS team,
                TRIM(SUBSTRING_INDEX(d.name, '-', 1))                                            AS department,
                SUBSTRING_INDEX(SUBSTRING_INDEX(d.name, '-', 2), '-', -1)                       AS desk,
                d.type,
                d.office_id,
                ofc.name                                                                          AS office,
                'Agent'                                                                           AS position,
                CASE
                    WHEN d.name = 'Laila Desk'              THEN 'Laila'
                    WHEN ofc.name IS NULL OR ofc.name = ''  THEN 'General'
                    ELSE CASE ofc.name
                        WHEN 'IN'          THEN 'India'
                        WHEN 'UY'          THEN 'Uruguay'
                        WHEN 'SA'          THEN 'South Africa'
                        WHEN 'LAG-NG'      THEN 'LAG Nigeria'
                        WHEN 'IL'          THEN 'Israel'
                        WHEN 'GMT'         THEN 'GMT'
                        WHEN 'General'     THEN 'General'
                        WHEN 'Global'      THEN 'General'
                        WHEN 'DU'          THEN 'Dubai'
                        WHEN 'CO'          THEN 'Columbia'
                        WHEN 'CY'          THEN 'Cyprus'
                        WHEN 'BG'          THEN 'Bulgaria'
                        WHEN 'ABJ-NG'      THEN 'ABJ Nigeria'
                        WHEN 'WL-BG'       THEN 'WL Bulgaria'
                        WHEN 'WL-PK'       THEN 'WL Pakistan'
                        WHEN 'WL-SL'       THEN 'WL Sri Lanka'
                        WHEN 'WL-IL'       THEN 'WL IL'
                        WHEN 'VN'          THEN 'Vietnam'
                        WHEN 'WL-Belgrad'  THEN 'WL Belgrad'
                        WHEN 'WL-SNS-UAW'  THEN 'WL UAE'
                        WHEN 'WL-ABUKING'  THEN 'WL ABUKING'
                        ELSE ofc.name
                    END
                END                                                                               AS office_name,
                d.name                                                                            AS agent_name,
                IF(LOWER(TRIM(SUBSTRING_INDEX(d.name, '-', 1))) LIKE '%conversion%',
                    'Sales', 'Retention')                                                         AS department_
            FROM desk d
            JOIN office ofc ON d.office_id = ofc.id
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


_TRADING_ACCOUNTS_SELECT = """
    SELECT
        bu.id                                       AS trading_account_id,
        CONCAT(bu.first_name, ' ', bu.last_name)   AS trading_account_name,
        bu.user_id                                  AS vtigeraccountid,
        bu.trade_group_string                       AS trade_group,
        bu.last_update_time                         AS last_update,
        (bu.equity / 100)                           AS equity,
        bu.open_pnl                                 AS open_pnl,
        bu.close_pnl                                AS total_pnl,
        bu.total_commission                         AS commission,
        (0 = bu.is_deleted)                         AS enable,
        (0 = bu.is_trading_active)                  AS enable_read_only,
        bu.external_id                              AS login,
        bu.currency                                 AS currency,
        IF(bu.is_demo, 1, 2)                        AS serverid,
        CASE
            WHEN u.acquisition_status = 0 AND u.sales_rep != 0     THEN u.sales_rep
            WHEN u.acquisition_status = 0 AND u.sales_rep = 0      THEN u.sales_desk_id
            WHEN u.acquisition_status = 1 AND u.retention_rep != 0 THEN u.retention_rep
            ELSE u.retention_desk_id
        END                                         AS assigned_to,
        (bu.balance / 100)                          AS balance,
        NULL                                        AS credit,
        bu.total_swap                               AS swaps,
        NULL                                        AS total_taxes,
        bu.leverage                                 AS leverage,
        bu.margin                                   AS margin,
        NULL                                        AS margin_level,
        bu.free_margin                              AS margin_free,
        bu.creation_time                            AS created_time,
        NULL                                        AS trading_server_created_timestamp,
        NULL                                        AS platform,
        bu.is_deleted                               AS deleted
    FROM v_ant_broker_user bu
    LEFT JOIN v_ant_users u ON u.id = bu.user_id
    WHERE bu.trade_group_string NOT LIKE '%test%'
      AND bu.is_demo != 1
      AND bu.is_deleted = 0
"""


def get_trading_accounts(hours: int = 24) -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = _TRADING_ACCOUNTS_SELECT + f"""
          AND bu.last_update_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_trading_accounts_full():
    """Yields DataFrames in chunks — safe for large broker_user tables."""
    conn = _get_connection(streaming=True)
    try:
        with conn.cursor() as cur:
            cur.execute(_TRADING_ACCOUNTS_SELECT)
            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                yield pd.DataFrame(rows)
    finally:
        conn.close()


def get_users() -> pd.DataFrame:
    conn = _get_connection()
    try:
        query = """
            SELECT
                o.id,
                max(email) as email,
                max(o.full_name) as full_name,
                max(if(o.is_active, 'Active', 'Inactive')) as status,
                max(substring_index(REPLACE(o.full_name, '.', ' '), ' ', 1)) as first_name,
                max(substring_index(REPLACE(o.full_name, '.', ' '), ' ', -1)) as last_name,
                max(o.role_id) as role_id,
                max(od.desk_id) as desk_id,
                max(o.language_iso) as language,
                max(o.last_logon_time) as last_logon_time,
                max(o.last_update_time) as last_update_time,
                max(d.name) as desk_name,
                max(if(opr.display_name not like '%Agent%' and opr.display_name != 'BDM', '', trim(SUBSTRING_INDEX(d.name, '-', 1)))) as team,
                max(if(opr.display_name not like '%Agent%' and opr.display_name != 'BDM', '', trim(SUBSTRING_INDEX(d.name, '-', -1)))) as department,
                max(substring_index(substring_index(d.name, '-', 2), '-', -1)) as desk,
                max(d.type) as type,
                max(d.office_id) as office_id,
                max(if(opr.display_name not like '%Agent%' and opr.display_name != 'BDM', 'General', ofc.name)) as office,
                max(if(opr.display_name like '%Agent%' or opr.display_name = 'BDM', 'Agent', opr.display_name)) as position
            FROM v_ant_operators o
            LEFT JOIN operator_desk_rel od ON o.id = od.operator_id
            LEFT JOIN desk d ON od.desk_id = d.id
            LEFT JOIN office ofc ON d.office_id = ofc.id
            LEFT JOIN operator_role opr ON o.role_id = opr.id
            WHERE opr.display_name != 'Affiliate'
              AND opr.display_name NOT LIKE '%Admin'
              AND opr.display_name != 'Dialer'
            GROUP BY o.id

            UNION

            SELECT
                d.id,
                null as email,
                d.name as full_name,
                'Active' as status,
                d.name as first_name,
                null as last_name,
                null as role_id,
                d.id as desk_id,
                null as language,
                current_timestamp() as last_logon_time,
                d.last_update_time,
                d.name as desk_name,
                trim(SUBSTRING_INDEX(d.name, '-', -1)) as team,
                trim(SUBSTRING_INDEX(d.name, '-', 1)) as department,
                substring_index(substring_index(d.name, '-', 2), '-', -1) as desk,
                d.type,
                d.office_id,
                ofc.name as office,
                'Agent' as position
            FROM desk d
            JOIN office ofc ON d.office_id = ofc.id
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()
