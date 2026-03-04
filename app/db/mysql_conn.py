import pymysql
import pandas as pd
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


def _get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB if MYSQL_DB else None,
        connect_timeout=10,
        ssl={"ssl": True},
    )


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
            WHERE uair.last_communication_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
               OR u.last_update_time           >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -{hours} HOUR)
        """
        df = pd.read_sql(query, conn)
        return df
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
