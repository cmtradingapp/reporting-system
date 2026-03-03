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
