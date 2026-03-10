ROLE_MAP = {
    'retention_all':      ("u.department_ = %s",                                         ['Retention']),
    'retention_gmt':      ("u.department_ = %s AND u.office = %s",                       ['Retention', 'GMT']),
    'retention_abj_ng':   ("u.department_ = %s AND u.office = %s",                       ['Retention', 'ABJ-NG']),
    'retention_lag_ng':   ("u.department_ = %s AND u.office = %s",                       ['Retention', 'LAG-NG']),
    'retention_sa':       ("u.department_ = %s AND u.office = %s",                       ['Retention', 'SA']),
    'retention_bg_team1': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG', 'BG Team 1']),
    'retention_bg_team2': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG', 'BG Team 2']),
    'retention_bg_team3': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG', 'BG Team 3']),
    'sales_gmt':          ("u.department_ = %s AND u.office = %s",                       ['Sales', 'GMT']),
    'sales_lag_ng':       ("u.department_ = %s AND u.office = %s",                       ['Sales', 'LAG-NG']),
    'sales_abj_ng':       ("u.department_ = %s AND u.office = %s",                       ['Sales', 'ABJ-NG']),
    'sales_sa':           ("u.department_ = %s AND u.office = %s",                       ['Sales', 'SA']),
    'sales_bg':           ("u.department_ = %s AND u.office = %s",                       ['Sales', 'BG']),
}


def get_role_filter(user: dict) -> dict:
    role = user['role']
    if role in ('admin', 'general'):
        return {'crm_where': '', 'crm_params': [], 'is_full_access': True, 'filter_type': 'none'}
    if role == 'agent':
        return {
            'crm_where': ' AND u.id = %s',
            'crm_params': [user.get('crm_user_id')],
            'is_full_access': False,
            'filter_type': 'agent',
        }
    if role in ROLE_MAP:
        frag, params = ROLE_MAP[role]
        return {'crm_where': ' AND ' + frag, 'crm_params': params, 'is_full_access': False, 'filter_type': 'crm'}
    return {'crm_where': ' AND 1=0', 'crm_params': [], 'is_full_access': False, 'filter_type': 'crm'}
