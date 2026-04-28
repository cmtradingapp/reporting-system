ROLE_MAP = {
    'retention_all':          ("u.department_ = %s",                                         ['Retention']),
    'sales_all':              ("u.department_ = %s",                                         ['Sales']),
    'retention_gmt':          ("u.department_ = %s AND u.office = %s",                       ['Retention', 'GMT']),
    'retention_cy':           ("u.department_ = %s AND u.office = %s",                       ['Retention', 'CY']),
    'retention_abj_ng':       ("u.department_ = %s AND u.office = %s",                       ['Retention', 'ABJ-NG']),
    'retention_lag_ng':       ("u.department_ = %s AND u.office = %s",                       ['Retention', 'LAG-NG']),
    'retention_sa':           ("u.department_ = %s AND u.office = %s",                       ['Retention', 'SA']),
    'retention_bg_team1':     ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG',     'BG Team 1']),
    'retention_bg_team2':     ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG',     'BG Team 2']),
    'retention_bg_team3':     ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'BG',     'BG Team 3']),
    'retention_abj_ng_team1': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'ABJ-NG', 'NG Team 1']),
    'retention_abj_ng_team2': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'ABJ-NG', 'NG Team 2']),
    'retention_abj_ng_team3': ("u.department_ = %s AND u.office = %s AND u.department = %s", ['Retention', 'ABJ-NG', 'NG Team 3']),
    'sales_gmt':              ("u.department_ = %s AND u.office = %s",                       ['Sales', 'GMT']),
    'sales_lag_ng':           ("u.department_ = %s AND u.office = %s",                       ['Sales', 'LAG-NG']),
    'sales_abj_ng':           ("u.department_ = %s AND u.office = %s",                       ['Sales', 'ABJ-NG']),
    'sales_sa':               ("u.department_ = %s AND u.office = %s",                       ['Sales', 'SA']),
    'sales_bg':               ("u.department_ = %s AND u.office = %s",                       ['Sales', 'BG']),
}

# Human-readable labels for the UI
ROLE_LABELS = {k: k.replace('_', ' ').title() for k in ROLE_MAP}
ROLE_LABELS.update({
    'retention_all':          'Retention - All',
    'sales_all':              'Sales - All',
    'retention_gmt':          'Retention - GMT',
    'retention_cy':           'Retention - Cyprus',
    'retention_abj_ng':       'Retention - ABJ-NG',
    'retention_lag_ng':       'Retention - LAG-NG',
    'retention_sa':           'Retention - SA',
    'retention_bg_team1':     'Retention - BG Team 1',
    'retention_bg_team2':     'Retention - BG Team 2',
    'retention_bg_team3':     'Retention - BG Team 3',
    'retention_abj_ng_team1': 'Retention - ABJ-NG Team 1',
    'retention_abj_ng_team2': 'Retention - ABJ-NG Team 2',
    'retention_abj_ng_team3': 'Retention - ABJ-NG Team 3',
    'sales_gmt':              'Sales - GMT',
    'sales_lag_ng':           'Sales - LAG-NG',
    'sales_abj_ng':           'Sales - ABJ-NG',
    'sales_sa':               'Sales - SA',
    'sales_bg':               'Sales - BG',
})


def get_role_filter(user: dict) -> dict:
    """Build SQL WHERE fragment for the user's role(s).
    Supports a primary `role` plus optional `extra_roles` list for multi-role users.
    Multiple roles are combined with OR so the user sees the union of all their teams.
    """
    role = user.get('role', '')
    extra_roles = user.get('extra_roles') or []  # list of additional role keys

    # Full-access roles — no filter needed
    if role in ('admin', 'general', 'marketing'):
        return {'crm_where': '', 'crm_params': [], 'is_full_access': True, 'filter_type': 'none'}

    # Agent — filter to their specific CRM user
    if role == 'agent' and not extra_roles:
        return {
            'crm_where': ' AND u.id = %s',
            'crm_params': [user.get('crm_user_id')],
            'is_full_access': False,
            'filter_type': 'agent',
        }

    # Collect all active roles (primary + extras)
    all_roles = ([role] if role in ROLE_MAP else []) + [r for r in extra_roles if r in ROLE_MAP]

    if not all_roles:
        return {'crm_where': ' AND 1=0', 'crm_params': [], 'is_full_access': False, 'filter_type': 'crm'}

    if len(all_roles) == 1:
        frag, params = ROLE_MAP[all_roles[0]]
        return {'crm_where': ' AND ' + frag, 'crm_params': params, 'is_full_access': False, 'filter_type': 'crm'}

    # Multiple roles: combine with OR
    or_parts, all_params = [], []
    for r in all_roles:
        frag, params = ROLE_MAP[r]
        or_parts.append(f'({frag})')
        all_params.extend(params)
    combined = ' OR '.join(or_parts)
    return {'crm_where': f' AND ({combined})', 'crm_params': all_params, 'is_full_access': False, 'filter_type': 'crm'}
