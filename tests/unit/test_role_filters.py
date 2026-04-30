"""Unit tests for app.auth.role_filters.get_role_filter.

This is the riskiest pure-logic surface in the codebase — it gates every
report's row-level data access. Any subtle bug here either leaks data
across teams (read: between offices that shouldn't see each other) or
hides legitimate rows from people who should see them. Worth pinning
hard.

Background incident: commit 37afb55 fixed a cache-key collision where
two users with different `extra_roles` got the same cache entry —
proving the role-set composition is observably user-visible.

Tests cover every branch in `get_role_filter`:
1. Full-access roles (admin/general/marketing) → empty filter
2. Pure agent → filter by user.id
3. Agent with extra_roles → falls through to ROLE_MAP composition
4. Single ROLE_MAP entry → AND <fragment>
5. Multiple roles (primary + extras) → AND ((f1) OR (f2) ...)
6. Unknown / empty role with no valid extras → deny-all (AND 1=0)
7. Mixed valid/invalid roles → invalid silently dropped
8. ROLE_LABELS / ROLE_MAP key consistency (data invariant)
"""

from __future__ import annotations

import pytest

from app.auth.role_filters import ROLE_LABELS, ROLE_MAP, get_role_filter

# ── Full-access roles ──────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize("role", ["admin", "general", "marketing"])
def test_full_access_roles_emit_no_filter(role: str) -> None:
    result = get_role_filter({"role": role})
    assert result == {
        "crm_where": "",
        "crm_params": [],
        "is_full_access": True,
        "filter_type": "none",
    }


@pytest.mark.unit
def test_full_access_role_ignores_extra_roles() -> None:
    # Admin should never get filtered, regardless of extras.
    result = get_role_filter({"role": "admin", "extra_roles": ["retention_gmt"]})
    assert result["is_full_access"] is True
    assert result["crm_where"] == ""


# ── Agent role ─────────────────────────────────────────────────────────


@pytest.mark.unit
def test_agent_role_filters_by_user_id() -> None:
    result = get_role_filter({"role": "agent", "crm_user_id": 4242})
    assert result == {
        "crm_where": " AND u.id = %s",
        "crm_params": [4242],
        "is_full_access": False,
        "filter_type": "agent",
    }


@pytest.mark.unit
def test_agent_with_missing_crm_user_id_passes_none_param() -> None:
    """No crm_user_id → param is None. Caller's SQL will safely match nothing."""
    result = get_role_filter({"role": "agent"})
    assert result["filter_type"] == "agent"
    assert result["crm_params"] == [None]


@pytest.mark.unit
def test_agent_with_extra_roles_falls_through_to_role_map() -> None:
    """An agent whose user doc also lists team-level roles is treated as multi-role."""
    result = get_role_filter({"role": "agent", "extra_roles": ["retention_gmt"]})
    # Falls past the agent branch (because extra_roles is truthy) → ROLE_MAP path
    # Primary role "agent" is not in ROLE_MAP, so all_roles = ["retention_gmt"]
    assert result["filter_type"] == "crm"
    frag, params = ROLE_MAP["retention_gmt"]
    assert result["crm_where"] == f" AND {frag}"
    assert result["crm_params"] == params


# ── Single ROLE_MAP role ───────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize("role", list(ROLE_MAP.keys()))
def test_every_role_map_entry_produces_well_formed_filter(role: str) -> None:
    """Every role in ROLE_MAP must produce a valid AND-prefixed WHERE fragment."""
    result = get_role_filter({"role": role})
    frag, params = ROLE_MAP[role]
    assert result["crm_where"] == f" AND {frag}"
    assert result["crm_params"] == params
    assert result["is_full_access"] is False
    assert result["filter_type"] == "crm"
    # Fragment uses %s placeholders, one per param
    assert result["crm_where"].count("%s") == len(params)


@pytest.mark.unit
def test_retention_gmt_filters_office() -> None:
    result = get_role_filter({"role": "retention_gmt"})
    assert result["crm_where"] == " AND u.department_ = %s AND u.office = %s"
    assert result["crm_params"] == ["Retention", "GMT"]


@pytest.mark.unit
def test_retention_bg_team1_filters_office_and_team() -> None:
    result = get_role_filter({"role": "retention_bg_team1"})
    assert result["crm_where"] == " AND u.department_ = %s AND u.office = %s AND u.department = %s"
    assert result["crm_params"] == ["Retention", "BG", "BG Team 1"]


# ── Multi-role (extra_roles) composition ───────────────────────────────


@pytest.mark.unit
def test_two_roles_compose_with_or() -> None:
    result = get_role_filter({"role": "retention_gmt", "extra_roles": ["sales_gmt"]})
    assert result["filter_type"] == "crm"
    # Both fragments parenthesised and joined with OR, prefixed with " AND ("
    assert result["crm_where"] == (
        " AND ((u.department_ = %s AND u.office = %s) OR (u.department_ = %s AND u.office = %s))"
    )
    # Params concatenated in order: primary first, extras after
    assert result["crm_params"] == ["Retention", "GMT", "Sales", "GMT"]


@pytest.mark.unit
def test_three_roles_compose_with_or_and_preserve_order() -> None:
    result = get_role_filter(
        {
            "role": "retention_gmt",
            "extra_roles": ["sales_gmt", "retention_sa"],
        }
    )
    assert result["filter_type"] == "crm"
    # All three OR'd
    assert result["crm_where"].count(" OR ") == 2
    # Params in declared order: primary, extra[0], extra[1]
    assert result["crm_params"] == ["Retention", "GMT", "Sales", "GMT", "Retention", "SA"]


@pytest.mark.unit
def test_only_extras_no_primary() -> None:
    """No primary role in ROLE_MAP, only valid extras → use extras only."""
    result = get_role_filter({"role": "", "extra_roles": ["retention_gmt"]})
    frag, params = ROLE_MAP["retention_gmt"]
    assert result["crm_where"] == f" AND {frag}"
    assert result["crm_params"] == params


# ── Deny-all paths ─────────────────────────────────────────────────────


@pytest.mark.unit
def test_unknown_role_no_extras_denies_all() -> None:
    result = get_role_filter({"role": "definitely_not_a_real_role"})
    assert result == {
        "crm_where": " AND 1=0",
        "crm_params": [],
        "is_full_access": False,
        "filter_type": "crm",
    }


@pytest.mark.unit
def test_empty_user_dict_denies_all() -> None:
    """No role, no extras → fail closed."""
    result = get_role_filter({})
    assert result["crm_where"] == " AND 1=0"
    assert result["is_full_access"] is False


@pytest.mark.unit
def test_unknown_role_with_unknown_extras_denies_all() -> None:
    result = get_role_filter({"role": "fake1", "extra_roles": ["fake2", "fake3"]})
    assert result["crm_where"] == " AND 1=0"


# ── Robustness: invalid entries silently filtered ──────────────────────


@pytest.mark.unit
def test_invalid_extras_dropped_valid_extras_kept() -> None:
    result = get_role_filter(
        {
            "role": "retention_gmt",
            "extra_roles": ["bogus", "sales_gmt", "also_bogus"],
        }
    )
    # Should compose as primary OR sales_gmt, ignoring the bogus entries
    assert result["filter_type"] == "crm"
    assert result["crm_params"] == ["Retention", "GMT", "Sales", "GMT"]


@pytest.mark.unit
def test_extra_roles_none_treated_as_empty() -> None:
    """`extra_roles: None` (rather than missing or []) must not crash."""
    result = get_role_filter({"role": "retention_gmt", "extra_roles": None})
    frag, params = ROLE_MAP["retention_gmt"]
    assert result["crm_where"] == f" AND {frag}"
    assert result["crm_params"] == params


# ── Data-invariant tests (structural) ──────────────────────────────────


@pytest.mark.unit
def test_role_labels_cover_every_role_map_key() -> None:
    """Every ROLE_MAP key must have a human-readable label — UI uses these."""
    missing = set(ROLE_MAP.keys()) - set(ROLE_LABELS.keys())
    assert not missing, f"ROLE_LABELS missing entries for: {missing}"


@pytest.mark.unit
def test_role_map_fragments_use_only_named_columns() -> None:
    """All fragments reference u.department_, u.office, u.department only — never raw user input."""
    allowed_columns = {"u.department_", "u.office", "u.department"}
    for role, (frag, _params) in ROLE_MAP.items():
        # Strip out the SQL operators / placeholders / spaces; what's left should be column references
        tokens = frag.replace("%s", "").replace("=", "").replace("AND", "").split()
        non_column = [t for t in tokens if t not in allowed_columns]
        assert not non_column, f"{role}: unexpected tokens in fragment: {non_column}"
