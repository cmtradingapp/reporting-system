"""Contract tests for the Marketing / Campaign Performance routes.

Covers `/campaign-performance` (page), `/api/campaign-performance/filter-options`,
`/api/campaign-performance` (KPI cards), and `/api/campaign-performance/table`
(grouped table). Auth, allowed_pages routing, group/period validation, and
cache-hit short-circuits — no DB needed for any of this.
"""

from __future__ import annotations

import pytest

from app import cache
from tests.conftest import make_user

# ── GET /campaign-performance (page) ──────────────────────────────────


@pytest.mark.contract
async def test_camp_page_redirects_to_login_without_auth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/campaign-performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_camp_page_renders_for_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/campaign-performance", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_camp_page_renders_for_marketing(client_factory) -> None:
    """`marketing` role with no allowed_pages restriction → renders."""
    async with client_factory(user=make_user(role="marketing", allowed_pages_list=None)) as c:
        r = await c.get("/campaign-performance", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_camp_page_redirects_when_marketing_not_in_allowed_pages(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get("/campaign-performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_camp_page_redirects_non_admin_with_no_allowed_pages(client_factory) -> None:
    """Non-admin with allowed_pages_list=None → still redirected unless role ∈ {admin,marketing,general}."""
    user = make_user(role="retention_gmt", allowed_pages_list=None)
    async with client_factory(user=user) as c:
        r = await c.get("/campaign-performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/performance"


# ── GET /api/campaign-performance/filter-options ──────────────────────


@pytest.mark.contract
async def test_camp_filter_options_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/campaign-performance/filter-options")
    assert r.status_code == 401


@pytest.mark.contract
async def test_camp_filter_options_cache_hit(client_factory) -> None:
    fake = {"channels": ["A", "B"], "marketing_groups": ["mg1"], "marker": "from-cache"}
    cache.set("camp_filter_opts_v3", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/campaign-performance/filter-options")
    assert r.status_code == 200
    assert r.json() == fake


# ── GET /api/campaign-performance (KPI cards) ─────────────────────────


@pytest.mark.contract
async def test_camp_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/campaign-performance",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_camp_api_invalid_date_to_returns_400(client_factory) -> None:
    """KPI route validates only date_to (not date_from)."""
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance",
            params={"date_from": "2026-04-01", "date_to": "garbage"},
        )
    # Has to first miss the cache — rely on the empty-cache state
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid date format"}


@pytest.mark.contract
async def test_camp_api_cache_hit_short_circuits(client_factory) -> None:
    """Cache key has many filter params; defaults map to None/empty per route logic."""
    fake = {"net_deposits": 0, "marker": "from-cache"}
    # Default cache key when no filters: matches the route's f-string
    # camp_perf_v14:{date_from}:{date_to}:None:None:None: ... :None
    key = "camp_perf_v14:2026-04-01:2026-04-30:None:None:None:::::::::::None"
    cache.set(key, fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    assert r.json() == fake


# ── GET /api/campaign-performance/table ───────────────────────────────


@pytest.mark.contract
async def test_camp_table_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/campaign-performance/table",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_camp_table_invalid_group1_returns_400(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance/table",
            params={
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "group1": "not-a-real-group",
            },
        )
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid group1"}


@pytest.mark.contract
async def test_camp_table_invalid_group2_returns_400(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance/table",
            params={
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "group1": "campaign_name",
                "group2": "bogus",
            },
        )
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid group2"}


@pytest.mark.contract
async def test_camp_table_invalid_period_returns_400(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance/table",
            params={
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "period": "century",
            },
        )
    assert r.status_code == 400
    assert r.json() == {"detail": "Invalid period"}


@pytest.mark.contract
@pytest.mark.parametrize("period", ["day", "month", "year"])
async def test_camp_table_accepts_every_valid_period(client_factory, period: str) -> None:
    """Each valid period must reach a cache check (we prime the cache so DB never fires)."""
    # The cache key spans many params; build the empty-filter shape. Read campaign_performance.py
    # _ck construction at line 1568+ for the exact f-string.
    fake = {"rows": [], "marker": f"period-{period}"}
    # Cache key shape from app/routes/campaign_performance.py:1568
    key = f"camp_tbl_v18:2026-04-01:2026-04-30:none:none:{period}:::::::None:None:None:None:::::None"
    cache.set(key, fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/campaign-performance/table",
            params={
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "period": period,
            },
        )
    assert r.status_code == 200
    assert r.json() == fake
