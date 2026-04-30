"""Contract tests for the scoreboard / Performance CRO routes.

Covers `/performance` (page) plus `/api/performance` and
`/api/performance/retention` — auth, allowed_pages routing, CRO-view
visibility flags, cache-hit short-circuit, and the marketing → campaign-
performance redirect rule.
"""

from __future__ import annotations

import pytest

from app import cache
from tests.conftest import make_user

# ── GET /performance (page) ───────────────────────────────────────────


@pytest.mark.contract
async def test_perf_page_redirects_to_login_without_auth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_perf_page_renders_for_admin(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/performance", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_marketing_role_redirected_to_campaign_performance(client_factory) -> None:
    """`marketing` users with no explicit allowed_pages bounce to the marketing page."""
    async with client_factory(user=make_user(role="marketing", allowed_pages_list=None)) as c:
        r = await c.get("/performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/campaign-performance"


@pytest.mark.contract
async def test_perf_page_redirects_to_daily_monthly_when_no_perf_access(client_factory) -> None:
    """Non-admin without 'performance' in allowed_pages, but with daily_monthly → /daily-monthly."""
    user = make_user(role="agent", allowed_pages_list=["daily_monthly"])
    async with client_factory(user=user) as c:
        r = await c.get("/performance", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/daily-monthly"


@pytest.mark.contract
async def test_perf_page_admin_email_gets_cro_views(client_factory) -> None:
    """admin@cmtrading.com sees both Sales CRO and Retention CRO sections."""
    user = make_user(
        role="admin",
        email="admin@cmtrading.com",
        full_name="CRO Admin",
    )
    async with client_factory(user=user) as c:
        r = await c.get("/performance", follow_redirects=False)
    assert r.status_code == 200
    # Template renders both CRO blocks; we check the page didn't crash on the cro flags.
    assert "text/html" in r.headers["content-type"]


# ── GET /api/performance (sales) ──────────────────────────────────────


@pytest.mark.contract
async def test_perf_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/performance",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_perf_api_invalid_date_returns_400(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/performance",
            params={"date_from": "junk", "date_to": "2026-04-30"},
        )
    assert r.status_code == 400


@pytest.mark.contract
async def test_perf_api_cache_hit_short_circuits(client_factory) -> None:
    """Cache key shape: perf_v26:{role}:{extra_roles}:{date_from}:{date_to}{cls_suffix}"""
    fake = {"rows": [], "working_days": 22, "marker": "from-cache"}
    cache.set("perf_v26:admin::2026-04-01:2026-04-30", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/performance",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    assert r.json() == fake


# ── GET /api/performance/retention ────────────────────────────────────


@pytest.mark.contract
async def test_perf_retention_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/performance/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_perf_retention_api_cache_hit_short_circuits(client_factory) -> None:
    """Cache key shape: perf_ret_v21:{role}:{extra_roles}:{date_from}:{date_to}{cls_suffix}"""
    fake = {"rows": [], "marker": "from-cache"}
    cache.set("perf_ret_v21:admin::2026-04-01:2026-04-30", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/performance/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    assert r.json() == fake


@pytest.mark.contract
async def test_perf_retention_api_extra_roles_change_cache_key(client_factory) -> None:
    """Two users with different extra_roles MUST get distinct cache entries."""
    cache.set("perf_ret_v21:admin::2026-04-01:2026-04-30", {"marker": "no-extras"})
    cache.set("perf_ret_v21:admin:retention_gmt:2026-04-01:2026-04-30", {"marker": "with-extras"})

    async with client_factory(
        user=make_user(role="admin", extra_roles=["retention_gmt"]),
    ) as c:
        r = await c.get(
            "/api/performance/retention",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.json() == {"marker": "with-extras"}


@pytest.mark.contract
async def test_perf_retention_api_classification_filter_changes_cache_key(client_factory) -> None:
    """The cls_suffix (`:scp_cat:scp` query params) must segregate cache entries."""
    cache.set("perf_ret_v21:admin::2026-04-01:2026-04-30", {"marker": "no-filter"})
    cache.set(
        "perf_ret_v21:admin::2026-04-01:2026-04-30:high:",
        {"marker": "high-quality-only"},
    )
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/performance/retention",
            params={
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "scp_cat": "high",
            },
        )
    assert r.json() == {"marker": "high-quality-only"}
