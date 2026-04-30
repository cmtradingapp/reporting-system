"""Contract tests for the remaining high-traffic report routes.

Covers (one block per file):
- app/routes/total_traders.py — `/total-traders` page + `/api/total-traders` + options
- app/routes/dashboard.py — `/dashboard` page + `/api/dashboard`
- app/routes/fsa_report.py — `/fsa-report` page + 4 section APIs (s3-s6)

Same shape as the DMP / scoreboard / campaign tests: auth, allowed_pages
gating, cache-hit short-circuit, input validation. No DB needed.
"""

from __future__ import annotations

import pytest

from app import cache
from tests.conftest import make_user

# ────────────────────────────────────────────────────────────────────
# total_traders
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_total_traders_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/total-traders", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_total_traders_page_admin_renders(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/total-traders", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_total_traders_page_redirects_when_no_access(client_factory) -> None:
    """Non-admin without `total_traders` in allowed_pages → /performance."""
    user = make_user(role="agent", allowed_pages_list=["dashboard"])
    async with client_factory(user=user) as c:
        r = await c.get("/total-traders", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_total_traders_options_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/total-traders/options")
    assert r.status_code == 401


@pytest.mark.contract
async def test_total_traders_options_forbidden_for_non_admin_without_access(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["dashboard"])
    async with client_factory(user=user) as c:
        r = await c.get("/api/total-traders/options")
    assert r.status_code == 403


@pytest.mark.contract
async def test_total_traders_options_cache_hit(client_factory) -> None:
    fake = {"offices": ["GMT", "BG"], "teams": ["BG Team 1"]}
    cache.set("total_traders_opts_v2", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/total-traders/options")
    assert r.status_code == 200
    assert r.json() == fake


@pytest.mark.contract
async def test_total_traders_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/total-traders",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_total_traders_api_invalid_date_returns_400(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/total-traders",
            params={"date_from": "not-a-date", "date_to": "2026-04-30"},
        )
    assert r.status_code == 400


@pytest.mark.contract
async def test_total_traders_api_cache_hit_short_circuits(client_factory) -> None:
    """Cache key shape: total_traders_v9:{date_from}:{end_date}:{f_office}:{f_team}:{f_class}:{ftc}:{role}"""
    fake = {"daily": [], "marker": "from-cache"}
    cache.set("total_traders_v9:2026-04-01:2026-04-30:::None:None:admin", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/total-traders",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    assert r.json() == fake


# ────────────────────────────────────────────────────────────────────
# dashboard
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_dashboard_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/dashboard", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_dashboard_page_admin_renders(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/dashboard", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_dashboard_page_redirects_non_admin_with_no_dashboard_access(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get("/dashboard", follow_redirects=False)
    # Per dashboard.py: ap is not None and "dashboard" not in ap → /performance
    assert r.status_code == 307  # FastAPI default for `RedirectResponse(url=...)` without status
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_dashboard_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/dashboard")
    assert r.status_code == 401


# ────────────────────────────────────────────────────────────────────
# fsa_report
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_fsa_report_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/fsa-report", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_fsa_report_page_admin_renders(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/fsa-report", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_fsa_report_page_redirects_non_admin_without_access(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get("/fsa-report", follow_redirects=False)
    # Default RedirectResponse has status 307 (temporary redirect)
    assert r.status_code == 307
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_fsa_report_page_renders_for_user_with_explicit_grant(client_factory) -> None:
    user = make_user(role="agent", allowed_pages_list=["fsa_report"])
    async with client_factory(user=user) as c:
        r = await c.get("/fsa-report", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
@pytest.mark.parametrize("section", [3, 4, 5, 6])
async def test_fsa_report_sections_unauth_return_401(client_factory, section: int) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(f"/api/fsa-report/section{section}", params={"year": 2026, "quarter": 1})
    assert r.status_code == 401


@pytest.mark.contract
@pytest.mark.parametrize("section", [3, 4, 5, 6])
async def test_fsa_report_sections_forbidden_for_non_admin_without_access(client_factory, section: int) -> None:
    user = make_user(role="agent", allowed_pages_list=["performance"])
    async with client_factory(user=user) as c:
        r = await c.get(f"/api/fsa-report/section{section}", params={"year": 2026, "quarter": 1})
    assert r.status_code == 403


@pytest.mark.contract
@pytest.mark.parametrize("section", [3, 4, 5, 6])
async def test_fsa_report_sections_cache_hit(client_factory, section: int) -> None:
    fake = {"section": section, "marker": "from-cache"}
    cache.set(f"fsa_s{section}_v1:2026:1", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(f"/api/fsa-report/section{section}", params={"year": 2026, "quarter": 1})
    assert r.status_code == 200
    assert r.json() == fake
