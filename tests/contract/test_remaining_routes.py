"""Contract tests for the remaining report routes.

Covers:
- /all-ftcs + /api/all-ftcs
- /eez-comparison + /api/eez-comparison (admin-only)
- /eez-old + /api/eez-old
- /transactions-report + /api/transactions-report
- /ftc-date + /api/ftc-date + /api/ftc-date/options

Same shape: auth + access boundary + cache-hit short-circuit.
"""

from __future__ import annotations

import pytest

from app import cache
from tests.conftest import make_user

# ────────────────────────────────────────────────────────────────────
# all_ftcs
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_all_ftcs_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/all-ftcs", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_all_ftcs_page_admin_renders(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/all-ftcs", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_all_ftcs_page_marketing_role_redirected(client_factory) -> None:
    """marketing/agent with no allowed_pages → /performance."""
    async with client_factory(user=make_user(role="marketing", allowed_pages_list=None)) as c:
        r = await c.get("/all-ftcs", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_all_ftcs_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get(
            "/api/all-ftcs",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 401


@pytest.mark.contract
async def test_all_ftcs_api_marketing_role_403(client_factory) -> None:
    async with client_factory(user=make_user(role="marketing", allowed_pages_list=None)) as c:
        r = await c.get(
            "/api/all-ftcs",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 403


@pytest.mark.contract
async def test_all_ftcs_api_cache_hit(client_factory) -> None:
    fake = {"rows": [], "marker": "from-cache"}
    cache.set("all_ftcs_v13:admin:2026-04-01:2026-04-30", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get(
            "/api/all-ftcs",
            params={"date_from": "2026-04-01", "date_to": "2026-04-30"},
        )
    assert r.status_code == 200
    assert r.json() == fake


# ────────────────────────────────────────────────────────────────────
# eez_comparison (admin-only)
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_eez_comparison_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/eez-comparison", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"


@pytest.mark.contract
async def test_eez_comparison_page_admin_renders(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/eez-comparison", follow_redirects=False)
    assert r.status_code == 200


@pytest.mark.contract
async def test_eez_comparison_page_redirects_non_admin(client_factory) -> None:
    """Hard admin-only — non-admin redirected regardless of allowed_pages."""
    async with client_factory(user=make_user(role="agent", allowed_pages_list=["eez"])) as c:
        r = await c.get("/eez-comparison", follow_redirects=False)
    assert r.status_code == 307
    assert r.headers["location"] == "/performance"


@pytest.mark.contract
async def test_eez_comparison_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/eez-comparison")
    assert r.status_code == 401


@pytest.mark.contract
async def test_eez_comparison_api_cache_hit(client_factory) -> None:
    fake = {"rows": [], "marker": "from-cache"}
    cache.set("eez_comparison_v22", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/eez-comparison")
    assert r.status_code == 200
    assert r.json() == fake


# ────────────────────────────────────────────────────────────────────
# eez_old
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_eez_old_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/eez-old", follow_redirects=False)
    assert r.status_code == 302


@pytest.mark.contract
async def test_eez_old_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/eez-old")
    assert r.status_code == 401


@pytest.mark.contract
async def test_eez_old_api_cache_hit(client_factory) -> None:
    fake = {"rows": [], "marker": "from-cache"}
    cache.set("eez_old_v15", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/eez-old")
    assert r.status_code == 200
    assert r.json() == fake


# ────────────────────────────────────────────────────────────────────
# transactions_report
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_transactions_report_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/transactions-report", follow_redirects=False)
    assert r.status_code == 302


@pytest.mark.contract
async def test_transactions_report_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/transactions-report")
    assert r.status_code == 401


@pytest.mark.contract
async def test_transactions_report_api_cache_hit(client_factory) -> None:
    fake = {"rows": [], "marker": "from-cache"}
    cache.set("txn_report_v1", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/transactions-report")
    assert r.status_code == 200
    assert r.json() == fake


# ────────────────────────────────────────────────────────────────────
# ftc_date
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_ftc_date_page_redirects_anon(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/ftc-date", follow_redirects=False)
    assert r.status_code == 302


@pytest.mark.contract
async def test_ftc_date_options_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/ftc-date/options")
    assert r.status_code == 401


@pytest.mark.contract
async def test_ftc_date_options_cache_hit(client_factory) -> None:
    fake = {"offices": [], "teams": []}
    cache.set("ftc_date_opts_v1", fake)
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/api/ftc-date/options")
    assert r.status_code == 200
    assert r.json() == fake


@pytest.mark.contract
async def test_ftc_date_api_unauth_returns_401(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/api/ftc-date", params={"end_date": "2026-04-30"})
    assert r.status_code == 401


# ────────────────────────────────────────────────────────────────────
# auth login flow (POST /login)
# ────────────────────────────────────────────────────────────────────


@pytest.mark.contract
async def test_login_get_renders_when_unauth(client_factory) -> None:
    async with client_factory(user=None) as c:
        r = await c.get("/login")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


@pytest.mark.contract
async def test_login_post_with_no_matching_user_returns_401(client_factory, monkeypatch) -> None:
    """`get_auth_user_by_email` is called by the login route. Stub it to return None."""
    from app.routes import auth as auth_route

    monkeypatch.setattr(auth_route, "get_auth_user_by_email", lambda email: None)

    async with client_factory(user=None) as c:
        r = await c.post("/login", data={"email": "nobody@cmtrading.com", "password": "wrong"})
    assert r.status_code == 401


@pytest.mark.contract
async def test_logout_clears_cookie_and_redirects(client_factory) -> None:
    async with client_factory(user=make_user(role="admin")) as c:
        r = await c.get("/logout", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"
    # Set-Cookie header should clear access_token
    set_cookie = r.headers.get("set-cookie", "")
    assert "access_token=" in set_cookie
