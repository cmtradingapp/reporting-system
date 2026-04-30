"""GUI smoke tests — real Chromium against a live uvicorn server.

Scoped narrowly for now (login flow + the login page itself). Multi-page
authed smoke tests are deferred until the integration tier seeds enough
data (materialised views, real KPI fixtures) for routes to render
cleanly without 500s in the empty test DB.

What we still pin here:
- POST /login validates credentials end-to-end through DB.
- The login page itself renders without uncaught JS exceptions —
  exactly the layer that would catch a `pageerror` regression in the
  login form's inline JS.

Failure mode this catches: uncaught JavaScript exceptions on the login
page (page-load JS bugs that don't depend on backend data).
"""

from __future__ import annotations

import re

import pytest
from playwright.sync_api import Page, expect


@pytest.fixture
def js_errors(page: Page) -> list[str]:
    """Collect uncaught JS exceptions for the test to assert on.

    `pageerror` is the right signal — it fires when an exception escapes
    application JS. `console.error` is too noisy in CI (CORS-blocked CDN
    scripts, network failures from missing MVs in the test DB).
    """
    errors: list[str] = []
    page.on("pageerror", lambda exc: errors.append(f"pageerror: {exc}"))
    return errors


# ── /login: page-level pins (no DB needed beyond the seeded admin) ────


@pytest.mark.gui
def test_login_page_renders(live_server: str, page: Page, js_errors: list[str]) -> None:
    page.goto(f"{live_server}/login")
    expect(page).to_have_title(re.compile(r"Login"))
    expect(page.locator('input[name="email"]')).to_be_visible()
    expect(page.locator('input[name="password"]')).to_be_visible()
    expect(page.locator('button[type="submit"]')).to_be_visible()
    assert js_errors == [], f"Uncaught JS exception on /login: {js_errors}"


@pytest.mark.gui
def test_login_with_wrong_password_shows_error(
    live_server: str,
    seeded_admin: dict,
    page: Page,
    js_errors: list[str],
) -> None:
    page.goto(f"{live_server}/login")
    page.fill('input[name="email"]', seeded_admin["email"])
    page.fill('input[name="password"]', "definitely-not-the-password")
    page.click('button[type="submit"]')
    expect(page.locator("text=Invalid email or password")).to_be_visible()
    assert js_errors == [], f"Uncaught JS exception on failed login: {js_errors}"


@pytest.mark.gui
def test_login_with_correct_credentials_leaves_login_page(
    live_server: str,
    seeded_admin: dict,
    page: Page,
    js_errors: list[str],
) -> None:
    """Correct creds → server redirects away from /login. Pin via direct URL check."""
    page.goto(f"{live_server}/login")
    page.fill('input[name="email"]', seeded_admin["email"])
    page.fill('input[name="password"]', seeded_admin["password"])
    page.click('button[type="submit"]')
    page.wait_for_url(re.compile(r".*(?<!/login).*$"), timeout=10_000)
    # We're off /login, on whatever the post-login redirect lands us at
    assert "/login" not in page.url, f"Still on /login after submit: {page.url}"
    assert js_errors == [], f"Uncaught JS exception on successful login: {js_errors}"


@pytest.mark.gui
def test_logout_clears_session_and_lands_on_login(
    live_server: str,
    seeded_admin: dict,
    page: Page,
    js_errors: list[str],
) -> None:
    """Login → logout → request authed page → bounced back to login."""
    # Log in first
    page.goto(f"{live_server}/login")
    page.fill('input[name="email"]', seeded_admin["email"])
    page.fill('input[name="password"]', seeded_admin["password"])
    page.click('button[type="submit"]')
    page.wait_for_url(re.compile(r".*(?<!/login).*$"), timeout=10_000)

    # Now hit /logout — should clear cookie + redirect to /login
    page.goto(f"{live_server}/logout")
    expect(page).to_have_url(re.compile(r".*/login$"))

    # Subsequent /performance request → bounced to /login (no cookie)
    page.goto(f"{live_server}/performance")
    assert "/login" in page.url
    assert js_errors == [], f"Uncaught JS exception on logout flow: {js_errors}"
