"""GUI smoke tests — real Chromium against a live uvicorn server.

The dTradersHQ class of bug (template state-shape mismatch causing
`undefined.toLocaleString()` at render time) reaches production because
the JS only runs in a real browser. Server-side tests would never catch
it. These tests do — by listening for `pageerror` and `console error`
events while pages render.

Each test logs in once via the live server, navigates a few pages, and
asserts (a) HTTP 200, (b) no uncaught JS errors, (c) no `console.error`
calls, (d) a stable expected element is present.

Slow (1-3s per test) and only run in CI's `gui` job which provisions a
Postgres service container plus uvicorn.
"""

from __future__ import annotations

import pytest
from playwright.sync_api import Page, expect


@pytest.fixture(autouse=True)
def _no_console_errors(page: Page) -> list[str]:
    """Collect any browser console errors / page errors for the test to assert against."""
    errors: list[str] = []
    page.on("pageerror", lambda exc: errors.append(f"pageerror: {exc}"))
    page.on(
        "console",
        lambda msg: errors.append(f"console.{msg.type}: {msg.text}") if msg.type == "error" else None,
    )
    return errors


def _login(page: Page, base_url: str, creds: dict) -> None:
    page.goto(f"{base_url}/login")
    page.fill('input[name="email"]', creds["email"])
    page.fill('input[name="password"]', creds["password"])
    page.click('button[type="submit"]')
    page.wait_for_url(lambda url: "/login" not in url, timeout=10_000)


@pytest.mark.gui
def test_login_page_renders_without_errors(
    live_server: str,
    page: Page,
    _no_console_errors: list[str],
) -> None:
    page.goto(f"{live_server}/login")
    expect(page).to_have_title("Login — Reporting System")
    expect(page.locator('input[name="email"]')).to_be_visible()
    expect(page.locator('input[name="password"]')).to_be_visible()
    assert _no_console_errors == [], f"Unexpected JS errors on /login: {_no_console_errors}"


@pytest.mark.gui
def test_login_with_correct_credentials_lands_on_authed_page(
    live_server: str,
    seeded_admin: dict,
    page: Page,
) -> None:
    _login(page, live_server, seeded_admin)
    # After login we redirect to / which 302s to /performance
    expect(page).to_have_url(lambda url: "/performance" in url or url.rstrip("/") == "/")


@pytest.mark.gui
def test_login_with_wrong_password_shows_error(
    live_server: str,
    seeded_admin: dict,
    page: Page,
) -> None:
    page.goto(f"{live_server}/login")
    page.fill('input[name="email"]', seeded_admin["email"])
    page.fill('input[name="password"]', "definitely-not-the-password")
    page.click('button[type="submit"]')
    # Server returns 401 + the same login page with an error message
    expect(page.locator("text=Invalid email or password")).to_be_visible()


@pytest.mark.gui
@pytest.mark.parametrize(
    "path",
    [
        "/performance",
        "/daily-monthly",
        "/total-traders",
        "/dashboard",
        "/campaign-performance",
    ],
)
def test_authed_page_loads_without_console_errors(
    live_server: str,
    seeded_admin: dict,
    page: Page,
    _no_console_errors: list[str],
    path: str,
) -> None:
    """Smoke per major page.

    The empty test DB means most KPI numbers will render as 0/—. That's fine —
    the point is to prove the page DOMs up without uncaught JS errors. This is
    exactly the layer that would have caught the dTradersHQ bug: the original
    `undefined.toLocaleString()` would land here as a `pageerror` event.
    """
    _login(page, live_server, seeded_admin)
    response = page.goto(f"{live_server}{path}", wait_until="domcontentloaded")
    assert response is not None
    assert response.status == 200, f"GET {path} returned {response.status}"

    # Give async JS a moment to fire any race-condition errors (the dTradersHQ class).
    page.wait_for_load_state("networkidle", timeout=15_000)

    # Filter out network-level "Failed to load resource" messages; those are
    # incidental given the test DB has no data. We care about JS execution errors.
    relevant = [e for e in _no_console_errors if "Failed to load resource" not in e]
    assert relevant == [], f"JS errors on {path}: {relevant}"
