"""Shared test fixtures.

Independence rule: every test starts from a clean slate. The autouse cache fixture
guarantees no test sees state left over from another. App import is gated by
TESTING=1 so the FastAPI lifespan never starts APScheduler / opens the production
DB pools / runs schema DDL during the test run.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

os.environ.setdefault("TESTING", "1")

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _reset_inmemory_cache() -> Iterator[None]:
    """Wipe the in-memory cache before every test for full independence."""
    try:
        from app import cache

        cache._reset_for_tests()
    except Exception:
        pass
    yield
    try:
        from app import cache

        cache._reset_for_tests()
    except Exception:
        pass


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def template_dir(repo_root: Path) -> Path:
    return repo_root / "app" / "templates"


@pytest.fixture
def routes_dir(repo_root: Path) -> Path:
    return repo_root / "app" / "routes"


@pytest.fixture
def test_database_url() -> str | None:
    """Postgres URL for integration tests. None ⇒ skip the test."""
    return os.environ.get("TEST_DATABASE_URL")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-skip integration tests when TEST_DATABASE_URL isn't set."""
    if os.environ.get("TEST_DATABASE_URL"):
        return
    skip_integration = pytest.mark.skip(reason="TEST_DATABASE_URL not set; skipping integration tier")
    for item in items:
        if "integration" in item.keywords or "tests/integration" in str(item.fspath).replace("\\", "/"):
            item.add_marker(skip_integration)


# ── Contract-tier helpers ─────────────────────────────────────────────


def make_user(
    user_id: int = 1,
    role: str = "admin",
    *,
    email: str = "user@example.test",
    extra_roles: list[str] | None = None,
    allowed_pages_list: list[str] | None = None,
    crm_user_id: int | None = None,
    department_: str = "",
    is_active: int = 1,
    full_name: str = "Test User",
) -> dict:
    """Build the user dict shape returned by `get_auth_user_by_id`.

    Defaults to an admin user; override fields per-test as needed.
    """
    return {
        "id": user_id,
        "email": email,
        "full_name": full_name,
        "role": role,
        "extra_roles": extra_roles,
        "allowed_pages_list": allowed_pages_list,
        "crm_user_id": crm_user_id,
        "department_": department_,
        "is_active": is_active,
    }


@pytest.fixture
def client_factory(monkeypatch: pytest.MonkeyPatch):
    """Build authenticated/anonymous httpx AsyncClients against the FastAPI app.

    Stubs the DB lookup in `app.auth.dependencies.get_auth_user_by_id` so contract
    tests don't need a Postgres. Pass a user dict (use `make_user(...)`) to mint a
    valid `access_token` cookie; pass None for an anonymous client.

    Returns a callable: `await client_factory(user)` → AsyncClient ready for use
    with `async with`.
    """
    from httpx import ASGITransport, AsyncClient

    from app.auth.auth import create_access_token
    from app.main import app

    user_db: dict[int, dict] = {}

    def _fake_get_auth_user_by_id(user_id: int) -> dict | None:
        return user_db.get(user_id)

    monkeypatch.setattr("app.auth.dependencies.get_auth_user_by_id", _fake_get_auth_user_by_id)

    transport = ASGITransport(app=app)

    def _make(user: dict | None = None) -> AsyncClient:
        cookies: dict[str, str] = {}
        if user is not None:
            user_db[user["id"]] = user
            cookies["access_token"] = create_access_token(user["id"])
        return AsyncClient(transport=transport, cookies=cookies, base_url="http://test")

    return _make
