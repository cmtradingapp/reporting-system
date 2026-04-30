"""GUI tier fixtures.

Starts a real uvicorn process pointing at the test Postgres so Playwright
has a server to talk to. Seeds an admin user once per session so tests can
log in.

Auto-skipped locally when TEST_DATABASE_URL isn't set.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import httpx
import pytest


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(url: str, timeout: float = 30.0) -> None:
    """Poll until uvicorn is accepting connections, or fail fast."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=2.0, follow_redirects=False)
            # Any HTTP response (200/302/etc.) means the server is up.
            if r.status_code < 600:
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.4)
    raise RuntimeError(f"Server at {url} didn't start within {timeout}s. Last error: {last_err!r}")


@pytest.fixture(scope="session")
def live_server(request: pytest.FixtureRequest) -> Iterator[str]:
    """Run a uvicorn process with the app pointed at the test Postgres.

    Yields the base URL (e.g. http://127.0.0.1:53421). Skipped if no DB.
    """
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("TEST_DATABASE_URL not set; GUI tests need a live server with DB")

    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env["TESTING"] = "1"  # short-circuits the lifespan (no scheduler / DDL)

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_server(f"{base_url}/login")
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@pytest.fixture(scope="session")
def seeded_admin(live_server: str) -> dict:
    """Create an admin user in the test DB once per session for GUI login tests.

    Returns the credentials so each test knows what to type in the login form.
    """
    from app.auth.auth import hash_password
    from app.db.postgres_conn import create_auth_user, get_auth_user_by_email

    email = "gui-admin@cmtrading.com"
    password = "gui-test-pw"

    # Idempotent: if already seeded by a previous test run on the same DB, skip create.
    existing = get_auth_user_by_email(email)
    if existing is None:
        create_auth_user(
            email=email,
            full_name="GUI Admin",
            password_hash=hash_password(password),
            role="admin",
            crm_user_id=None,
        )
    return {"email": email, "password": password}
