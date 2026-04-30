"""Contract tier smoke + bootstrap.

Two purposes:
1. Prevent pytest exit-code 5 ("no tests collected") in CI before Phase 2 lands.
2. Verify that `import app.main` works with TESTING=1 — proving the lifespan
   short-circuit holds and the FastAPI app object is constructible without
   triggering APScheduler / DB pools / DDL. Any future regression that breaks
   bare `import app.main` for tests (e.g. a top-level side effect creeping in)
   fails here before downstream contract tests get confusing errors.

Phase 2 will replace this with real route-level contract tests via
`httpx.AsyncClient(transport=ASGITransport(app=app))`.
"""

from __future__ import annotations

import pytest


@pytest.mark.contract
def test_app_module_imports_under_testing_flag() -> None:
    from app.main import app

    assert app is not None


@pytest.mark.contract
def test_app_has_registered_routes() -> None:
    from app.main import app

    paths = [getattr(r, "path", "") for r in app.routes]
    assert any(p.startswith("/api/") for p in paths), (
        "No /api/ routes registered — route module imports may be silently failing."
    )


@pytest.mark.contract
def test_lifespan_skipped_under_testing() -> None:
    """The TESTING=1 gate in app/main.py must short-circuit the lifespan."""
    import os

    from app import main as app_main

    assert os.environ.get("TESTING") == "1"
    assert app_main._TESTING is True, (
        "_TESTING flag did not pick up TESTING=1 — APScheduler / DB DDL would run during tests."
    )
