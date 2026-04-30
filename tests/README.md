# Tests

Phase 0 of the TDD rollout — see `make-sure-in-this-stateful-reddy.md` for the full plan.

## TL;DR

```bash
make setup       # install dev deps + pre-commit + Playwright Chromium
make test-fast   # unit tests only — what pre-commit runs (sub-15s)
make test        # full suite (needs TEST_DATABASE_URL for integration tier)
make coverage    # opens htmlcov/index.html
make lint        # ruff + mypy
```

## Layout

```
tests/
├── conftest.py               # shared fixtures (cache reset, paths, DB skip)
├── fixtures/
│   └── schema.sql            # minimal Postgres schema for integration tier
├── unit/                     # fast, no IO, no DB — runs in pre-commit
├── contract/                 # FastAPI route tests via httpx.AsyncClient (DB mocked)
├── integration/              # real Postgres, ETL idempotency, MV refresh
└── gui/                      # Playwright browser tests
```

## The four tiers

| Tier | Requires | Speed/test | Marker |
|---|---|---|---|
| `unit` | nothing | <2 ms | `@pytest.mark.unit` |
| `contract` | nothing (DB mocked) | 10–100 ms | `@pytest.mark.contract` |
| `integration` | `TEST_DATABASE_URL` env var pointing at a Postgres | 100 ms – 2 s | `@pytest.mark.integration` |
| `gui` | dev server running on `BASE_URL` | 1–15 s | `@pytest.mark.gui` |

Integration tests auto-skip without `TEST_DATABASE_URL`. CI provides one via a Postgres service container.

## Independence rule

Every test sets up its own state, asserts, and tears down. The `_reset_inmemory_cache` autouse fixture wipes `app.cache` around each test. `pytest-randomly` randomises order to catch hidden coupling.

## Writing a test

- **Unit**: pure-logic. Import the function, call it, assert. No mocks of `time`, no I/O. Use `monkeypatch` for environment.
- **Contract**: spin up `httpx.AsyncClient(transport=ASGITransport(app=app))`, hit a route, assert status + JSON shape. Stub the DB layer at the connection-factory boundary, not at psycopg2 internals.
- **Integration**: connect to `TEST_DATABASE_URL`, seed only the rows the test needs, call the function, assert end state. Use `pytest.fixture(scope="function")` so each test gets a clean transaction (rollback in teardown).
- **GUI**: Playwright. Each test is one user journey. Wait on observable state (text content, URL change), never `sleep()`.

## When you fix a bug

Add a regression test in the **most local** tier that catches the bug:

| Bug type | Goes in |
|---|---|
| JS template state-shape mistake (the `dTradersHQ` class) | `tests/unit/test_template_state_invariants.py` |
| Wrong SQL output | `tests/integration/` |
| API returns wrong shape | `tests/contract/` |
| Page renders error to user | `tests/gui/` |

Whenever possible, prefer a **generalised invariant** over a one-off pin (the way `test_defaults_cover_every_used_key` catches the whole class of mistake, not just `dailyTradersHQ`).

## SOLID under cover

We don't refactor the world up front. As you write contract tests for a route:

1. Write the test against current behaviour first (it must pass before any refactor).
2. Extract the route's SQL into `app/repositories/<area>.py` (SRP).
3. Add unit tests for the repo with a fake `Connection` (DIP / ISP via `Depends(get_db)`).
4. The route shrinks to: parse params → call repo → format response.

That sequence keeps the test green at every step.
