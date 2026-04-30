"""Unit tests for app.cache — TTL semantics + isolation hooks.

`extra_roles` cache-key collision was a real bug (commit 37afb55). Cache
itself is the right place to test isolation primitives.
"""

from __future__ import annotations

import time

import pytest

from app import cache


@pytest.mark.unit
def test_set_and_get_roundtrip() -> None:
    cache.set("k1", {"a": 1})
    assert cache.get("k1") == {"a": 1}


@pytest.mark.unit
def test_get_returns_none_for_missing_key() -> None:
    assert cache.get("does-not-exist") is None


@pytest.mark.unit
def test_ttl_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    cache.set("k", "v", ttl=1)
    assert cache.get("k") == "v"
    monkeypatch.setattr(time, "time", lambda: time.time() + 5)
    assert cache.get("k") is None


@pytest.mark.unit
def test_invalidate_all_clears_short_store() -> None:
    cache.set("a", 1)
    cache.set("b", 2)
    cache.invalidate_all()
    assert cache.get("a") is None
    assert cache.get("b") is None


@pytest.mark.unit
def test_long_store_is_separate_namespace() -> None:
    cache.set("k", "short")
    cache.set_long("k", "long", ttl=60)
    assert cache.get("k") == "short"
    assert cache.get_long("k") == "long"


@pytest.mark.unit
def test_reset_for_tests_wipes_both_namespaces() -> None:
    cache.set("a", 1)
    cache.set_long("b", 2, ttl=60)
    cache._reset_for_tests()
    assert cache.get("a") is None
    assert cache.get_long("b") is None
