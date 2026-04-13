import threading
import time

_lock = threading.Lock()
_store: dict = {}  # key -> (data, expires_at_unix)

TTL = 90   # seconds (90 s; warm_cache refreshes every 60 s)


def get(key: str):
    with _lock:
        entry = _store.get(key)
        if entry and time.time() < entry[1]:
            return entry[0]
    return None


def set(key: str, data):
    with _lock:
        _store[key] = (data, time.time() + TTL)


def invalidate_all():
    with _lock:
        _store.clear()


# Long-TTL "last-known-good" cache (separate namespace, caller sets TTL).
_long_store: dict = {}  # key -> (data, expires_at_unix)


def set_long(key: str, data, ttl: int):
    with _lock:
        _long_store[key] = (data, time.time() + ttl)


def get_long(key: str):
    with _lock:
        entry = _long_store.get(key)
        if entry and time.time() < entry[1]:
            return entry[0]
    return None
