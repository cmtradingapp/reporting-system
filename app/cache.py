import threading
import time

_lock = threading.Lock()
_store: dict = {}  # key -> (data, expires_at_unix)

TTL = 300  # seconds (5 min — matches ETL sync interval)


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
