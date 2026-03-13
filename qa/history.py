import json
import os
from typing import List
from qa.checks.base import QAResult, STATUS


def _history_path(folder: str) -> str:
    return os.path.join(folder, "history.json")


def load_history(folder: str) -> dict:
    path = _history_path(folder)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def update_history(date: str, score: float, results: List[QAResult], folder: str) -> None:
    os.makedirs(folder, exist_ok=True)
    history = load_history(folder)
    pass_count  = sum(1 for r in results if r.status == STATUS["PASS"])
    fail_count  = sum(1 for r in results if r.status == STATUS["FAIL"])
    warn_count  = sum(1 for r in results if r.status == STATUS["WARN"])
    error_count = sum(1 for r in results if r.status == STATUS["ERROR"])
    history[date] = {
        "score": round(score, 2),
        "total": len(results),
        "pass":  pass_count,
        "fail":  fail_count,
        "warn":  warn_count,
        "error": error_count,
    }
    with open(_history_path(folder), "w") as f:
        json.dump(history, f, indent=2)
