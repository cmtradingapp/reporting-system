"""Sanity checks for the test infra itself."""

from __future__ import annotations

import os

import pytest


@pytest.mark.unit
def test_testing_env_is_set() -> None:
    """conftest must set TESTING=1 before app import to skip APScheduler."""
    assert os.environ.get("TESTING") == "1"


@pytest.mark.unit
def test_repo_root_resolves(repo_root) -> None:
    assert (repo_root / "app").is_dir()
    assert (repo_root / "app" / "main.py").is_file()


@pytest.mark.unit
def test_template_dir_resolves(template_dir) -> None:
    assert (template_dir / "daily_monthly_performance.html").is_file()
