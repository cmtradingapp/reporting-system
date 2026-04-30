"""Frontend state-shape invariants — pin the bug class fixed by f04f525.

The dTradersHQ regression: a global-defaults JS object had a key referenced
at a use site (`retGlobal.dailyTradersHQ`) that wasn't declared in the
defaults literal. When the sales API responded before retention, `retGlobal`
was still the initial object (no `dailyTradersHQ`), so the read returned
`undefined` and `.toLocaleString()` crashed render.

These tests parse the template and assert that every key read off a defaults
object has a declaration in the corresponding `Object.freeze({...})` block.
Any future addition of `someObj.X` without adding `X: 0` to the defaults
will fail these tests immediately — regardless of which page or which key.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# (defaults_const_name, runtime_var_name) — extend this list when new defaults
# objects are introduced in any template file.
_INVARIANTS: list[tuple[str, str]] = [
    ("RET_GLOBAL_DEFAULTS", "retGlobal"),
    ("GLOBAL_STATS_DEFAULTS", "globalStats"),
]


_DEFAULTS_RE = re.compile(
    r"const\s+(?P<name>\w+)\s*=\s*Object\.freeze\(\s*\{(?P<body>.*?)\}\s*\)",
    re.DOTALL,
)


def _extract_default_keys(source: str, const_name: str) -> set[str]:
    for m in _DEFAULTS_RE.finditer(source):
        if m.group("name") == const_name:
            body = m.group("body")
            return set(re.findall(r"^\s*(\w+)\s*:", body, re.MULTILINE))
    return set()


def _extract_used_keys(source: str, var_name: str) -> set[str]:
    return set(re.findall(rf"\b{re.escape(var_name)}\.(\w+)", source))


@pytest.fixture
def dmp_template_source(template_dir: Path) -> str:
    return (template_dir / "daily_monthly_performance.html").read_text(encoding="utf-8")


@pytest.mark.unit
def test_dailytradershq_in_ret_global_defaults(dmp_template_source: str) -> None:
    """The exact key from f04f525. Belt-and-suspenders pin."""
    declared = _extract_default_keys(dmp_template_source, "RET_GLOBAL_DEFAULTS")
    assert "dailyTradersHQ" in declared, (
        "dailyTradersHQ missing from RET_GLOBAL_DEFAULTS — this is the bug fixed by f04f525. "
        "Adding it to the use site without the defaults causes "
        "`Cannot read properties of undefined (reading 'toLocaleString')`."
    )


@pytest.mark.unit
@pytest.mark.parametrize(("const_name", "var_name"), _INVARIANTS)
def test_defaults_cover_every_used_key(dmp_template_source: str, const_name: str, var_name: str) -> None:
    """Generalized rule: every `varName.X` read must have `X` declared in the defaults."""
    declared = _extract_default_keys(dmp_template_source, const_name)
    assert declared, f"{const_name} declaration not found — was Object.freeze removed?"
    used = _extract_used_keys(dmp_template_source, var_name)
    missing = used - declared
    assert not missing, (
        f"{var_name}.X referenced but missing from {const_name}: {sorted(missing)}. "
        f"This is the dTradersHQ bug class — undefined.toLocaleString() crashes the page. "
        f"Add the missing keys to {const_name} (with default value 0)."
    )


@pytest.mark.unit
def test_assignment_spreads_defaults(dmp_template_source: str) -> None:
    """Reassignments must spread DEFAULTS so missing API fields don't reintroduce undefined."""
    for const_name, var_name in _INVARIANTS:
        pattern = rf"{re.escape(var_name)}\s*=\s*\{{[^{{}}]*\.\.\.{re.escape(const_name)}"
        assert re.search(pattern, dmp_template_source), (
            f"Assignment to {var_name} must start with `...{const_name}` so any field "
            f"missing from the API response gracefully degrades to its default. "
            f"Without the spread, partial responses re-introduce undefined values."
        )
