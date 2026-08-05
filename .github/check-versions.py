"""Fail when the package and binary versions disagree.

The Python distribution version (PEP 440, `0.1.0a2`) and the gylo-cli crate
version (semver, `0.1.0-alpha.2`) are the same release spelled in two
grammars. Nothing else keeps them together, and getting it wrong ships a
binary whose `--version` contradicts `pip show`.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

SEMVER_PRE = {"alpha": "a", "beta": "b", "rc": "rc"}


def pep440_of(semver: str) -> str:
    base, _, pre = semver.partition("-")
    if not pre:
        return base
    label, _, number = pre.partition(".")
    if label not in SEMVER_PRE or not number.isdigit():
        raise SystemExit(f"unrecognised crate pre-release {semver!r}")
    return f"{base}{SEMVER_PRE[label]}{number}"


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    package = pyproject["project"]["version"]

    crate_toml = (root / "crates/gylo-cli/Cargo.toml").read_text()
    matched = re.search(r'^version = "([^"]+)"', crate_toml, re.M)
    if matched is None:
        raise SystemExit("no version in crates/gylo-cli/Cargo.toml")
    crate = matched.group(1)

    if pep440_of(crate) != package:
        print(
            f"version mismatch: pyproject.toml says {package}, gylo-cli says "
            f"{crate} (= {pep440_of(crate)}). They are one release in two "
            f"grammars and must move together."
        )
        return 1
    print(f"versions agree: {package} == {crate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
