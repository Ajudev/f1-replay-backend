"""Ensure FastF1 and pandas stay isolated from non-ingestion packages."""

from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN = {"fastf1", "pandas"}
PACKAGES = ("api", "services", "models", "domain")


def _iter_python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_non_ingestion_packages_do_not_import_fastf1_or_pandas() -> None:
    app_root = Path(__file__).resolve().parents[1] / "app"
    offenders: list[str] = []
    for package in PACKAGES:
        package_root = app_root / package
        for path in _iter_python_files(package_root):
            imported = _imported_modules(path)
            bad = imported & FORBIDDEN
            if bad:
                offenders.append(f"{path.relative_to(app_root.parent)}: {sorted(bad)}")
    assert offenders == []
