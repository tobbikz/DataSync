#!/usr/bin/env python3
"""Fail if embedded prod_ops SQL has nested bare $$ inside DO $$ blocks."""

from __future__ import annotations

import re
import sys
from pathlib import Path

HEADER = Path(__file__).resolve().parents[1] / "cpp/include/prod_ops_embedded.hpp"

SECTIONS = (
    ("schema_patches", "PO_schema_patch"),
    ("datasync_incremental", "PO_datasync_inc"),
    ("monitoring_views", "PO_monitoring_v"),
    ("datasync_baseline", "PO_datasync_bas"),
    ("datalake_lake", "PO_datalake_lak"),
)


def extract_section(text: str, section: str, marker: str) -> str:
    pattern = rf'inline std::string_view {section}\(\) \{{\s*return R"{marker}\((.*?)\){marker}";'
    match = re.search(pattern, text, re.S)
    return match.group(1) if match else ""


def find_issues(sql: str, section: str) -> list[str]:
    out: list[str] = []
    for match in re.finditer(r"DO \$\$(.*?)END \$\$;", sql, re.S):
        body = match.group(1)
        line = sql[: match.start()].count("\n") + 1
        if re.search(r"AS \$\$", body):
            out.append(f"{section}:{line}: CREATE FUNCTION uses AS $$ inside DO $$ (use tagged delimiter)")
        if re.search(r"^\s*\$\$;", body, re.M):
            out.append(f"{section}:{line}: bare $$; closes function body inside DO $$ block")
    return out


def main() -> int:
    text = HEADER.read_text(encoding="utf-8")
    issues: list[str] = []
    for section, marker in SECTIONS:
        sql = extract_section(text, section, marker)
        if sql:
            issues.extend(find_issues(sql, section))
    if issues:
        print("embedded SQL dollar-quote issues:", file=sys.stderr)
        for issue in issues:
            print(f"  {issue}", file=sys.stderr)
        return 1
    print("OK: no nested bare $$ inside DO $$ blocks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
