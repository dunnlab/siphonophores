#!/usr/bin/env python3
"""Recompute ``ocrlang`` from ``doclang`` across the bib.

``ocrlang`` is derived here rather than in the pipeline so that it stays a
literal in the bib and a directly-fingerprinted input (corpus#215). The
consequence is that improving the mapping does nothing until this is re-run —
which rewrites the bib, so the change is visible in a diff and invalidates the
affected documents' OCR. That is the point, and this is the script that closes
the loop.

**Never touches a pin naming a ``*_vert`` pack.** Vertical setting is
typesetting, which BCP-47 cannot express, so no ``doclang`` can imply it and
re-derivation would replace ``jpn_vert`` with ``jpn+eng`` — worth 0.574 against
0.246 on the pages concerned. Those seven pins are a measurement, not a
derivation, and this script leaves them alone.

Usage::

    python scripts/rederive_ocrlang.py --dry-run
    python scripts/rederive_ocrlang.py
"""
from __future__ import annotations

import argparse
import collections
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from apply_page_annotations import derive_ocrlang  # noqa: E402
from bibio import parse_bib, render_bib  # noqa: E402

BIB = REPO / "siphonophores.bib"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bib", type=Path, default=BIB)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    doc = parse_bib(a.bib.read_text(encoding="utf-8"))
    changed, kept_vert, unresolved = 0, 0, []
    moves = collections.Counter()
    for entry in doc.entries:
        doclang = entry.get("doclang")
        if not doclang:
            continue
        current = (entry.get("ocrlang") or "").strip()
        if "_vert" in current:
            kept_vert += 1
            continue
        derived = derive_ocrlang(doclang)
        if derived is None:
            unresolved.append((entry.get("file"), doclang))
            continue
        if derived != current:
            moves[f"{current or '-'} -> {derived}"] += 1
            entry.set_field("ocrlang", derived)
            changed += 1

    for m, c in moves.most_common():
        print(f"  {c:5d}  {m}")
    print(f"\n{changed} ocrlang value(s) changed; {kept_vert} vertical pin(s) left alone")
    if unresolved:
        print(f"{len(unresolved)} doclang value(s) resolved to no pack:")
        for f, d in unresolved[:10]:
            print(f"   {f}  doclang={d!r}")
    if a.dry_run:
        print("dry run, nothing written")
        return 0
    a.bib.write_text(render_bib(doc), encoding="utf-8")
    print(f"wrote {a.bib}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
