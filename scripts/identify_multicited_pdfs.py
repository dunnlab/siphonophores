#!/usr/bin/env python3
"""For each PDF cited by >1 bib entry, decide which entry the PDF actually is.

Reads the bib + library/, finds every basename referenced by 2+ records,
extracts a title from the PDF's first page, and fuzzy-matches that title
against each candidate record's title. Reports the result so a human can
fix up the bib (keep the matching record's `file =` field, drop the
others' `file =`, and add separate PDFs for the unmatched papers).

Output: ``logs/multicited_pdfs.log``.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path

import bibtexparser
from pypdf import PdfReader
from rapidfuzz import fuzz

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)
BIB = REPO / "siphonophores.bib"
OUT = LOGS / "multicited_pdfs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("identify_multicited")


def first_page_text(pdf: Path) -> str:
    try:
        return PdfReader(str(pdf)).pages[0].extract_text() or ""
    except Exception as exc:
        log.warning("could not read %s: %s", pdf.name, exc)
        return ""


def candidate_title_lines(text: str, *, max_lines: int = 25) -> list[str]:
    """Return the first ``max_lines`` plausible title-ish lines from page 1.

    We don't try to find *the* title — fuzzy-matching against many short
    snippets is fine because token_set_ratio is forgiving.
    """
    if not text:
        return []
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if len(line) < 8:
            continue
        low = line.lower()
        if any(low.startswith(p) for p in ("doi", "http", "©", "received", "accepted", "abstract")):
            continue
        lines.append(line)
        if len(lines) >= max_lines:
            break
    return lines


def best_score(entry_title: str, page_lines: list[str]) -> int:
    """Highest token-set-ratio between the entry title and any page-1 line.

    Also tries a sliding window of joined lines so that titles split across
    multiple lines still match.
    """
    if not entry_title or not page_lines:
        return 0
    needle = entry_title.lower()
    best = 0
    for line in page_lines:
        s = fuzz.token_set_ratio(needle, line.lower())
        if s > best:
            best = s
    # Sliding 2-line and 3-line windows
    for n in (2, 3):
        for i in range(len(page_lines) - n + 1):
            joined = " ".join(page_lines[i:i + n]).lower()
            s = fuzz.token_set_ratio(needle, joined)
            if s > best:
                best = s
    return best


def main() -> None:
    with BIB.open() as f:
        db = bibtexparser.load(f)
    by_file: dict[str, list[dict]] = defaultdict(list)
    for e in db.entries:
        f = (e.get("file") or "").strip()
        if f:
            by_file[f].append(e)

    multi = {name: ents for name, ents in by_file.items() if len(ents) > 1}

    # Locate the PDF on disk.
    paths_by_name: dict[str, Path] = {}
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        paths_by_name.setdefault(p.name, p)

    out_lines: list[str] = []
    out_lines.append(f"# Multi-cited PDF inspection ({len(multi)} files)")
    out_lines.append("# For each PDF: scores each candidate record's title against page 1.")
    out_lines.append("# Higher score = more likely match. >= 80 = strong match.")
    out_lines.append("")

    for name in sorted(multi):
        ents = multi[name]
        path = paths_by_name.get(name)
        out_lines.append(f"--- {name} ({len(ents)} candidates) ---")
        if not path:
            out_lines.append("  (PDF not found on disk)")
            out_lines.append("")
            continue
        page_lines = candidate_title_lines(first_page_text(path))
        snippet = " | ".join(page_lines[:3])[:180]
        out_lines.append(f"  page-1 snippet: {snippet}")
        scored = []
        for e in ents:
            t = (e.get("title") or "").strip()
            score = best_score(t, page_lines)
            scored.append((score, e))
        scored.sort(key=lambda r: -r[0])
        for score, e in scored:
            t = (e.get("title") or "")[:90]
            j = (e.get("journal") or "")[:60]
            out_lines.append(f"  [{score:>3}]  {e['ID']}: {t}  ({j})")
        out_lines.append("")

    OUT.write_text("\n".join(out_lines))
    print("\n".join(out_lines))
    log.info("wrote %s", OUT)


if __name__ == "__main__":
    main()
