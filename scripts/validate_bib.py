#!/usr/bin/env python3
"""Validate ``siphonophores.bib`` against ``library/`` and surface issues.

This is the day-to-day curation tool now that the bib is the source of
truth. The docx / orphan-reconciliation pipeline is for *initial* import;
once you've decided what's in the bib, this script tells you whether the
bib is internally consistent and inventoried correctly against the PDFs.

Output: ``logs/validate_bib.log`` (also printed to stdout). Sections:

    SUMMARY            high-level coverage numbers
    INVENTORY GAPS     bib ↔ library/ mismatches that need fixing
    METADATA HEALTH    missing/conflicting fields
    DATA INTEGRITY     malformed DOIs, URLs, etc.
    STATS BY YEAR      decade-by-decade coverage table

Goal: a clean run is one where INVENTORY GAPS is empty and you've
explicitly accepted whatever METADATA HEALTH issues remain.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import bibtexparser

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)
BIB = REPO / "siphonophores.bib"
OUT = LOGS / "validate_bib.log"

DOI_RE = re.compile(r"^10\.\d{4,9}/.+$")
URL_RE = re.compile(r"^https?://[^\s]+$")


def collect_library_pdfs() -> set[str]:
    """All PDF basenames in library/ except the gitignored Z* shelf."""
    out: set[str] = set()
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        out.add(p.name)
    return out


def pdf_paths_by_basename() -> dict[str, list[Path]]:
    """basename → list of full paths (catches duplicate basenames in subdirs)."""
    out: dict[str, list[Path]] = defaultdict(list)
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        out[p.name].append(p)
    return out


def load_bib() -> list[dict]:
    parser = bibtexparser.bparser.BibTexParser(common_strings=True)
    with BIB.open() as f:
        db = bibtexparser.load(f, parser=parser)
    return db.entries


def get_year(entry: dict) -> int | None:
    y = entry.get("year")
    if not y:
        return None
    m = re.search(r"\d{4}", y)
    return int(m.group(0)) if m else None


def report(lines: list[str], heading: str | None = None) -> str:
    if heading:
        body = f"\n{'=' * 76}\n{heading}\n{'=' * 76}\n"
    else:
        body = ""
    return body + "\n".join(lines) + ("\n" if lines else "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="list every offender, not just counts")
    args = ap.parse_args()

    entries = load_bib()
    lib_pdfs = collect_library_pdfs()
    paths_by_name = pdf_paths_by_basename()

    out_lines: list[str] = [
        f"siphonophores.bib validation",
        f"  bib: {BIB}",
        f"  library: {LIBRARY}",
        f"  records:  {len(entries)}",
        f"  pdfs:     {len(lib_pdfs)} (excluding library/Z*)",
    ]

    # ----- 1. Index bib by file basename -----
    bib_by_file: dict[str, list[dict]] = defaultdict(list)
    bib_no_file: list[dict] = []
    for e in entries:
        f = (e.get("file") or "").strip()
        if not f:
            bib_no_file.append(e)
        else:
            bib_by_file[f].append(e)

    # ----- 2. Inventory gaps -----
    gaps_lines: list[str] = []

    bib_referenced = set(bib_by_file.keys())
    missing_files = sorted(name for name in bib_referenced if name not in lib_pdfs)
    orphan_pdfs = sorted(name for name in lib_pdfs if name not in bib_referenced)
    multi_referenced = {name: ents for name, ents in bib_by_file.items() if len(ents) > 1}
    duplicate_basenames = {name: paths for name, paths in paths_by_name.items() if len(paths) > 1}

    gaps_lines.append(f"bib entries with no `file` field:                    {len(bib_no_file)}")
    gaps_lines.append(f"bib `file` references that don't exist on disk:      {len(missing_files)}")
    gaps_lines.append(f"PDFs in library/ not referenced by any bib entry:    {len(orphan_pdfs)}")
    gaps_lines.append(f"PDFs referenced by more than one bib entry:          {len(multi_referenced)}")
    gaps_lines.append(f"PDF basenames that exist in multiple library/ subdirs: {len(duplicate_basenames)}")

    if args.full or len(missing_files) <= 20:
        if missing_files:
            gaps_lines.append("\n  -- bib `file` referring to missing PDFs:")
            for name in missing_files:
                ents = bib_by_file[name]
                key = ents[0].get("ID", "?")
                gaps_lines.append(f"     {name}  (cited by {key})")
    if args.full or len(orphan_pdfs) <= 30:
        if orphan_pdfs:
            gaps_lines.append("\n  -- PDFs not referenced by any bib entry:")
            for name in orphan_pdfs:
                # show subdir to disambiguate
                paths = paths_by_name[name]
                gaps_lines.append(f"     {paths[0].relative_to(LIBRARY)}")
    if multi_referenced:
        gaps_lines.append("\n  -- PDFs cited by multiple bib entries:")
        for name, ents in sorted(multi_referenced.items()):
            keys = ", ".join(e.get("ID", "?") for e in ents)
            gaps_lines.append(f"     {name}  -> {keys}")
    if duplicate_basenames:
        gaps_lines.append("\n  -- PDF basenames present in multiple subdirs:")
        for name, paths in sorted(duplicate_basenames.items()):
            gaps_lines.append(f"     {name}: {[str(p.relative_to(LIBRARY)) for p in paths]}")

    out_lines.append(report(gaps_lines, "INVENTORY GAPS"))

    # ----- 3. Metadata health -----
    health_lines: list[str] = []

    # Duplicate citation keys
    id_counts = Counter(e["ID"] for e in entries)
    dup_keys = {k: n for k, n in id_counts.items() if n > 1}

    # Required-ish fields
    missing_author = [e for e in entries if not (e.get("author") or "").strip()]
    missing_year = [e for e in entries if get_year(e) is None]
    missing_title = [e for e in entries if not (e.get("title") or "").strip()]
    missing_journal = [e for e in entries if not (e.get("journal") or "").strip()]

    # No-DOI in modern era
    modern_no_doi = [
        e for e in entries
        if (yr := get_year(e)) is not None and yr >= 2000 and not (e.get("doi") or "").strip()
    ]

    # No URL anywhere (no doi, no url field)
    no_url_no_doi = [
        e for e in entries
        if not (e.get("doi") or "").strip() and not (e.get("url") or "").strip()
    ]

    health_lines.append(f"duplicate citation keys:                {len(dup_keys)}")
    health_lines.append(f"missing author:                         {len(missing_author)}")
    health_lines.append(f"missing year:                           {len(missing_year)}")
    health_lines.append(f"missing title:                          {len(missing_title)}")
    health_lines.append(f"missing journal/source:                 {len(missing_journal)}")
    health_lines.append(f"entries from 2000+ with no DOI:         {len(modern_no_doi)}")
    health_lines.append(f"entries with neither DOI nor URL:       {len(no_url_no_doi)}")

    if dup_keys:
        health_lines.append("\n  -- duplicate keys:")
        for k, n in sorted(dup_keys.items()):
            health_lines.append(f"     {k}  ({n}x)")
    if args.full or len(missing_year) <= 10:
        if missing_year:
            health_lines.append("\n  -- entries with no year:")
            for e in missing_year:
                health_lines.append(f"     {e['ID']}: {(e.get('title') or '')[:80]}")
    if args.full or len(missing_journal) <= 10:
        if missing_journal:
            health_lines.append("\n  -- entries with no journal/source:")
            for e in missing_journal:
                health_lines.append(f"     {e['ID']}: {(e.get('title') or '')[:80]}")
    if args.full and modern_no_doi:
        health_lines.append("\n  -- 2000+ entries with no DOI:")
        for e in modern_no_doi:
            yr = get_year(e)
            health_lines.append(f"     {e['ID']} ({yr}): {(e.get('title') or '')[:80]}")

    out_lines.append(report(health_lines, "METADATA HEALTH"))

    # ----- 4. Data integrity -----
    integrity_lines: list[str] = []

    bad_dois: list[tuple[str, str]] = []
    bad_urls: list[tuple[str, str]] = []
    doi_url_mismatch: list[str] = []
    for e in entries:
        doi = (e.get("doi") or "").strip()
        url = (e.get("url") or "").strip()
        if doi and not DOI_RE.match(doi):
            bad_dois.append((e["ID"], doi))
        if url and not URL_RE.match(url):
            bad_urls.append((e["ID"], url))
        if doi and url and url.startswith("https://doi.org/") and url[len("https://doi.org/"):] != doi:
            doi_url_mismatch.append(e["ID"])

    integrity_lines.append(f"malformed DOIs:                         {len(bad_dois)}")
    integrity_lines.append(f"malformed URLs:                         {len(bad_urls)}")
    integrity_lines.append(f"doi.org URL doesn't match `doi` field:  {len(doi_url_mismatch)}")

    for label, items in (("malformed DOIs", bad_dois), ("malformed URLs", bad_urls)):
        if items and (args.full or len(items) <= 10):
            integrity_lines.append(f"\n  -- {label}:")
            for k, v in items:
                integrity_lines.append(f"     {k}: {v}")
    if doi_url_mismatch and (args.full or len(doi_url_mismatch) <= 10):
        integrity_lines.append("\n  -- DOI/URL mismatches:")
        for k in doi_url_mismatch:
            integrity_lines.append(f"     {k}")

    out_lines.append(report(integrity_lines, "DATA INTEGRITY"))

    # ----- 5. Year stats -----
    by_decade: dict[str, dict[str, int]] = defaultdict(lambda: dict(total=0, with_doi=0, with_file=0, with_url=0))
    for e in entries:
        yr = get_year(e)
        if yr is None:
            decade = "unknown"
        elif yr < 1800:
            decade = "<1800"
        else:
            decade = f"{yr // 10 * 10}s"
        bucket = by_decade[decade]
        bucket["total"] += 1
        if (e.get("doi") or "").strip():
            bucket["with_doi"] += 1
        if (e.get("file") or "").strip():
            bucket["with_file"] += 1
        if (e.get("url") or "").strip():
            bucket["with_url"] += 1

    def decade_key(d: str) -> tuple[int, str]:
        if d == "unknown":
            return (10000, d)
        if d == "<1800":
            return (-1, d)
        return (int(d[:-1]), d)

    stats_lines: list[str] = []
    stats_lines.append(f"  {'decade':<8} {'records':>8} {'with file':>10} {'with doi':>10} {'with url':>10}")
    for d in sorted(by_decade.keys(), key=decade_key):
        b = by_decade[d]
        stats_lines.append(
            f"  {d:<8} {b['total']:>8} "
            f"{b['with_file']:>10} {b['with_doi']:>10} {b['with_url']:>10}"
        )

    out_lines.append(report(stats_lines, "STATS BY DECADE"))

    output = "\n".join(out_lines) + "\n"
    OUT.write_text(output)
    sys.stdout.write(output)


if __name__ == "__main__":
    main()
