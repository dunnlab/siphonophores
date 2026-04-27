#!/usr/bin/env python3
"""Suggest pairings between bib entries with no `file` and unreferenced PDFs.

This is the curation-mode counterpart to ``reconcile_orphans.py`` (which
operated on the docx). The docx is pristine; the bib is the live artifact;
so reconciling against the bib is the relevant workflow now.

Inputs:
    siphonophores.bib                    — source of truth
    library/*.pdf                        — file inventory
    build/dois.json (optional)           — verified DOIs extracted from PDFs

For every bib entry with no ``file =`` field × every PDF not referenced by
any entry, we score the pair on:

    * DOI match — strongest signal. If the PDF has a Crossref-verified DOI
      that matches the entry's DOI, we call it a probable.
    * year match (exact / ±1 / ±2)
    * first-author-surname fuzzy similarity vs. the surname-shaped chunk of
      the filename
    * title fuzzy — uses any title we know for the PDF (Crossref title for
      a verified DOI, else first-page extraction)

Output: ``logs/bib_orphan_reconciliation.log`` grouped into:
    probable   — DOI match, OR year+surname+title all aligned
    maybe      — year off by 1–2 OR weaker single-signal agreement
    uncertain  — eyeball
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Optional

import bibtexparser
from pypdf import PdfReader
from rapidfuzz import fuzz

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)
BIB = REPO / "siphonophores.bib"
OUT = LOGS / "bib_orphan_reconciliation.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "reconcile_bib_orphans.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("reconcile_bib_orphans")


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def first_author_surname(author_field: str) -> str:
    """Return the first author's surname from a BibTeX `and`-separated list."""
    if not author_field:
        return ""
    first = author_field.split(" and ", 1)[0]
    if "," in first:
        return first.split(",", 1)[0].strip()
    parts = first.strip().split()
    return parts[-1] if parts else ""


def parse_pdf_filename(stem: str) -> tuple[str, Optional[int]]:
    """Return (author_chunk, year) from a PDF stem; year may be None."""
    m = re.match(r"^(.+?)(\d{4})", stem)
    if not m:
        return stem, None
    return m.group(1), int(m.group(2))


def first_page_title_lines(pdf: Path, *, max_lines: int = 25) -> list[str]:
    try:
        text = PdfReader(str(pdf)).pages[0].extract_text() or ""
    except Exception as exc:
        log.warning("could not read %s: %s", pdf.name, exc)
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


def best_title_score(entry_title: str, page_lines: list[str]) -> int:
    if not entry_title or not page_lines:
        return 0
    needle = entry_title.lower()
    best = 0
    for line in page_lines:
        s = fuzz.token_set_ratio(needle, line.lower())
        best = max(best, s)
    for n in (2, 3):
        for i in range(len(page_lines) - n + 1):
            joined = " ".join(page_lines[i:i + n]).lower()
            best = max(best, fuzz.token_set_ratio(needle, joined))
    return best


def score_pair(entry: dict, pdf_sig: dict) -> tuple[int, dict]:
    breakdown: dict = {"doi": 0, "year": 0, "surname": 0, "title": 0}

    # DOI match — strongest. Worth 200 alone if both sides agree.
    e_doi = (entry.get("doi") or "").strip().lower()
    p_doi = (pdf_sig.get("doi") or "").strip().lower()
    if e_doi and p_doi and e_doi == p_doi:
        breakdown["doi"] = 200

    # Year (no boost when DOI already matched).
    e_year = None
    if entry.get("year"):
        m = re.search(r"\d{4}", entry["year"])
        if m:
            e_year = int(m.group(0))
    p_year = pdf_sig.get("year")
    if e_year and p_year:
        diff = abs(e_year - p_year)
        if diff == 0:
            breakdown["year"] = 60
        elif diff == 1:
            breakdown["year"] = 30
        elif diff == 2:
            breakdown["year"] = 10
        elif diff <= 5:
            breakdown["year"] = 2
    elif p_year is None:
        breakdown["year"] = 5

    # Surname fuzzy.
    e_sn = fold(first_author_surname(entry.get("author", "")))
    p_chunk = fold(re.sub(r"_?et[ _]?al_?", "", pdf_sig.get("author_chunk", "")))
    if e_sn and p_chunk:
        score = max(
            fuzz.ratio(e_sn, p_chunk),
            fuzz.partial_ratio(e_sn, p_chunk[: max(8, len(e_sn))]),
        )
        breakdown["surname"] = int(score * 0.7)

    # Title fuzzy.
    e_title = (entry.get("title") or "").strip()
    p_title_lines = pdf_sig.get("page_lines") or []
    p_title = pdf_sig.get("title")
    if e_title:
        if p_title:
            t = fuzz.token_set_ratio(e_title.lower(), p_title.lower())
            breakdown["title"] = max(breakdown["title"], int(t * 0.7))
        if p_title_lines:
            breakdown["title"] = max(breakdown["title"], int(best_title_score(e_title, p_title_lines) * 0.7))

    return sum(breakdown.values()), breakdown


def bucket_for(score: int, breakdown: dict, *, has_pdf_title: bool) -> Optional[str]:
    if breakdown["doi"] >= 200:
        return "probable"
    year, surn, ttl = breakdown["year"], breakdown["surname"], breakdown["title"]
    title_supportive = ttl >= 40
    title_disagrees = has_pdf_title and ttl < 20
    if year >= 30 and surn >= 40 and (title_supportive or not has_pdf_title):
        return "probable"
    if title_supportive and surn >= 40:
        return "probable"
    if score >= 80 and (year >= 30 or ttl >= 40) and not title_disagrees:
        return "maybe"
    if score >= 60 and not title_disagrees:
        return "uncertain"
    return None


def main() -> None:
    with BIB.open() as f:
        db = bibtexparser.load(f)

    # Bib side: entries with no `file =`.
    file_less_entries = [e for e in db.entries if not (e.get("file") or "").strip()]
    referenced_files = {(e.get("file") or "").strip() for e in db.entries if (e.get("file") or "").strip()}

    # PDF side: PDFs in library/ that no bib entry points at.
    orphan_pdfs: list[Path] = []
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        if p.name in referenced_files:
            continue
        orphan_pdfs.append(p)

    log.info("bib entries with no file: %d", len(file_less_entries))
    log.info("PDFs not in any bib entry: %d", len(orphan_pdfs))

    # Optional: known DOIs per PDF basename, from extract_dois.py output.
    dois_cache: dict = {}
    p = BUILD / "dois.json"
    if p.exists():
        dois_cache = json.loads(p.read_text())

    # Build pdf signals.
    pdf_sigs: list[dict] = []
    for path in orphan_pdfs:
        author_chunk, year = parse_pdf_filename(path.stem)
        sig: dict = {
            "path": path,
            "rel": str(path.relative_to(LIBRARY)),
            "stem": path.stem,
            "author_chunk": author_chunk,
            "year": year,
            "doi": None,
            "title": None,
            "page_lines": None,
        }
        rec = dois_cache.get(path.name) or {}
        if rec.get("doi"):
            sig["doi"] = rec["doi"]
        if rec.get("title"):
            sig["title"] = rec["title"]
        # If we don't have a Crossref title from the cache, sniff page 1.
        if sig["title"] is None:
            sig["page_lines"] = first_page_title_lines(path)
        pdf_sigs.append(sig)

    # Score all pairs.
    pairs: list[tuple[int, dict, dict, dict]] = []
    for entry in file_less_entries:
        for sig in pdf_sigs:
            score, breakdown = score_pair(entry, sig)
            bucket = bucket_for(score, breakdown, has_pdf_title=bool(sig.get("title") or sig.get("page_lines")))
            if bucket is None:
                continue
            pairs.append((score, breakdown, entry, sig))

    # Top-2 per side to keep the report readable.
    by_entry: dict[int, list[tuple]] = {}
    by_pdf: dict[str, list[tuple]] = {}
    for record in pairs:
        score, _, entry, sig = record
        by_entry.setdefault(id(entry), []).append(record)
        by_pdf.setdefault(sig["rel"], []).append(record)
    keep: set[tuple[int, str]] = set()
    for cs in by_entry.values():
        cs.sort(key=lambda r: -r[0])
        for r in cs[:2]:
            keep.add((id(r[2]), r[3]["rel"]))
    for cs in by_pdf.values():
        cs.sort(key=lambda r: -r[0])
        for r in cs[:2]:
            keep.add((id(r[2]), r[3]["rel"]))
    pairs = [r for r in pairs if (id(r[2]), r[3]["rel"]) in keep]
    pairs.sort(key=lambda r: -r[0])

    buckets: dict[str, list[tuple]] = {"probable": [], "maybe": [], "uncertain": []}
    for record in pairs:
        score, bd, entry, sig = record
        b = bucket_for(score, bd, has_pdf_title=bool(sig.get("title") or sig.get("page_lines")))
        if b:
            buckets[b].append(record)

    with OUT.open("w") as fh:
        fh.write(
            f"# Bib-orphan reconciliation suggestions\n"
            f"# {len(file_less_entries)} bib entries with no file × {len(orphan_pdfs)} unreferenced PDFs\n"
            "# Confidence buckets:\n"
            "#   probable  = DOI match, OR year+surname+title all align\n"
            "#   maybe     = year off-by-one OR weaker single-signal agreement\n"
            "#   uncertain = eyeball before acting\n\n"
        )
        for bucket_name in ("probable", "maybe", "uncertain"):
            rows = buckets[bucket_name]
            fh.write(f"\n=========== {bucket_name.upper()}  ({len(rows)}) ===========\n\n")
            for score, bd, entry, sig in rows:
                fh.write(
                    f"score={score}  doi={bd['doi']} year={bd['year']} surname={bd['surname']} title={bd['title']}\n"
                )
                title = (entry.get("title") or "")[:160]
                fh.write(f"  bib:  {entry['ID']}  {entry.get('year','?')}  {title}\n")
                fh.write(f"  pdf:  {sig['rel']}")
                if sig.get("title"):
                    fh.write(f"   [title: {sig['title'][:120]}]")
                fh.write("\n\n")

    log.info(
        "wrote %s — probable=%d, maybe=%d, uncertain=%d",
        OUT,
        len(buckets["probable"]),
        len(buckets["maybe"]),
        len(buckets["uncertain"]),
    )


if __name__ == "__main__":
    main()
