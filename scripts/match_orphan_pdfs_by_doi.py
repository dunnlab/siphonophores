#!/usr/bin/env python3
"""Match unreferenced PDFs to bib entries by DOI.

For every PDF in ``library/`` that no bib entry currently references:
    1. Find its DOI: prefer the cached value in ``build/dois.json``; if no
       cached entry, extract a DOI from page 1 text (any year, not just
       post-1997) and verify it against Crossref.
    2. If a verified DOI exists, look for a bib entry whose ``doi`` field
       matches. Two outcomes:
         - **DOI in bib**: report as a high-confidence pairing; the bib
           entry just needs a ``file =`` slot to point at the PDF.
         - **DOI not in bib**: this is a new paper. Report it with the
           Crossref metadata (title, container, year, authors) so a new
           bib record can be drafted.
    3. PDFs with no extractable DOI are reported separately for manual
       review (often pre-DOI-era papers).

Output: ``logs/orphan_pdfs_by_doi.log``. Pass ``--apply-matches`` to inject
``file = {basename}`` into the matching bib entries (only the
``DOI in bib`` cases — never the ``new`` ones).
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from collections import defaultdict
from pathlib import Path

import bibtexparser
import requests
from pypdf import PdfReader

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)
BIB = REPO / "siphonophores.bib"
DOIS_CACHE = BUILD / "dois.json"
OUT = LOGS / "orphan_pdfs_by_doi.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("match_orphan_pdfs_by_doi")

USER_AGENT = (
    "siphonophores-bib (https://github.com/caseywdunn/siphonophores; "
    "mailto:caseywdunn@gmail.com)"
)
CROSSREF_WORK = "https://api.crossref.org/works/{doi}"

DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9<>]+", re.IGNORECASE)
TRAILING_PUNCT = re.compile(r"[.,;:\)\]\}\s]+$")


def clean_doi(d: str) -> str:
    d = TRAILING_PUNCT.sub("", d.strip())
    if d.endswith(")") and d.count("(") < d.count(")"):
        d = d.rstrip(")")
    return d


def extract_doi_from_text(text: str) -> str | None:
    if not text:
        return None
    flat = re.sub(r"[​‌‍﻿]", "", text)
    flat = flat.replace("–", "-").replace("—", "-")
    flat = re.sub(r"\bdoi\s*:?\s*\n?", "", flat, flags=re.IGNORECASE)
    flat = re.sub(r"https?://(dx\.)?doi\.org/", "", flat, flags=re.IGNORECASE)
    seen: list[tuple[int, str]] = []
    for m in DOI_RE.finditer(flat):
        d = clean_doi(m.group(0))
        if not (8 <= len(d) <= 200 and "/" in d):
            continue
        if d.endswith(("-", "_", ".")):
            continue
        if d.split("/", 1)[1] == "":
            continue
        seen.append((m.start(), d))
    if not seen:
        return None
    counts: dict[str, int] = {}
    first_pos: dict[str, int] = {}
    for pos, d in seen:
        counts[d] = counts.get(d, 0) + 1
        first_pos.setdefault(d, pos)
    return min(counts.keys(), key=lambda d: (first_pos[d], -counts[d]))


def page_text(pdf: Path, *, max_pages: int = 3) -> str:
    try:
        reader = PdfReader(str(pdf))
    except Exception as exc:
        log.warning("could not open %s: %s", pdf.name, exc)
        return ""
    n = len(reader.pages)
    pages = list(range(min(max_pages, n)))
    if n > max_pages:
        pages.append(n - 1)
    chunks = []
    for i in pages:
        try:
            chunks.append(reader.pages[i].extract_text() or "")
        except Exception as exc:
            log.warning("extract page %d of %s: %s", i, pdf.name, exc)
    return "\n".join(chunks)


def verify_doi(doi: str, *, session: requests.Session, timeout: float = 12.0) -> dict | None:
    try:
        r = session.get(
            CROSSREF_WORK.format(doi=requests.utils.quote(doi, safe="")),
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )
    except requests.RequestException as exc:
        log.warning("crossref network error for %s: %s", doi, exc)
        return None
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        log.warning("crossref %d for %s", r.status_code, doi)
        return None
    try:
        return r.json().get("message")
    except ValueError:
        return None


def load_bib() -> tuple[list[dict], dict[str, str]]:
    """Return ``(entries, doi_to_key)`` where doi_to_key only contains entries
    that don't already have a ``file =`` slot — those are the ones a DOI
    match could meaningfully fill in. Entries that are already file-linked
    don't need a new file pointer; if the orphan PDF has the same DOI it's
    likely a duplicate scan, not a curation gap."""
    with BIB.open() as f:
        db = bibtexparser.load(f)
    by_doi: dict[str, str] = {}
    for e in db.entries:
        d = (e.get("doi") or "").strip().lower()
        if not d:
            continue
        if (e.get("file") or "").strip():
            continue
        by_doi[d] = e["ID"]
    return db.entries, by_doi


def collect_orphan_pdfs(entries: list[dict]) -> list[Path]:
    referenced = {(e.get("file") or "").strip() for e in entries if (e.get("file") or "").strip()}
    out: list[Path] = []
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        if p.name in referenced:
            continue
        out.append(p)
    return sorted(out, key=lambda p: p.name)


def insert_file_field(text: str, key: str, fname: str) -> str:
    pattern = re.compile(
        rf"(@article\{{{re.escape(key)},\n(?:  [^\n]*\n)*?)(\}})",
        flags=re.MULTILINE,
    )
    new_text, n = pattern.subn(rf"\1  file = {{{fname}}},\n\2", text, count=1)
    if n != 1:
        raise RuntimeError(f"failed to insert file for {key} ({n} hits)")
    return new_text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-matches", action="store_true",
                    help="add file= to bib entries whose DOI matches an orphan PDF's DOI")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.05)
    args = ap.parse_args()

    entries, doi_to_key = load_bib()
    orphan_pdfs = collect_orphan_pdfs(entries)
    log.info("orphan PDFs: %d", len(orphan_pdfs))
    log.info("bib entries with DOIs: %d", len(doi_to_key))

    cache: dict = {}
    if DOIS_CACHE.exists():
        cache = json.loads(DOIS_CACHE.read_text())

    session = requests.Session()
    matches_in_bib: list[dict] = []
    new_papers: list[dict] = []
    no_doi: list[dict] = []

    items = orphan_pdfs[: args.limit] if args.limit else orphan_pdfs
    for n, pdf in enumerate(items, 1):
        rec = cache.get(pdf.name) or {}
        doi = (rec.get("doi") or None)
        verified = bool(rec.get("verified"))
        cr_meta: dict | None = None

        if not doi or not verified:
            text = page_text(pdf)
            extracted = extract_doi_from_text(text)
            if extracted:
                cr_meta = verify_doi(extracted, session=session)
                if cr_meta:
                    doi = cr_meta.get("DOI", extracted).lower()
                    cache[pdf.name] = {
                        "doi": doi,
                        "verified": True,
                        "title": (cr_meta.get("title") or [None])[0],
                        "container_title": (cr_meta.get("container-title") or [None])[0],
                        "issued_year": (cr_meta.get("issued", {}) or {}).get("date-parts", [[None]])[0][0],
                        "url": cr_meta.get("URL"),
                    }
                else:
                    cache[pdf.name] = {"doi": None, "raw_doi_in_pdf": extracted, "verified": False}
                    doi = None
                time.sleep(args.sleep)
            else:
                cache[pdf.name] = {**(cache.get(pdf.name) or {}), "doi": None}
                doi = None

        if not doi:
            no_doi.append({"pdf": pdf, "rel": str(pdf.relative_to(LIBRARY))})
            continue

        # If we don't have Crossref metadata yet (cache hit), fetch it for the report.
        if cr_meta is None and doi:
            cached_title = (cache.get(pdf.name) or {}).get("title")
            if not cached_title:
                cr_meta = verify_doi(doi, session=session)
                if cr_meta:
                    cache[pdf.name].update({
                        "title": (cr_meta.get("title") or [None])[0],
                        "container_title": (cr_meta.get("container-title") or [None])[0],
                        "issued_year": (cr_meta.get("issued", {}) or {}).get("date-parts", [[None]])[0][0],
                        "url": cr_meta.get("URL"),
                    })
                time.sleep(args.sleep)

        bib_key = doi_to_key.get(doi.lower())
        record = {
            "pdf": pdf,
            "rel": str(pdf.relative_to(LIBRARY)),
            "doi": doi,
            "title": (cache.get(pdf.name) or {}).get("title"),
            "container_title": (cache.get(pdf.name) or {}).get("container_title"),
            "issued_year": (cache.get(pdf.name) or {}).get("issued_year"),
            "url": (cache.get(pdf.name) or {}).get("url"),
        }
        if bib_key:
            record["bib_key"] = bib_key
            matches_in_bib.append(record)
        else:
            new_papers.append(record)

        if n % 25 == 0:
            DOIS_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
            log.info("checkpoint: %d / %d", n, len(items))

    DOIS_CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))

    # ---------- Report ----------
    out_lines: list[str] = []
    out_lines.append(f"# Orphan PDFs by DOI ({len(orphan_pdfs)} PDFs)")
    out_lines.append(f"# DOI matches an existing bib entry:    {len(matches_in_bib)}")
    out_lines.append(f"# DOI exists but no bib entry (new):    {len(new_papers)}")
    out_lines.append(f"# No DOI extractable:                   {len(no_doi)}")
    out_lines.append("")

    out_lines.append(f"\n=========== DOI MATCHES IN BIB ({len(matches_in_bib)}) ===========\n")
    for r in matches_in_bib:
        out_lines.append(f"{r['rel']}")
        out_lines.append(f"  doi:     {r['doi']}")
        out_lines.append(f"  bib:     {r['bib_key']}")
        if r.get("title"):
            out_lines.append(f"  title:   {r['title'][:120]}")
        out_lines.append("")

    out_lines.append(f"\n=========== NEW PAPERS — bib entry needed ({len(new_papers)}) ===========\n")
    for r in new_papers:
        out_lines.append(f"{r['rel']}")
        out_lines.append(f"  doi:     {r['doi']}")
        if r.get("title"):
            out_lines.append(f"  title:   {r['title'][:140]}")
        if r.get("container_title"):
            out_lines.append(f"  journal: {r['container_title']}")
        if r.get("issued_year"):
            out_lines.append(f"  year:    {r['issued_year']}")
        if r.get("url"):
            out_lines.append(f"  url:     {r['url']}")
        out_lines.append("")

    out_lines.append(f"\n=========== NO DOI EXTRACTED ({len(no_doi)}) ===========\n")
    for r in no_doi:
        out_lines.append(f"  {r['rel']}")

    OUT.write_text("\n".join(out_lines) + "\n")
    log.info(
        "wrote %s — matches-in-bib=%d, new=%d, no-doi=%d",
        OUT, len(matches_in_bib), len(new_papers), len(no_doi),
    )

    # ---------- Apply matches ----------
    if args.apply_matches and matches_in_bib:
        bib_text = BIB.read_text()
        applied = 0
        for r in matches_in_bib:
            try:
                bib_text = insert_file_field(bib_text, r["bib_key"], r["pdf"].name)
                applied += 1
            except RuntimeError as exc:
                log.warning("could not apply for %s: %s", r["bib_key"], exc)
        BIB.write_text(bib_text)
        log.info("applied %d file= insertions", applied)


if __name__ == "__main__":
    main()
