#!/usr/bin/env python3
"""Extract DOIs from PDFs in the library.

Strategy:
    * Only attempt extraction for papers with year >= 1997 (DOIs were
      not regularly assigned to papers before then).
    * Read the first 3 pages and last page of each PDF — DOIs typically
      appear on the first page, occasionally in the back-matter / footer.
    * Match against a conservative DOI regex; reject obvious decoys.
    * Verify each candidate against the Crossref API.
    * Write ``build/dois.json`` keyed on the PDF basename, with the verified
      DOI plus the Crossref title/journal/year/volume/page metadata.

Run options::

    python scripts/extract_dois.py            # process all matched PDFs
    python scripts/extract_dois.py --resume   # skip basenames already in dois.json
    python scripts/extract_dois.py --limit N  # cap the number of PDFs (testing)
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

import requests
from pypdf import PdfReader

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

CACHE = BUILD / "dois.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "extract_dois.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("extract_dois")

# Pre-1997: DOIs were rare. Skip earlier papers per project guidance.
MIN_YEAR_FOR_DOI = 1997

# Match a DOI like 10.1234/foo.bar — the trailing-character set is what causes
# the headaches. We grab a generous run and clean up the tail afterwards.
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9<>]+", re.IGNORECASE)
# Trailing punctuation we should strip (commas, periods at end of sentence,
# closing parens that don't have a matching open in the DOI).
TRAILING_PUNCT = re.compile(r"[.,;:\)\]\}\s]+$")

USER_AGENT = "siphonophores-bib (https://github.com/caseywdunn/siphonophores; mailto:caseywdunn@gmail.com)"

CROSSREF_WORK = "https://api.crossref.org/works/{doi}"


def clean_doi(d: str) -> str:
    d = d.strip()
    d = TRAILING_PUNCT.sub("", d)
    # Sometimes the regex grabs trailing closing paren without matching open.
    if d.endswith(")") and d.count("(") < d.count(")"):
        d = d.rstrip(")")
    if d.endswith(">") and d.count("<") < d.count(">"):
        d = d.rstrip(">")
    return d


def first_doi_in(text: str) -> Optional[str]:
    if not text:
        return None
    flat = re.sub(r"[​‌‍﻿]", "", text)
    flat = flat.replace("–", "-").replace("—", "-")
    flat = re.sub(r"\bdoi\s*:?\s*\n?", "", flat, flags=re.IGNORECASE)
    flat = re.sub(r"https?://(dx\.)?doi\.org/", "", flat, flags=re.IGNORECASE)
    seen: list[tuple[int, str]] = []  # (offset_in_text, doi)
    for m in DOI_RE.finditer(flat):
        d = clean_doi(m.group(0))
        if not (8 <= len(d) <= 200 and "/" in d):
            continue
        # Reject obviously truncated DOIs.
        if d.endswith(("-", "_", ".")):
            continue
        # Crossref DOIs always have something after the slash.
        if d.split("/", 1)[1] == "":
            continue
        seen.append((m.start(), d))
    if not seen:
        return None
    # Group identical DOIs and keep only the most-mentioned one (a paper's true
    # DOI usually appears in header + footer + references).
    counts: dict[str, int] = {}
    first_pos: dict[str, int] = {}
    for pos, d in seen:
        counts[d] = counts.get(d, 0) + 1
        first_pos.setdefault(d, pos)
    # Prefer the DOI that appears earliest in the text; tie-break by frequency.
    return min(counts.keys(), key=lambda d: (first_pos[d], -counts[d]))


def extract_text(pdf: Path, max_pages: int = 3) -> str:
    try:
        reader = PdfReader(str(pdf))
    except Exception as exc:
        log.warning("could not open %s: %s", pdf.name, exc)
        return ""
    n = len(reader.pages)
    pages_to_read = list(range(min(max_pages, n)))
    if n > max_pages:
        pages_to_read.append(n - 1)  # also try the last page
    chunks = []
    for i in pages_to_read:
        try:
            chunks.append(reader.pages[i].extract_text() or "")
        except Exception as exc:
            log.warning("extract page %d of %s: %s", i, pdf.name, exc)
    return "\n".join(chunks)


def verify_doi(doi: str, *, session: requests.Session, timeout: float = 12.0) -> Optional[dict]:
    """Look up the DOI on Crossref. Return the message dict, or None."""
    url = CROSSREF_WORK.format(doi=requests.utils.quote(doi, safe=""))
    try:
        r = session.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true", help="skip PDFs already in cache")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.05, help="delay between Crossref calls")
    args = ap.parse_args()

    entries = json.loads((BUILD / "match.json").read_text())

    cache: dict[str, dict] = {}
    if CACHE.exists() and args.resume:
        cache = json.loads(CACHE.read_text())
        log.info("resuming with %d cached DOIs", len(cache))

    session = requests.Session()
    processed = 0
    found = 0
    verified = 0

    # Build a unique list of (pdf, year) pairs so duplicate-PDF entries don't
    # cause us to call Crossref twice.
    seen_pdfs: dict[str, int] = {}
    for e in entries:
        pdf = e.get("pdf")
        if not pdf:
            continue
        try:
            year = int(e["year"]) if e.get("year") else 0
        except ValueError:
            year = 0
        if pdf not in seen_pdfs or year > seen_pdfs[pdf]:
            seen_pdfs[pdf] = year

    items = list(seen_pdfs.items())
    if args.limit:
        items = items[: args.limit]

    for pdf_rel, year in items:
        pdf_path = LIBRARY / pdf_rel
        basename = pdf_path.name

        if args.resume and basename in cache:
            continue

        if year and year < MIN_YEAR_FOR_DOI:
            cache[basename] = {"doi": None, "skipped": "pre-1997", "year": year}
            continue

        if not pdf_path.exists():
            log.warning("missing pdf: %s", pdf_rel)
            continue

        text = extract_text(pdf_path)
        doi = first_doi_in(text)
        processed += 1
        if not doi:
            cache[basename] = {"doi": None, "year": year}
            continue
        found += 1
        msg = verify_doi(doi, session=session)
        if msg is None:
            log.warning("could not verify DOI %s in %s", doi, basename)
            cache[basename] = {"doi": None, "raw_doi_in_pdf": doi, "year": year, "verified": False}
        else:
            verified += 1
            cache[basename] = {
                "doi": msg.get("DOI", doi).lower(),
                "year": year,
                "verified": True,
                "title": (msg.get("title") or [None])[0],
                "container_title": (msg.get("container-title") or [None])[0],
                "volume": msg.get("volume"),
                "issue": msg.get("issue"),
                "page": msg.get("page"),
                "issued_year": (msg.get("issued", {}) or {}).get("date-parts", [[None]])[0][0],
                "url": msg.get("URL"),
            }
        time.sleep(args.sleep)

        if processed % 50 == 0:
            CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
            log.info(
                "checkpoint: processed %d, doi-found %d, verified %d",
                processed,
                found,
                verified,
            )

    CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
    log.info(
        "done: processed %d, doi-found %d, verified %d (cache %d)",
        processed,
        found,
        verified,
        len(cache),
    )


if __name__ == "__main__":
    main()
