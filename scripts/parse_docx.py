#!/usr/bin/env python3
"""Parse AASCANNED LITERATURE.docx into structured JSON entries.

Reads the Phil Pugh reference list and produces ``build/entries.json`` with
one record per reference. Each record holds the raw paragraph text plus
parsed fields (authors, year, title, journal/source, vol, pages, year_suffix).
The parser is best-effort: anything it can't confidently parse is left as
``None`` and the raw text is preserved so downstream scripts can recover.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

from docx import Document

REPO = Path(__file__).resolve().parents[1]
DOCX = REPO / "AASCANNED LITERATURE.docx"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "parse_docx.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("parse_docx")

SECTION_RE = re.compile(r"^[A-Z]{3}\s*[–—-]\s*\d+\s*$")
# Trailing 'scanned' / 'In Library' annotations Pugh added — strip when parsing.
TRAILING_NOTE_RE = re.compile(r"\s*\[(In Library|scanned|already in library)[^]]*\]\s*$", re.IGNORECASE)


def is_section_marker(text: str) -> bool:
    return bool(SECTION_RE.match(text.strip()))


def is_meta_paragraph(text: str) -> bool:
    """Pugh's notes about the document itself (header / footnote material)."""
    t = text.strip()
    if not t:
        return True
    if is_section_marker(t):
        return True
    low = t.lower()
    bad_starts = (
        "getting confused",
        "red indictes",
        "red indicates",
        "blue indicates",
        "actual number",
        "[excluding",
        "[l. agassiz",
        "figures in brackets",
    )
    return any(low.startswith(b) for b in bad_starts)


# Match start-of-entry: "Surname, A.B. ... YEAR." or "Surname, A.B. & Other, X. (YEAR)."
# We detect the year (possibly suffixed a/b/c) and use it as a fixed pivot.
YEAR_RE = re.compile(r"\b(1[5-9]\d\d|20\d\d)([a-z](?:[+,]?[a-z])*)?\b")


def split_authors_year(entry: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Split ``Authors YEAR[suffix]. Rest`` into (authors, year, suffix, rest).

    Tolerant of (YEAR), YEAR., YEAR, and 'Year' before period.
    """
    # Find the first plausible publication-year token
    m = YEAR_RE.search(entry)
    if not m:
        return None, None, None, entry
    year = m.group(1)
    suffix = m.group(2) or ""
    # authors is text before the year, stripped of trailing punctuation/parens
    authors = entry[: m.start()].strip()
    # Strip trailing comma/paren/space, but keep the period after last initials.
    authors = re.sub(r"[\s,(]+$", "", authors)
    # rest is after the year (and any closing paren / period that immediately follows)
    rest = entry[m.end():]
    rest = rest.lstrip(").,: \t")
    # collapse leading "." that may remain
    return authors, year, suffix, rest


def parse_authors(authors_str: str) -> list[str]:
    """Split an author block into a list of "Surname, F.M." names.

    Handles:
        "Pugh, P.R."                    -> ["Pugh, P.R."]
        "Pugh, P.R. & Mackie, G.O."     -> ["Pugh, P.R.", "Mackie, G.O."]
        "Smith, A., Jones, B. & Doe, C." -> three names
        "Smith, A.B., Jones, C." (last name lacks &) -> two names

    The strategy: split on " & " or " and " first, then on commas — but only
    where the comma is between two distinct authors (i.e. after initials).
    """
    if not authors_str:
        return []
    # Normalise non-breaking spaces, ampersands, " and "
    s = re.sub(r"\s+", " ", authors_str.replace("\xa0", " ")).strip()
    s = re.sub(r"\s+&\s+", " & ", s)
    s = re.sub(r",?\s+and\s+", " & ", s)
    # Split on & first
    chunks = [c.strip() for c in s.split("&") if c.strip()]
    authors: list[str] = []
    for chunk in chunks:
        # Within a chunk, split on commas that come after a closing initials block.
        # Pattern: "Surname, A.B., Surname2, C.D." — surname is followed by initials,
        # then comma+space introduces the next author.
        #
        # We look for ", " preceded by what looks like initials (single letters with dots),
        # and split there.
        parts = re.split(r"(?<=[A-Z]\.)(?:,\s+|\s*;\s+)(?=[A-ZÁÉÍÓÚÑÄÖÜĐØČŠŽ])", chunk)
        for p in parts:
            p = p.strip().rstrip(",;")
            if p:
                authors.append(p)
    return authors


def split_after_year(rest: str) -> tuple[Optional[str], Optional[str]]:
    """From the post-year block, split "Title. Source-info." into (title, source).

    Pugh's titles end at the first period that isn't part of a known abbreviation.
    We use a heuristic: find the first period followed by space + uppercase letter.
    """
    if not rest:
        return None, None
    # Strip trailing brackets like [In Library] etc.
    rest = TRAILING_NOTE_RE.sub("", rest).strip()
    # Find candidate end-of-title periods
    for m in re.finditer(r"\.\s+", rest):
        idx = m.end()
        if idx >= len(rest):
            break
        next_ch = rest[idx]
        if next_ch.isupper() or next_ch.isdigit() or next_ch in "“\"'":
            title = rest[: m.start()].strip()
            source = rest[idx:].strip()
            if title:
                return title, source
    # No split found: whole thing is title-ish
    return rest.strip(), None


VOL_PAGES_RES = [
    # Volume X, pages Y-Z   /  Volume(Issue), pages
    re.compile(r"\b(\d+)\s*\((\d+(?:[-–]\d+)?)\)\s*[,:]\s*(\d+\s*[-–]\s*\d+)\b"),
    re.compile(r"\b(\d+)\s*[,:]\s*(\d+\s*[-–]\s*\d+)\b"),
    re.compile(r"\bvol\.?\s*(\d+)[^,]*,\s*(\d+\s*[-–]\s*\d+)\b", re.IGNORECASE),
]


def parse_source(source: str) -> dict:
    """From "Journal name 12, 34-56." extract journal, volume, pages.

    This is purely heuristic and we keep the raw string for reference.
    """
    out: dict = {"raw": source}
    if not source:
        return out
    s = source.rstrip(".").strip()
    # Try each pattern — first that matches wins
    for r in VOL_PAGES_RES:
        m = r.search(s)
        if m:
            if r.pattern.startswith(r"\b(\d+)\s*\("):
                vol, issue, pages = m.groups()
                out["volume"] = vol
                out["number"] = issue
                out["pages"] = pages.replace("–", "--").replace("-", "--")
            else:
                vol, pages = m.groups()[:2]
                out["volume"] = vol
                out["pages"] = pages.replace("–", "--").replace("-", "--")
            journal = s[: m.start()].rstrip(", ").strip()
            out["journal"] = journal or None
            return out
    # Fallback: no vol/pages structure — could be a book
    out["journal"] = s
    return out


def parse_entry(text: str) -> dict:
    raw = text.strip()
    raw = re.sub(r"\s+", " ", raw)
    authors_str, year, year_suffix, rest = split_authors_year(raw)
    title, source = split_after_year(rest) if rest else (None, None)
    src = parse_source(source) if source else {"raw": None}
    return {
        "raw": raw,
        "authors_raw": authors_str,
        "authors": parse_authors(authors_str) if authors_str else [],
        "year": year,
        "year_suffix": year_suffix or None,
        "title": title,
        "source_raw": source,
        "journal": src.get("journal"),
        "volume": src.get("volume"),
        "number": src.get("number"),
        "pages": src.get("pages"),
    }


def main() -> None:
    doc = Document(str(DOCX))
    entries = []
    skipped_meta = 0
    for p in doc.paragraphs:
        text = p.text
        if not text.strip():
            continue
        if is_meta_paragraph(text):
            skipped_meta += 1
            continue
        parsed = parse_entry(text)
        if parsed["year"] is None:
            log.warning("no year detected: %s", parsed["raw"][:160])
        if not parsed["authors"]:
            log.warning("no authors detected: %s", parsed["raw"][:160])
        entries.append(parsed)
    log.info("parsed %d entries (skipped %d meta paragraphs)", len(entries), skipped_meta)
    out = BUILD / "entries.json"
    out.write_text(json.dumps(entries, indent=2, ensure_ascii=False))
    log.info("wrote %s", out)


if __name__ == "__main__":
    main()
