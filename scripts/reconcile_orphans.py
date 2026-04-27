#!/usr/bin/env python3
"""Suggest reconciliations between unmatched docx entries and unmatched PDFs.

After ``match_library.py`` runs, two orphan sets remain:
    * docx entries with no PDF in ``library/``
    * PDFs in ``library/`` with no docx entry pointing at them

Many of those orphans are actually the *same* paper but the original matcher
couldn't bridge the gap (PDF filename doesn't have a year; author surname has
a different transliteration; year typo on one side; etc.). This script does
a more expensive cross-product comparison and assigns each candidate pair
to one of three buckets:

    probable   — high confidence (year matches and surname+title agree)
    maybe      — medium confidence (year off-by-one OR surname is fuzzy
                 BUT another signal matches)
    uncertain  — weak signal worth a human eyeball but not auto-accept

Signals used (any combination weighted by score):
    * year match (exact / ±1 / ±2)
    * surname similarity (rapidfuzz on the first author surname vs. the
      surname-shaped chunk pulled from the PDF filename)
    * title similarity — uses the Crossref-verified title when ``dois.json``
      has one, else extracts the first big text block from the PDF's first
      page as a best-effort title

Output: ``logs/orphan_reconciliation.log`` with a report grouped by bucket.
This is advisory only — no JSON artifact is written, because applying the
reconciliations means editing ``AASCANNED LITERATURE.docx`` (renaming PDFs
or adding entries) which we don't do automatically.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

from pypdf import PdfReader
from rapidfuzz import fuzz

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "reconcile_orphans.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("reconcile_orphans")

OUT = LOGS / "orphan_reconciliation.log"


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def surname(author_field: str) -> str:
    if "," in author_field:
        return author_field.split(",", 1)[0].strip()
    return author_field.strip()


def parse_pdf_filename(stem: str) -> tuple[str, Optional[int], Optional[str]]:
    """Extract (author_chunk, year, suffix) from a PDF stem.

    Examples:
        Abreu_Noguiera1988               -> ("Abreu_Noguiera", 1988, "")
        AgassizL1862ab                    -> ("AgassizL", 1862, "ab")
        Margulis1982a_Rudjakovia          -> ("Margulis", 1982, "a")
        KingPhysalia                      -> ("KingPhysalia", None, None)
    """
    m = re.match(r"^(.+?)(\d{4})([a-z]+)?", stem)
    if not m:
        return stem, None, None
    return m.group(1), int(m.group(2)), m.group(3) or ""


def extract_first_page_title(pdf_path: Path) -> Optional[str]:
    """Best-effort: use the first long line on page 1 as the title."""
    try:
        reader = PdfReader(str(pdf_path))
        text = reader.pages[0].extract_text() or ""
    except Exception as exc:
        log.warning("could not read %s: %s", pdf_path.name, exc)
        return None
    if not text:
        return None
    # Heuristic: title is usually the first line >= 20 chars that doesn't
    # start with "doi", "received", year, etc.
    for line in (l.strip() for l in text.splitlines()):
        if len(line) < 20:
            continue
        low = line.lower()
        if any(low.startswith(p) for p in ("doi", "http", "©", "received", "accepted", "abstract", "vol")):
            continue
        if re.match(r"^\s*\d", line):
            continue
        return line[:160]
    return None


def collect_orphans() -> tuple[list[dict], list[Path]]:
    entries = json.loads((BUILD / "match.json").read_text())
    docx_orphans = [e for e in entries if e.get("pdf") is None]

    used = {LIBRARY / e["pdf"] for e in entries if e.get("pdf")}
    pdf_orphans: list[Path] = []
    for p in LIBRARY.rglob("*.pdf"):
        if p in used:
            continue
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        pdf_orphans.append(p)
    return docx_orphans, pdf_orphans


def pdf_signals(pdf: Path, dois_cache: dict, cr_cache: dict) -> dict:
    """Return everything we know about an orphan PDF."""
    author_chunk, year, suffix = parse_pdf_filename(pdf.stem)
    sig: dict = {
        "path": pdf,
        "rel": str(pdf.relative_to(LIBRARY)),
        "stem": pdf.stem,
        "author_chunk": author_chunk,
        "year": year,
        "suffix": suffix,
        "title": None,
    }
    rec = dois_cache.get(pdf.name)
    if rec and rec.get("title"):
        sig["title"] = rec["title"]
    return sig


def score_pair(entry: dict, pdf_sig: dict) -> tuple[int, dict]:
    """Score 0–200 for a candidate (entry, pdf) pair, plus signal breakdown."""
    breakdown: dict = {"year": 0, "surname": 0, "title": 0}

    # Year — strong feature.
    e_year = int(entry["year"]) if entry.get("year") else None
    p_year = pdf_sig["year"]
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
        # PDFs with no year in filename can still match if title is a strong hit.
        breakdown["year"] = 5

    # Surname — fuzzy on first author + filename author chunk.
    if entry.get("authors"):
        e_sn = fold(surname(entry["authors"][0]))
        p_chunk = fold(re.sub(r"_?et[ _]?al_?", "", pdf_sig["author_chunk"]))
        if e_sn and p_chunk:
            score = max(
                fuzz.ratio(e_sn, p_chunk),
                fuzz.partial_ratio(e_sn, p_chunk[: max(8, len(e_sn))]),
            )
            breakdown["surname"] = int(score * 0.7)  # cap at 70

    # Title — fuzzy when we have a candidate title for the PDF.
    if entry.get("title") and pdf_sig.get("title"):
        e_title = entry["title"].lower()
        p_title = pdf_sig["title"].lower()
        score = fuzz.token_set_ratio(e_title, p_title)
        breakdown["title"] = int(score * 0.7)

    return sum(breakdown.values()), breakdown


def bucket_for(score: int, breakdown: dict, *, has_pdf_title: bool) -> Optional[str]:
    """Decide which confidence bucket a score lands in.

    The title signal disambiguates the probable bucket. If we *have* a title
    from the PDF but it disagrees with the entry, the pair gets demoted —
    otherwise PDFs that share author+year (e.g. Pages_White_Rodhouse1996 vs
    Pages_Gonzalez_Gonzalez1996) tie even though only one is the real match.
    """
    year, surn, ttl = breakdown["year"], breakdown["surname"], breakdown["title"]
    title_supportive = ttl >= 40
    title_disagrees = has_pdf_title and ttl < 20

    if year >= 30 and surn >= 40 and (title_supportive or not has_pdf_title):
        return "probable"
    if title_supportive and surn >= 40:
        return "probable"  # year typos get rescued when title agrees
    if score >= 80 and (year >= 30 or ttl >= 40) and not title_disagrees:
        return "maybe"
    if score >= 60 and not title_disagrees:
        return "uncertain"
    return None


def main() -> None:
    docx_orphans, pdf_orphans = collect_orphans()
    log.info("docx orphans: %d", len(docx_orphans))
    log.info("pdf  orphans: %d", len(pdf_orphans))

    dois_cache = json.loads((BUILD / "dois.json").read_text()) if (BUILD / "dois.json").exists() else {}
    cr_cache = json.loads((BUILD / "crossref_dois.json").read_text()) if (BUILD / "crossref_dois.json").exists() else {}

    # Pull title hints for orphan PDFs — from Crossref (if we have a verified
    # DOI for the file) or from the PDF's first page as a fallback.
    pdf_sigs: list[dict] = []
    for p in pdf_orphans:
        sig = pdf_signals(p, dois_cache, cr_cache)
        if sig["title"] is None:
            sig["title"] = extract_first_page_title(p)
        pdf_sigs.append(sig)

    # Score the cross-product.
    pairs: list[tuple[int, dict, dict, dict]] = []
    for entry in docx_orphans:
        for sig in pdf_sigs:
            score, breakdown = score_pair(entry, sig)
            bucket = bucket_for(score, breakdown, has_pdf_title=bool(sig.get("title")))
            if bucket is None:
                continue
            pairs.append((score, breakdown, entry, sig))

    # For each docx entry, only keep the top-2 PDF candidates so the report
    # stays scannable; same for each PDF.
    by_entry: dict[int, list[tuple]] = {}
    by_pdf: dict[str, list[tuple]] = {}
    for score, bd, entry, sig in pairs:
        eid = id(entry)
        by_entry.setdefault(eid, []).append((score, bd, entry, sig))
        by_pdf.setdefault(sig["rel"], []).append((score, bd, entry, sig))

    keep: set[tuple[int, str]] = set()
    for cands in by_entry.values():
        cands.sort(key=lambda c: -c[0])
        for c in cands[:2]:
            keep.add((id(c[2]), c[3]["rel"]))
    for cands in by_pdf.values():
        cands.sort(key=lambda c: -c[0])
        for c in cands[:2]:
            keep.add((id(c[2]), c[3]["rel"]))

    pairs = [
        (score, bd, entry, sig)
        for score, bd, entry, sig in pairs
        if (id(entry), sig["rel"]) in keep
    ]
    pairs.sort(key=lambda r: -r[0])

    buckets: dict[str, list[tuple]] = {"probable": [], "maybe": [], "uncertain": []}
    for score, bd, entry, sig in pairs:
        b = bucket_for(score, bd, has_pdf_title=bool(sig.get("title")))
        if b:
            buckets[b].append((score, bd, entry, sig))

    with OUT.open("w") as fh:
        fh.write(
            "# Orphan reconciliation suggestions\n"
            f"# {len(docx_orphans)} docx orphans × {len(pdf_orphans)} pdf orphans\n"
            "# Confidence buckets:\n"
            "#   probable  = year match + strong surname/title agreement\n"
            "#   maybe     = year off-by-one OR surname matches with another signal\n"
            "#   uncertain = weak signal — human review recommended\n\n"
        )
        for bucket_name in ("probable", "maybe", "uncertain"):
            rows = buckets[bucket_name]
            fh.write(f"\n=========== {bucket_name.upper()}  ({len(rows)}) ===========\n\n")
            for score, bd, entry, sig in rows:
                fh.write(
                    f"score={score}  year={bd['year']} surname={bd['surname']} title={bd['title']}\n"
                )
                fh.write(f"  docx: {entry['raw'][:200]}\n")
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
