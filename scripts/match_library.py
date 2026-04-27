#!/usr/bin/env python3
"""Match parsed docx entries to PDFs in ``library/``.

The PDF filenames in Pugh's library follow a fairly consistent convention:
    Surname[_Coauthor]YEAR[a-z]?.pdf
    Surname_etalYEAR[a-z]?.pdf

Examples:
    Abreu_Noguiera1988.pdf
    Adachi_etal2017.pdf
    AgassizL1862ab.pdf

Strategy:
    1. Build a candidate-key for every PDF basename (and entry).
    2. Try exact key match first, then a relaxed match (drop case / punctuation /
       initials disambiguators like the trailing 'A'/'L' in 'AgassizA1865').
    3. Whatever's left over is reported in logs as orphans.

Output: ``build/match.json`` with for every entry the chosen PDF (or null).
Also writes ``logs/match_warnings.log`` and ``logs/orphans.log``.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from rapidfuzz import fuzz, process

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "match_library.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("match_library")


def fold(s: str) -> str:
    """Lowercase + strip diacritics + drop non-alphanumerics."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def surname(author: str) -> str:
    """Return the surname portion of "Surname, F.M." (or just the whole thing)."""
    if "," in author:
        return author.split(",", 1)[0].strip()
    return author.strip()


# Trailing forename-initial disambiguators Pugh used in filenames:
# AgassizA1863 (Alexander), AgassizL1860 (Louis). When matching we drop these.
INITIAL_TAIL_RE = re.compile(r"([a-z]+?)([a-z])(\d{4}[a-z]*)$")


TRANSLATION_TAIL_RE = re.compile(r"(tr|trans|translation|parttrans|original|originaldrawings)$", re.IGNORECASE)


def candidate_keys_for_filename(stem: str) -> list[str]:
    """Generate possible normalized keys for matching.

    Pugh's filenames sometimes have descriptive tails after a separator:
        Margulis1982a_Rudjakovia.pdf  -> the part *before* the separator is the
        Pugh2006a Kephyes.pdf            citation key; what follows is a tag.
    We strip such tails by keeping only what's up to-and-including the first
    "...YEAR[a-z]?" hit in the original stem, then fold.
    """
    keys: set[str] = set()
    m = re.match(r"^(.+?\d{4})([a-z]+)?", stem)
    head_key = None
    if m:
        head = m.group(0)
        head_key = fold(head)
        keys.add(head_key)
        suf = m.group(2) or ""
        if len(suf) > 1:
            base_no_suf = fold(m.group(1))
            for letter in suf:
                keys.add(base_no_suf + letter)
            # Some "tr"/"trans" annotations look like a suffix to the regex;
            # strip the suffix entirely so the key becomes `<head><year>`.
            if TRANSLATION_TAIL_RE.fullmatch(suf):
                keys.add(fold(m.group(1)))
    keys.add(fold(stem))
    if head_key:
        m2 = INITIAL_TAIL_RE.match(head_key)
        if m2:
            keys.add(m2.group(1) + m2.group(3))
    return sorted(k for k in keys if k)


def surname_year_key(entry: dict) -> str | None:
    """Compact surname+year key for fuzzy lookup, e.g. ``abreunogueira1988``."""
    if not entry["year"] or not entry["authors"]:
        return None
    surnames = [fold(surname(a)) for a in entry["authors"][:2] if surname(a)]
    if not surnames:
        return None
    if len(entry["authors"]) >= 3:
        return surnames[0] + "etal" + entry["year"] + (entry["year_suffix"] or "")
    return "".join(surnames) + entry["year"] + (entry["year_suffix"] or "")


def candidate_keys_for_entry(entry: dict) -> list[str]:
    """Generate possible normalized keys for a parsed entry."""
    if not entry["year"] or not entry["authors"]:
        return []
    year = entry["year"]
    suffix = (entry["year_suffix"] or "")
    authors = entry["authors"]
    # Compose surnames
    s1 = fold(surname(authors[0])) if authors else ""
    if not s1:
        return []
    keys: set[str] = set()
    if len(authors) == 1:
        keys.add(f"{s1}{year}{suffix}")
        if suffix:
            for letter in suffix:
                keys.add(f"{s1}{year}{letter}")
    elif len(authors) == 2:
        s2 = fold(surname(authors[1]))
        keys.add(f"{s1}{s2}{year}{suffix}")
        keys.add(f"{s1}_{s2}{year}{suffix}".replace("_", ""))  # already folded
        keys.add(f"{s1}etal{year}{suffix}")  # sometimes used
        if suffix:
            for letter in suffix:
                keys.add(f"{s1}{s2}{year}{letter}")
                keys.add(f"{s1}etal{year}{letter}")
    else:
        # >=3 authors: Pugh used 'etal'
        keys.add(f"{s1}etal{year}{suffix}")
        s2 = fold(surname(authors[1]))
        keys.add(f"{s1}{s2}{year}{suffix}")  # backup
        if suffix:
            for letter in suffix:
                keys.add(f"{s1}etal{year}{letter}")
                keys.add(f"{s1}{s2}{year}{letter}")
    return sorted(keys)


def main() -> None:
    entries = json.loads((BUILD / "entries.json").read_text())

    # Index PDFs: relative path under library/ -> absolute path
    pdfs: dict[str, Path] = {}
    pdf_keys: dict[str, list[Path]] = defaultdict(list)
    for p in LIBRARY.rglob("*.pdf"):
        # Skip ignored Z* sub-tree (already gitignored). Files like Z* still
        # exist on disk so we still consider them — only library/Z* directory.
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        pdfs[str(rel)] = p
        for key in candidate_keys_for_filename(p.stem):
            pdf_keys[key].append(p)

    def prefer(pool: list[Path]) -> Path:
        """Pick the most "primary" PDF from a pool of equal-keyed candidates.

        Prefer the shortest stem (no `_translation`, `_plates`, `tr` etc.) as
        the canonical PDF for the bib entry.
        """
        return sorted(pool, key=lambda p: (len(p.stem), p.stem))[0]

    # Match each entry — exact key pass
    used_pdfs: set[Path] = set()
    matched = 0
    ambiguous = 0
    for entry in entries:
        keys = candidate_keys_for_entry(entry)
        entry["match_keys"] = keys
        chosen: Path | None = None
        chosen_key: str | None = None
        for k in keys:
            cands = pdf_keys.get(k, [])
            if not cands:
                continue
            unused = [c for c in cands if c not in used_pdfs]
            pick_pool = unused or cands
            if len(pick_pool) > 1:
                first = entry["authors"][0]
                first_letter = surname(first)[:1].upper() if first else ""
                shelf_pref = [c for c in pick_pool if c.parent.name.startswith(first_letter)]
                if shelf_pref:
                    pick_pool = shelf_pref
            chosen = prefer(pick_pool)
            chosen_key = k
            if len(pick_pool) > 1 and len({c.name for c in pick_pool}) > 1:
                ambiguous += 1
                log.warning(
                    "ambiguous match for %r — picked %s among %s",
                    entry["raw"][:80],
                    chosen.name,
                    [str(c.relative_to(LIBRARY)) for c in pick_pool],
                )
            break
        if chosen:
            used_pdfs.add(chosen)
            entry["pdf"] = str(chosen.relative_to(LIBRARY))
            entry["pdf_basename"] = chosen.name
            entry["match_key_used"] = chosen_key
            entry["match_kind"] = "exact"
            matched += 1
        else:
            entry["pdf"] = None
            entry["pdf_basename"] = None
            entry["match_key_used"] = None
            entry["match_kind"] = None

    # Fuzzy pass — match by surname-year for any unmatched entries.
    # Build a pool of keys for unused PDFs that share the same year.
    unused_pool: dict[str, list[Path]] = defaultdict(list)
    for path in pdfs.values():
        if path in used_pdfs:
            continue
        m = re.match(r"^(.+?)(\d{4})([a-z]?)", path.stem)
        if not m:
            continue
        s_part, year, suffix = m.group(1), m.group(2), m.group(3)
        s_part = re.sub(r"_?et[ _]?al_?", "", s_part, flags=re.I)
        key = fold(s_part) + year + suffix
        unused_pool[key].append(path)

    fuzz_matched = 0
    pool_keys = list(unused_pool.keys())
    # Keep an index of pool keys grouped by year for fast filtering.
    by_year: dict[str, list[str]] = defaultdict(list)
    for k in pool_keys:
        m_y = re.search(r"(\d{4})", k)
        if m_y:
            by_year[m_y.group(1)].append(k)

    for entry in entries:
        if entry["pdf"] is not None:
            continue
        if not entry["year"] or not entry["authors"]:
            continue
        sy = surname_year_key(entry)
        if not sy:
            continue
        first_sn = fold(surname(entry["authors"][0]))
        candidates = by_year.get(entry["year"], [])
        # Loosely require the surname to share a 4-char prefix with the file's
        # author chunk — guards against cross-author collisions in popular years.
        if first_sn:
            candidates = [k for k in candidates if k[: min(4, len(first_sn))] == first_sn[:4]]
        nearby_used = False
        if not candidates:
            # Year off-by-one — only used when the surname match is strong.
            try:
                yr = int(entry["year"])
                for offset in (-1, 1):
                    nearby = by_year.get(str(yr + offset), [])
                    if first_sn:
                        nearby = [k for k in nearby if k[: min(4, len(first_sn))] == first_sn[:4]]
                    if nearby:
                        candidates = nearby
                        nearby_used = True
                        break
            except ValueError:
                pass
        if not candidates:
            continue
        cutoff = 85 if nearby_used else 70
        best = process.extractOne(sy, candidates, scorer=fuzz.ratio, score_cutoff=cutoff)
        if not best:
            best = process.extractOne(
                first_sn + entry["year"] + (entry["year_suffix"] or ""),
                candidates,
                scorer=fuzz.ratio,
                score_cutoff=cutoff,
            )
        if not best:
            continue
        match_key, score, _ = best
        candidate_pool = [p for p in unused_pool[match_key] if p not in used_pdfs]
        if not candidate_pool:
            continue
        chosen = prefer(candidate_pool)
        used_pdfs.add(chosen)
        entry["pdf"] = str(chosen.relative_to(LIBRARY))
        entry["pdf_basename"] = chosen.name
        entry["match_key_used"] = f"fuzzy:{match_key}"
        entry["match_kind"] = "fuzzy"
        fuzz_matched += 1
        matched += 1
        log.info("fuzzy %d: %s ~ %s -> %s", score, sy, match_key, chosen.name)

    # Orphans
    docx_orphans = [e for e in entries if e["pdf"] is None]
    pdf_orphans = sorted(
        str(p.relative_to(LIBRARY)) for p in pdfs.values() if p not in used_pdfs
    )

    log.info(
        "matched %d / %d entries (%d ambiguous, %d via fuzzy)",
        matched,
        len(entries),
        ambiguous,
        fuzz_matched,
    )
    log.info("docx-only orphans: %d", len(docx_orphans))
    log.info("pdf-only orphans: %d", len(pdf_orphans))

    # Per-orphan logs
    with (LOGS / "orphans_docx.log").open("w") as fh:
        fh.write(f"# Entries in AASCANNED LITERATURE.docx with no matched PDF ({len(docx_orphans)})\n")
        for e in docx_orphans:
            fh.write(e["raw"] + "\n")
    with (LOGS / "orphans_pdf.log").open("w") as fh:
        fh.write(f"# PDFs in library/ with no matched docx entry ({len(pdf_orphans)})\n")
        for p in pdf_orphans:
            fh.write(p + "\n")

    (BUILD / "match.json").write_text(json.dumps(entries, indent=2, ensure_ascii=False))
    log.info("wrote build/match.json, logs/orphans_docx.log, logs/orphans_pdf.log")


if __name__ == "__main__":
    main()
