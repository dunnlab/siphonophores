#!/usr/bin/env python3
"""Query Crossref for retroactively-assigned DOIs.

Strategy:
    For each entry that does NOT yet have a verified DOI (either because the
    PDF predates 1997 or because :mod:`extract_dois` couldn't find one), build
    a Crossref bibliographic-search query from the parsed title + first author
    surname + year. We accept a candidate iff:

        * title-similarity to our entry is >= 80% (rapidfuzz token-set ratio), AND
        * author surname appears in the Crossref author list, AND
        * issued year matches our parsed year (within ±1 to allow ePub vs print).

    Anything below these thresholds is rejected and logged. Conservative by
    design: we'd rather leave a DOI off than emit a wrong one.

Output: ``build/crossref_dois.json`` keyed on entry index, containing the DOI
plus the matched-record metadata for downstream URL/journal-name use.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
import unicodedata
from pathlib import Path
from typing import Optional

import requests
from rapidfuzz import fuzz

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

CACHE = BUILD / "crossref_dois.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "crossref_lookup.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("crossref_lookup")

USER_AGENT = "siphonophores-bib (https://github.com/caseywdunn/siphonophores; mailto:caseywdunn@gmail.com)"
SEARCH_URL = "https://api.crossref.org/works"

# Conservative thresholds — see docstring.
TITLE_THRESHOLD = 80
TITLE_THRESHOLD_STRICT = 90  # used when author surname is too common
COMMON_SURNAMES = {"smith", "lee", "kim", "wang", "li", "chen", "zhang"}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def first_surname(authors: list[str]) -> str:
    if not authors:
        return ""
    a = authors[0]
    if "," in a:
        return a.split(",", 1)[0].strip()
    return a.strip()


def cr_author_surnames(item: dict) -> list[str]:
    out = []
    for a in item.get("author", []) or []:
        if isinstance(a, dict) and a.get("family"):
            out.append(norm(a["family"]))
    return out


def cr_year(item: dict) -> Optional[int]:
    for key in ("published-print", "issued", "published-online", "created"):
        d = item.get(key, {})
        parts = (d or {}).get("date-parts") or []
        if parts and parts[0] and parts[0][0]:
            try:
                return int(parts[0][0])
            except (TypeError, ValueError):
                continue
    return None


def cr_title(item: dict) -> str:
    titles = item.get("title") or []
    return titles[0] if titles else ""


def search_crossref(
    *,
    title: str,
    author_surname: str,
    year: Optional[int],
    session: requests.Session,
    rows: int = 5,
    timeout: float = 12.0,
) -> list[dict]:
    params: dict[str, str | int] = {"rows": rows}
    q_parts = []
    if title:
        params["query.bibliographic"] = title
    if author_surname:
        params["query.author"] = author_surname
    if year:
        params["filter"] = f"from-pub-date:{year-1},until-pub-date:{year+1}"
    try:
        r = session.get(
            SEARCH_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        log.warning("network error for %s: %s", title[:60], exc)
        return []
    if r.status_code != 200:
        log.warning("crossref %d for %s", r.status_code, title[:60])
        return []
    try:
        return (r.json().get("message", {}) or {}).get("items", []) or []
    except ValueError:
        return []


def best_match(
    *,
    entry_title: str,
    entry_surname: str,
    entry_year: Optional[int],
    candidates: list[dict],
) -> tuple[Optional[dict], float, str]:
    """Pick the best candidate or return (None, score, reason)."""
    if not entry_title or not candidates:
        return None, 0.0, "no-candidates" if not candidates else "no-title"
    surname_n = norm(entry_surname)
    threshold = TITLE_THRESHOLD_STRICT if surname_n in COMMON_SURNAMES else TITLE_THRESHOLD
    title_n = norm(entry_title)
    best: tuple[Optional[dict], float, str] = (None, 0.0, "no-match")
    for item in candidates:
        cand_title_n = norm(cr_title(item))
        if not cand_title_n:
            continue
        score = fuzz.token_set_ratio(title_n, cand_title_n)
        if score < threshold:
            continue
        # Author surname must appear in the candidate's author list.
        cand_surnames = cr_author_surnames(item)
        if surname_n and not any(
            s == surname_n or s.startswith(surname_n[:5]) or surname_n.startswith(s[:5])
            for s in cand_surnames
        ):
            continue
        # Year tolerance: ±1 year (Crossref filter already enforced ±1).
        cy = cr_year(item)
        if entry_year and cy and abs(cy - entry_year) > 1:
            continue
        if score > best[1]:
            best = (item, score, "ok")
    return best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--min-year", type=int, default=1700, help="Skip entries earlier than this")
    args = ap.parse_args()

    entries = json.loads((BUILD / "match.json").read_text())
    pdf_dois = {}
    if (BUILD / "dois.json").exists():
        pdf_dois = json.loads((BUILD / "dois.json").read_text())

    cache: dict[str, dict] = {}
    if CACHE.exists() and args.resume:
        cache = json.loads(CACHE.read_text())
        log.info("resuming with %d cached crossref entries", len(cache))

    session = requests.Session()
    queried = 0
    accepted = 0
    rejected = 0

    todo = []
    for idx, e in enumerate(entries):
        if not e.get("year") or not e.get("title") or not e.get("authors"):
            continue
        try:
            yr = int(e["year"])
        except ValueError:
            continue
        if yr < args.min_year:
            continue
        key = str(idx)
        if args.resume and key in cache:
            continue
        todo.append((idx, e))

    if args.limit:
        todo = todo[: args.limit]

    log.info("querying crossref for %d entries", len(todo))

    for n, (idx, entry) in enumerate(todo, 1):
        sn = first_surname(entry["authors"])
        try:
            yr = int(entry["year"])
        except ValueError:
            continue
        items = search_crossref(
            title=entry["title"],
            author_surname=sn,
            year=yr,
            session=session,
            rows=5,
        )
        queried += 1
        match, score, reason = best_match(
            entry_title=entry["title"],
            entry_surname=sn,
            entry_year=yr,
            candidates=items,
        )
        if match:
            accepted += 1
            cr_doi = match.get("DOI", "").lower()
            cache[str(idx)] = {
                "doi": cr_doi,
                "title": cr_title(match),
                "container_title": (match.get("container-title") or [None])[0],
                "volume": match.get("volume"),
                "issue": match.get("issue"),
                "page": match.get("page"),
                "issued_year": cr_year(match),
                "url": match.get("URL"),
                "score": score,
                "method": "crossref-search",
            }
            # Cross-check against any DOI extracted from the PDF text.
            pdf_doi = (pdf_dois.get(entry.get("pdf_basename") or "", {}) or {}).get("doi")
            if pdf_doi and pdf_doi.lower() != cr_doi:
                log.warning(
                    "DOI mismatch for entry #%d %r: pdf=%s, crossref=%s",
                    idx,
                    entry["raw"][:80],
                    pdf_doi,
                    cr_doi,
                )
        else:
            rejected += 1
            cache[str(idx)] = {"doi": None, "rejected_reason": reason, "candidates": len(items)}
            log.info(
                "no-match[%s]: %s — %s",
                reason,
                entry["title"][:80],
                f"{len(items)} candidates",
            )
        time.sleep(args.sleep)
        if n % 50 == 0:
            CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
            log.info(
                "checkpoint: queried %d / %d, accepted %d, rejected %d",
                n,
                len(todo),
                accepted,
                rejected,
            )

    CACHE.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
    log.info(
        "done: queried %d, accepted %d, rejected %d (cache %d)",
        queried,
        accepted,
        rejected,
        len(cache),
    )


if __name__ == "__main__":
    main()
