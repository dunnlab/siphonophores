#!/usr/bin/env python3
"""One-shot DOI verification against Crossref.

Intended for the initial-build push, not routine validation. For each entry
with a ``doi`` field we hit ``https://api.crossref.org/works/{doi}`` and
compare the returned title / year / first-author surname against what's in
the bib. Results are bucketed:

    verified    title fuzzy >= 80, year +/-1, first-author surname matches
    suspicious  partial match — e.g. title agrees but author or year differs
    wrong       low title similarity AND author/year mismatch
    dead        DOI did not resolve (4xx)
    error       transport / parse error

Output:
    logs/verify_dois.log      human-readable detail (printed to stdout too)
    build/doi_check.json      cached Crossref responses keyed by DOI

Re-running is cheap: the cache satisfies any DOI we've seen before. Delete
``build/doi_check.json`` to force a fresh fetch.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import bibtexparser
import requests
from rapidfuzz import fuzz


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _normalize_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)              # html tags from Crossref
    s = re.sub(r"\\[a-zA-Z]+\{([^}]*)\}", r"\1", s)  # \emph{x} latex
    s = re.sub(r"[{}]", "", s)                   # bare braces
    return s

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
LOGS = REPO / "logs"
BUILD = REPO / "build"
LOGS.mkdir(exist_ok=True)
BUILD.mkdir(exist_ok=True)
OUT_LOG = LOGS / "verify_dois.log"
CACHE = BUILD / "doi_check.json"

UA = "siphonophores-bib-verifier/0.1 (mailto:caseywdunn@gmail.com)"
CROSSREF = "https://api.crossref.org/works/{doi}"
TITLE_THRESHOLD = 80  # rapidfuzz token_set_ratio


def load_bib() -> list[dict]:
    parser = bibtexparser.bparser.BibTexParser(common_strings=True)
    with BIB.open() as f:
        return bibtexparser.load(f, parser=parser).entries


def first_author_surname(author_field: str) -> str:
    if not author_field:
        return ""
    first = author_field.split(" and ")[0].strip()
    # Drop trailing "et al." so we don't pick "al" as a surname.
    first = re.sub(r"\bet\s+al\.?\s*$", "", first, flags=re.IGNORECASE).strip().rstrip(",.")
    if "," in first:
        last = first.split(",")[0]
    else:
        parts = first.split()
        last = parts[-1] if parts else ""
    return re.sub(r"[^A-Za-z]", "", _strip_accents(last)).lower()


def get_year(entry: dict) -> int | None:
    y = entry.get("year") or ""
    m = re.search(r"\d{4}", y)
    return int(m.group(0)) if m else None


def fetch_one(doi: str, session: requests.Session) -> tuple[str, int, dict | None, str | None]:
    """Return (doi, http_status, message-payload-or-None, error-or-None).

    Retries on 429 / 5xx with exponential backoff (honoring Retry-After).
    """
    backoff = 1.0
    for attempt in range(6):
        try:
            r = session.get(CROSSREF.format(doi=doi), timeout=20,
                            headers={"User-Agent": UA})
        except requests.RequestException as e:
            return (doi, 0, None, f"request failed: {e}")
        if r.status_code == 200:
            try:
                return (doi, 200, r.json().get("message"), None)
            except ValueError as e:
                return (doi, 200, None, f"json parse: {e}")
        if r.status_code == 404:
            return (doi, 404, None, None)
        if r.status_code in (429, 500, 502, 503, 504):
            wait = float(r.headers.get("Retry-After") or backoff)
            time.sleep(min(wait, 30))
            backoff = min(backoff * 2, 30)
            continue
        return (doi, r.status_code, None, f"http {r.status_code}")
    return (doi, r.status_code, None, f"http {r.status_code} after retries")


def classify(entry: dict, message: dict | None) -> tuple[str, dict]:
    """Return (bucket, detail) for one (entry, crossref-message) pair."""
    if message is None:
        return ("dead", {})

    bib_title = (entry.get("title") or "").strip()
    bib_year = get_year(entry)
    bib_surname = first_author_surname(entry.get("author") or "")

    cr_title = ""
    if message.get("title"):
        cr_title = message["title"][0] if isinstance(message["title"], list) else message["title"]
    cr_title = _normalize_title(cr_title)
    bib_title = _normalize_title(bib_title)
    cr_authors = message.get("author") or []

    def _norm_surname(s: str) -> str:
        s = re.sub(r"[^A-Za-z]", "", _strip_accents(s or "")).lower()
        # Strip honorific/suffix concatenations Crossref sometimes returns.
        for suf in ("junior", "jrr", "jr", "sr", "iii", "ii", "iv"):
            if s.endswith(suf) and len(s) > len(suf) + 2:
                s = s[: -len(suf)]
                break
        return s

    cr_surnames = {_norm_surname(a.get("family") or "")
                   for a in cr_authors if isinstance(a, dict)}
    cr_surnames.discard("")
    cr_year = None
    for k in ("issued", "published", "published-online", "published-print"):
        v = (message.get(k) or {}).get("date-parts")
        if v and v[0] and isinstance(v[0][0], int):
            cr_year = v[0][0]
            break

    title_sim = (
        fuzz.token_set_ratio(bib_title.lower(), cr_title.lower())
        if (bib_title and cr_title) else 0
    )
    title_ok = title_sim >= TITLE_THRESHOLD
    # Bib surnames sometimes mis-parsed (e.g. "EdwardsL" if author lacks
    # comma, "al" from "et al"). Substring-either-direction is forgiving.
    author_ok = bool(bib_surname) and any(
        bib_surname == s or (len(bib_surname) >= 4 and bib_surname in s)
        or (len(s) >= 4 and s in bib_surname)
        for s in cr_surnames
    )
    year_ok = bib_year is not None and cr_year is not None and abs(bib_year - cr_year) <= 1

    detail = {
        "bib_title": bib_title[:120],
        "cr_title": cr_title[:120],
        "title_sim": title_sim,
        "bib_year": bib_year,
        "cr_year": cr_year,
        "bib_surname": bib_surname,
        "cr_surnames": sorted(cr_surnames)[:6],
        "title_ok": title_ok,
        "year_ok": year_ok,
        "author_ok": author_ok,
    }

    if title_ok and year_ok and author_ok:
        return ("verified", detail)
    score = sum([title_ok, year_ok, author_ok])
    if score >= 2:
        return ("suspicious", detail)
    return ("wrong", detail)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4, help="concurrent Crossref requests")
    ap.add_argument("--no-cache", action="store_true", help="ignore cached responses")
    args = ap.parse_args()

    cache: dict[str, dict | None] = {}
    if CACHE.exists() and not args.no_cache:
        cache = json.loads(CACHE.read_text())
        # Don't trust cached 429s or transient errors; re-fetch them.
        cache = {
            d: v for d, v in cache.items()
            if v and (v.get("status") in (200, 404)) and not v.get("error")
        }

    entries = load_bib()
    with_doi = [(e["ID"], (e.get("doi") or "").strip().lower(), e) for e in entries
                if (e.get("doi") or "").strip()]

    print(f"verifying {len(with_doi)} DOIs (cache: {len(cache)} hits available)", file=sys.stderr)

    needs_fetch = [d for _, d, _ in with_doi if d not in cache]
    if needs_fetch:
        print(f"fetching {len(needs_fetch)} from Crossref...", file=sys.stderr)
        session = requests.Session()
        t0 = time.time()
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(fetch_one, d, session): d for d in needs_fetch}
            for fut in as_completed(futs):
                doi, status, msg, err = fut.result()
                cache[doi] = {"status": status, "message": msg, "error": err}
                done += 1
                if done % 50 == 0:
                    print(f"  {done}/{len(needs_fetch)} done", file=sys.stderr)
        CACHE.write_text(json.dumps(cache, indent=2))
        print(f"fetched in {time.time()-t0:.1f}s; cache -> {CACHE}", file=sys.stderr)

    buckets: dict[str, list[tuple[str, str, dict]]] = {
        "verified": [], "suspicious": [], "wrong": [], "dead": [], "error": [],
    }
    for key, doi, entry in with_doi:
        rec = cache.get(doi) or {}
        if rec.get("error"):
            buckets["error"].append((key, doi, {"err": rec["error"]}))
            continue
        if rec.get("status") == 404:
            buckets["dead"].append((key, doi, {}))
            continue
        bucket, detail = classify(entry, rec.get("message"))
        buckets[bucket].append((key, doi, detail))

    out_lines: list[str] = []
    out_lines.append("DOI verification against Crossref")
    out_lines.append(f"  total entries with DOI: {len(with_doi)}")
    for b in ("verified", "suspicious", "wrong", "dead", "error"):
        out_lines.append(f"  {b:<11} {len(buckets[b]):>5}")

    for label in ("wrong", "suspicious", "dead", "error"):
        items = buckets[label]
        if not items:
            continue
        out_lines.append("")
        out_lines.append(f"=== {label.upper()} ({len(items)}) ===")
        for key, doi, d in sorted(items):
            out_lines.append(f"  {key}  {doi}")
            if label == "error":
                out_lines.append(f"      {d.get('err', '')}")
                continue
            if label == "dead":
                continue
            out_lines.append(
                f"      title_sim={d['title_sim']}  year_ok={d['year_ok']}  author_ok={d['author_ok']}"
            )
            if d["bib_title"] != d["cr_title"]:
                out_lines.append(f"      bib title:  {d['bib_title']}")
                out_lines.append(f"      cr  title:  {d['cr_title']}")
            if not d["year_ok"]:
                out_lines.append(f"      bib year={d['bib_year']}  cr year={d['cr_year']}")
            if not d["author_ok"]:
                out_lines.append(f"      bib surname={d['bib_surname']}  cr surnames={d['cr_surnames']}")

    output = "\n".join(out_lines) + "\n"
    OUT_LOG.write_text(output)
    sys.stdout.write(output)


if __name__ == "__main__":
    main()
