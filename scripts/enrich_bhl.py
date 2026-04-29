#!/usr/bin/env python3
"""Look up BHL metadata for library PDFs that show BHL/IA provenance.

Run after ``find_bhl.py``-style detection. For each library PDF whose
embedded metadata indicates a BHL or Internet-Archive origin but whose
bib entry does NOT yet carry a BHL DOI, query BHL's PublicationSearch
API and propose:

    doi       = {10.5962/bhl.title.<id>}
    url       = {https://www.biodiversitylibrary.org/bibliography/<id>}
    bhl_title = {<id>}

Output: ``build/bhl_proposals.json`` — a list of {key, proposal,
confidence, reasoning, alternatives}. We never write the bib; review
the proposals first.

Requires environment variable ``BHL_API_KEY``. Sign up at
https://www.biodiversitylibrary.org/getapikey.aspx if you don't have one.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import unicodedata
from pathlib import Path

import bibtexparser
import requests
from rapidfuzz import fuzz

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
LIBRARY = REPO / "library"
BUILD = REPO / "build"
BUILD.mkdir(exist_ok=True)
PROPOSALS = BUILD / "bhl_proposals.json"

API = "https://www.biodiversitylibrary.org/api3"
UA = "siphonophores-bhl-enrichment/0.1 (mailto:caseywdunn@gmail.com)"

# Detection signals from the PDF (mirrors find_bhl.py)
def pdf_signals(path: Path) -> dict:
    try:
        r = subprocess.run(["pdfinfo", str(path)], capture_output=True,
                           text=True, timeout=15)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {}
    if r.returncode != 0:
        return {}
    out: dict[str, str] = {}
    for line in r.stdout.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def is_bhl_pdf(meta: dict) -> bool:
    creator = (meta.get("Creator") or "").lower()
    producer = (meta.get("Producer") or "").lower()
    return ("biodiversity heritage library" in creator
            or "internet archive" in creator
            or "luradocument" in producer)


# ---- bib helpers ----
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def first_author_surname(author_field: str) -> str:
    if not author_field:
        return ""
    first = author_field.split(" and ")[0].strip()
    first = re.sub(r"\bet\s+al\.?\s*$", "", first, flags=re.IGNORECASE).strip().rstrip(",.")
    if "," in first:
        last = first.split(",")[0]
    else:
        parts = first.split()
        last = parts[-1] if parts else ""
    return re.sub(r"[^A-Za-z]", "", strip_accents(last)).lower()


def get_year(entry: dict) -> int | None:
    y = entry.get("year") or ""
    m = re.search(r"\d{4}", y)
    return int(m.group(0)) if m else None


# ---- API ----
def api_call(session: requests.Session, key: str, **params) -> dict | None:
    params["apikey"] = key
    params["format"] = "json"
    try:
        r = session.get(API, params=params, headers={"User-Agent": UA}, timeout=30)
        if r.status_code != 200:
            return {"_http": r.status_code}
        return r.json()
    except (requests.RequestException, ValueError) as e:
        return {"_error": str(e)}


def score_match(entry: dict, candidate: dict) -> tuple[float, dict]:
    """Score how well a BHL search result matches our bib entry."""
    bib_title = entry.get("title") or ""
    bib_year = get_year(entry)
    bib_surname = first_author_surname(entry.get("author") or "")

    cand_title = candidate.get("Title") or ""
    cand_year = None
    pub_date = candidate.get("PublicationDate") or ""
    m = re.search(r"\d{4}", pub_date)
    if m:
        cand_year = int(m.group(0))

    cand_surnames = set()
    for a in candidate.get("Authors") or []:
        name = a.get("Name") or ""
        if "," in name:
            last = name.split(",")[0]
        else:
            last = name.split()[-1] if name.split() else ""
        cand_surnames.add(re.sub(r"[^A-Za-z]", "", strip_accents(last)).lower())
    cand_surnames.discard("")

    title_sim = fuzz.token_set_ratio(bib_title.lower(), cand_title.lower()) if bib_title and cand_title else 0
    year_ok = (bib_year is not None and cand_year is not None
               and abs(bib_year - cand_year) <= 1)
    author_ok = bool(bib_surname) and any(
        bib_surname == s or (len(bib_surname) >= 4 and bib_surname in s)
        or (len(s) >= 4 and s in bib_surname)
        for s in cand_surnames
    )

    # composite score: title weighted highest
    score = title_sim
    if year_ok:
        score += 20
    if author_ok:
        score += 30
    return score, {
        "title_sim": title_sim,
        "year_ok": year_ok,
        "author_ok": author_ok,
        "cand_title": cand_title[:120],
        "cand_year": cand_year,
        "cand_surnames": sorted(cand_surnames)[:6],
    }


def lookup_one(session: requests.Session, key: str, entry: dict) -> dict:
    """Search BHL for an entry and return the best match (or None)."""
    title = entry.get("title") or ""
    if not title:
        return {"status": "skip", "reason": "no bib title"}

    # Strip latex artefacts before searching
    search = re.sub(r"\{|\}", "", title)
    search = re.sub(r"\s+", " ", search).strip()

    resp = api_call(session, os.environ["BHL_API_KEY"],
                    op="PublicationSearch", searchterm=search, searchtype="F")
    if not resp or resp.get("Status") != "ok":
        return {"status": "error", "reason": f"search failed: {resp}"}

    results = resp.get("Result") or []
    if not results:
        return {"status": "no_match", "reason": "empty result"}

    scored = []
    for cand in results[:30]:
        if cand.get("BHLType") not in ("Item", "Part"):
            continue
        score, detail = score_match(entry, cand)
        scored.append((score, cand, detail))
    scored.sort(key=lambda x: -x[0])

    if not scored:
        return {"status": "no_match", "reason": "no Item/Part results"}

    best_score, best, best_detail = scored[0]
    bhl_type = best.get("BHLType")

    # DOI scheme depends on level: parts get bhl.part.<PartID>, titles get bhl.title.<TitleID>.
    if bhl_type == "Part":
        bhl_id = best.get("PartID")
        if not bhl_id:
            return {"status": "no_match", "reason": "best Part has no PartID"}
        proposal = {
            "doi": f"10.5962/bhl.part.{bhl_id}",
            "url": f"https://www.biodiversitylibrary.org/part/{bhl_id}",
            "bhl_part": str(bhl_id),
        }
    else:  # Item -> use the parent Title's DOI
        bhl_id = best.get("TitleID")
        if not bhl_id:
            return {"status": "no_match", "reason": "best Item has no TitleID"}
        proposal = {
            "doi": f"10.5962/bhl.title.{bhl_id}",
            "url": f"https://www.biodiversitylibrary.org/bibliography/{bhl_id}",
            "bhl_title": str(bhl_id),
        }

    if (best_detail["title_sim"] >= 80 and best_detail["year_ok"]
            and best_detail["author_ok"]):
        confidence = "high"
    elif best_detail["title_sim"] >= 70 and (best_detail["year_ok"] or best_detail["author_ok"]):
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "status": "match",
        "confidence": confidence,
        "score": best_score,
        "match": best_detail,
        "bhl_type": bhl_type,
        "proposal": proposal,
        "alternatives": [
            {
                "type": c.get("BHLType"),
                "id": c.get("PartID") or c.get("TitleID"),
                "title": c.get("Title", "")[:80],
                "year": c.get("PublicationDate") or c.get("Date"),
                "score": s,
            }
            for s, c, _ in scored[1:5]
        ],
    }


def main() -> None:
    if "BHL_API_KEY" not in os.environ:
        sys.exit("set BHL_API_KEY env var")

    parser = bibtexparser.bparser.BibTexParser(common_strings=True)
    with BIB.open() as f:
        entries = bibtexparser.load(f, parser=parser).entries

    # Build filename -> entry index
    by_file: dict[str, dict] = {}
    for e in entries:
        fn = (e.get("file") or "").strip()
        if fn and not (e.get("doi") or "").lower().startswith("10.5962/bhl"):
            by_file[fn] = e

    # Walk library and collect candidates with BHL signals but no BHL DOI in bib
    candidates: list[tuple[Path, dict, dict]] = []
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and rel.parts[0] == "orphans":
            continue
        if p.name not in by_file:
            continue
        meta = pdf_signals(p)
        if not is_bhl_pdf(meta):
            continue
        candidates.append((p, by_file[p.name], meta))

    print(f"candidates to look up: {len(candidates)}", file=sys.stderr)

    session = requests.Session()
    proposals = []
    for i, (path, entry, meta) in enumerate(candidates, 1):
        result = lookup_one(session, entry["ID"], entry)
        result["key"] = entry["ID"]
        result["bib_title"] = (entry.get("title") or "")[:120]
        result["bib_year"] = get_year(entry)
        result["pdf_keywords"] = meta.get("Keywords", "")
        proposals.append(result)
        if i % 10 == 0:
            print(f"  {i}/{len(candidates)}", file=sys.stderr)
        time.sleep(0.7)  # be polite to BHL

    PROPOSALS.write_text(json.dumps(proposals, indent=2))
    print(f"\nwrote {PROPOSALS}", file=sys.stderr)

    # Quick summary
    by_status: dict[str, int] = {}
    by_conf: dict[str, int] = {}
    for p in proposals:
        by_status[p["status"]] = by_status.get(p["status"], 0) + 1
        if p["status"] == "match":
            by_conf[p["confidence"]] = by_conf.get(p["confidence"], 0) + 1
    print(f"\nstatus counts: {by_status}", file=sys.stderr)
    print(f"match confidence: {by_conf}", file=sys.stderr)


if __name__ == "__main__":
    main()
