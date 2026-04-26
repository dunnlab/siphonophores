#!/usr/bin/env python3
"""HEAD-check every URL we plan to emit in the bib.

Runs *before* :mod:`build_bib`'s URL slot is populated — but in practice we
just use it to drop URLs from already-generated DOIs / BHL records that
404. The matter of which URL to *prefer* lives in :mod:`build_bib`.

Strategy:
    * For each entry's candidate URLs (DOI, BHL part, journal URL), do a
      ``HEAD`` (falling back to a small ``GET``) and consider 2xx/3xx OK.
    * Cache results in ``build/url_status.json`` so we don't re-check on
      every run.

Output: ``build/url_status.json`` mapping URL -> {status, ok, fetched_at}.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

CACHE = BUILD / "url_status.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "verify_urls.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("verify_urls")

USER_AGENT = (
    "Mozilla/5.0 (siphonophores-bib; https://github.com/caseywdunn/siphonophores; "
    "mailto:caseywdunn@gmail.com)"
)


def candidate_urls(entry: dict, pdf_doi: dict | None, cr_record: dict | None) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add(u: str | None) -> None:
        if u and u not in seen and u.startswith(("http://", "https://")):
            urls.append(u)
            seen.add(u)

    pdf_doi_val = pdf_doi.get("doi") if pdf_doi else None
    cr_doi_val = cr_record.get("doi") if cr_record else None
    for d in (pdf_doi_val, cr_doi_val):
        if d:
            add(f"https://doi.org/{d}")
            if d.startswith("10.5962/bhl.part."):
                add(f"https://www.biodiversitylibrary.org/part/{d.split('.')[-1]}")
            elif d.startswith("10.5962/bhl.title."):
                add(f"https://www.biodiversitylibrary.org/bibliography/{d.split('.')[-1]}")
    if cr_record and cr_record.get("url"):
        add(cr_record["url"])
    return urls


def doi_resolves(doi: str, *, session: requests.Session, timeout: float = 12.0) -> bool:
    """Use the DOI Foundation handles API — doesn't depend on publisher HEAD support."""
    try:
        r = session.get(
            f"https://doi.org/api/handles/{doi}",
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )
    except requests.RequestException as exc:
        log.warning("network error for %s: %s", doi, exc)
        return False
    if r.status_code != 200:
        return False
    try:
        return r.json().get("responseCode") == 1
    except ValueError:
        return False


def head_check(url: str, *, session: requests.Session, timeout: float = 12.0) -> int:
    """For non-DOI URLs (BHL bibliography, journal homepages)."""
    try:
        r = session.head(url, allow_redirects=True, timeout=timeout, headers={"User-Agent": USER_AGENT})
        if r.status_code in (405, 403, 501):
            r = session.get(
                url,
                allow_redirects=True,
                timeout=timeout,
                headers={"User-Agent": USER_AGENT},
                stream=True,
            )
            r.close()
        return r.status_code
    except requests.RequestException as exc:
        log.warning("network error for %s: %s", url, exc)
        return 0


def check_url(url: str, *, session: requests.Session) -> tuple[int, bool]:
    """Return ``(status_code_or_marker, is_ok)`` for the URL.

    A 403 from non-DOI sites is treated as **inconclusive** rather than bad —
    BHL and many publishers sit behind Cloudflare and refuse non-browser
    HEAD/GET requests, so a 403 there usually means the URL exists but we
    can't see it. We only mark a URL bad when we get a definitive 404/410.
    """
    if url.startswith("https://doi.org/"):
        doi = url[len("https://doi.org/"):]
        ok = doi_resolves(doi, session=session)
        return (1 if ok else 100, ok)
    status = head_check(url, session=session)
    if status in (404, 410):
        return (status, False)
    if 200 <= status < 400:
        return (status, True)
    return (status, True)  # treat unknown / 403 / network errors as unverified-OK


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.05)
    args = ap.parse_args()

    entries = json.loads((BUILD / "match.json").read_text())
    pdf_dois = json.loads((BUILD / "dois.json").read_text()) if (BUILD / "dois.json").exists() else {}
    cr = (
        json.loads((BUILD / "crossref_dois.json").read_text())
        if (BUILD / "crossref_dois.json").exists()
        else {}
    )

    cache: dict[str, dict] = {}
    if CACHE.exists() and args.resume:
        cache = json.loads(CACHE.read_text())
        log.info("resuming with %d cached URL statuses", len(cache))

    # Gather unique candidate URLs across entries.
    all_urls: set[str] = set()
    for idx, e in enumerate(entries):
        pdf = pdf_dois.get(e.get("pdf_basename") or "") if e.get("pdf_basename") else None
        cr_record = cr.get(str(idx))
        for u in candidate_urls(e, pdf, cr_record):
            all_urls.add(u)

    todo = [u for u in sorted(all_urls) if u not in cache]
    if args.limit:
        todo = todo[: args.limit]
    log.info("checking %d URLs (%d cached)", len(todo), len(cache))

    session = requests.Session()
    bad = 0
    for n, url in enumerate(todo, 1):
        status, ok = check_url(url, session=session)
        cache[url] = {"status": status, "ok": ok, "fetched_at": int(time.time())}
        if not ok:
            bad += 1
            log.warning("non-OK %s for %s", status, url)
        if n % 100 == 0:
            CACHE.write_text(json.dumps(cache, indent=2))
            log.info("checkpoint: %d / %d, bad %d", n, len(todo), bad)
        time.sleep(args.sleep)

    CACHE.write_text(json.dumps(cache, indent=2))
    log.info("done: %d total in cache, %d bad", len(cache), sum(1 for v in cache.values() if not v["ok"]))


if __name__ == "__main__":
    main()
