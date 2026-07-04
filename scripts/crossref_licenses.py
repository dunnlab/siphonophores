#!/usr/bin/env python3
"""Sweep Crossref for Creative-Commons licenses on every DOI in the bib.

Crossref's ``message.license`` array is the authoritative, per-article record
of a work's license. But it is noisy, so we are deliberately conservative:

    * Keep ONLY ``creativecommons.org`` URLs. Publisher TDM / "standard terms"
      URLs (Elsevier, Wiley, Springer) are not open licenses.
    * Require ``content-version == "vor"`` (version of record). An ``am``
      (accepted-manuscript) CC license is a green-OA self-archiving right and
      does NOT imply the published article is CC — accepting it would be a
      false positive. ``vor`` and unspecified are kept; ``am`` / ``tdm`` are
      dropped.
    * Family + version are read straight from the URL path (never assumed),
      with the same OCR-robust flag detection used for the text sweep.

Writes ``build/crossref_licenses.json`` keyed on bib entry key:
    {key: {doi, license, url, content_version, all_cc_urls}} for hits,
    and records misses so a --resume run skips them.

    python scripts/crossref_licenses.py            # sweep all DOIs (resumable)
    python scripts/crossref_licenses.py --limit N  # cap (testing)
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_licenses import canonical_cc_url, family_from_flags  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BUILD.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)
CACHE = BUILD / "crossref_licenses.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOGS / "crossref_licenses.log", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("crossref_licenses")

USER_AGENT = "siphonophores-bib (https://github.com/caseywdunn/siphonophores; mailto:caseywdunn@gmail.com)"
CROSSREF = "https://api.crossref.org/works/"

CC_URL_RE = re.compile(
    r"creativecommons\.org/(?:licenses/([a-z\-]+)|publicdomain/zero)/(\d\.\d)",
    re.IGNORECASE,
)
ACCEPT_VERSIONS = {"vor", "unspecified", ""}  # reject "am", "tdm"


def spdx_from_cc_url(url: str):
    m = CC_URL_RE.search(url)
    if not m:
        return None
    ver, path = m.group(2), m.group(1)
    if path is None:
        return f"CC0-{ver}"
    fam = family_from_flags(path)
    return f"CC-{fam}-{ver}" if fam else None


def bib_dois():
    txt = BIB.read_text(encoding="utf-8")
    out = []
    for m in re.finditer(r"@\w+\s*\{\s*([^,]+),", txt):
        s = m.end(); n = txt.find("\n@", s)
        body = txt[s:(n if n != -1 else len(txt))]
        dm = re.search(r"(?im)^\s*doi\s*=\s*\{([^}]+)\}", body)
        lm = re.search(r"(?im)^\s*license\s*=\s*\{", body)
        if dm:
            out.append((m.group(1).strip(), dm.group(1).strip(), bool(lm)))
    return out


def query(doi: str):
    try:
        resp = requests.get(CROSSREF + requests.utils.quote(doi, safe=""),
                            headers={"User-Agent": USER_AGENT}, timeout=30)
    except requests.RequestException as exc:
        log.warning("request failed %s: %s", doi, exc)
        return "error", None
    if resp.status_code == 404:
        return "not_found", None
    if resp.status_code != 200:
        return f"http_{resp.status_code}", None
    licenses = resp.json().get("message", {}).get("license", []) or []
    all_cc = []
    best = None
    for lic in licenses:
        spdx = spdx_from_cc_url(lic.get("URL", ""))
        if not spdx:
            continue
        cv = (lic.get("content-version") or "").lower()
        all_cc.append({"spdx": spdx, "content_version": cv, "url": lic.get("URL")})
        if cv in ACCEPT_VERSIONS and best is None:
            best = {"license": spdx, "content_version": cv, "url": lic.get("URL")}
    if best is None:
        # a CC license exists but only as am/tdm -> record, do not accept
        return ("cc_but_not_vor" if all_cc else "no_cc"), {"all_cc_urls": all_cc}
    best["all_cc_urls"] = all_cc
    return "ok", best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    cache = json.loads(CACHE.read_text()) if (args.resume and CACHE.exists()) else {}
    rows = bib_dois()
    if args.limit:
        rows = rows[:args.limit]

    n_hit = n_seen = 0
    for key, doi, has_lic in rows:
        if args.resume and key in cache:
            continue
        status, data = query(doi)
        time.sleep(0.15)
        rec = {"doi": doi, "status": status, "already_licensed": has_lic}
        if data:
            rec.update(data)
        cache[key] = rec
        n_seen += 1
        if status == "ok":
            n_hit += 1
            log.info("%-32s %s (%s)", key, data["license"], data["content_version"])
        if n_seen % 50 == 0:
            CACHE.write_text(json.dumps(cache, indent=2))
            log.info("... %d queried, %d CC hits", n_seen, n_hit)

    CACHE.write_text(json.dumps(cache, indent=2))
    from collections import Counter
    st = Counter(v["status"] for v in cache.values())
    log.info("DONE. queried=%d  ->  %s", len(cache), CACHE)
    for k, v in st.most_common():
        log.info("  %-16s %d", k, v)
    licdist = Counter(v["license"] for v in cache.values() if v.get("status") == "ok")
    log.info("CC (vor) license distribution:")
    for lic, n in licdist.most_common():
        log.info("  %-16s %d", lic, n)


if __name__ == "__main__":
    main()
