#!/usr/bin/env python3
"""Detect open-access licenses from the docling full text of each paper.

Source of truth is the corpus bundle's per-document ``text.json`` (the same
docling text the MCP server serves), NOT a re-extraction from the PDFs.

Strategy (conservative — the priority is avoiding false positives):
    * The canonical signal is a ``creativecommons.org`` URL. Family + version
      are read straight from the URL path, so we never *assume* a version.
    * A prose phrase ("Creative Commons Attribution ... License") is a weaker
      signal: it fixes the family but often not the version.
    * OCR noise is normalised structurally (detect nc / nd / sa flags rather
      than string-matching an exact path) so "bync-nd/4.0" -> CC-BY-NC-ND-4.0.
    * Each candidate is tiered:
        HIGH   - a clean CC URL with family+version, and no conflicting URL.
        MEDIUM - phrase only (no resolvable version), or a lone version-less
                 URL, needing a human glance.
        CONFLICT - the document carries two different license families/versions
                 (often a reference-list citation of someone else's license);
                 always routed to manual review.
    * Every candidate keeps the matched snippet so a human can eyeball it.

Papers are matched back to ``.bib`` entries via ``metadata.json`` filename ->
the entry's ``file = {...}`` field.

Writes ``build/licenses_report.json`` and prints a summary. Applies nothing;
use the report to decide what to write to the .bib.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
BUILD = REPO / "build"
BUILD.mkdir(exist_ok=True)
REPORT = BUILD / "licenses_report.json"

# The docling text lives in the corpus repo's build output.
DOCS = Path("/Users/cdunn/repos/corpus/output/documents")

CANONICAL_URL = {
    "CC0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
}


def canonical_cc_url(spdx: str) -> str:
    if spdx in CANONICAL_URL:
        return CANONICAL_URL[spdx]
    # CC-BY-NC-ND-4.0 -> by-nc-nd/4.0
    m = re.match(r"CC-((?:BY)(?:-NC)?(?:-SA|-ND)?)-(\d\.\d)", spdx)
    if not m:
        return ""
    path = m.group(1).lower()
    return f"https://creativecommons.org/licenses/{path}/{m.group(2)}/"


def family_from_flags(code: str) -> str | None:
    """Reconstruct a canonical BY-family from a noisy URL path segment."""
    code = code.lower()
    if "by" not in code:
        return None
    parts = ["BY"]
    if "nc" in code:
        parts.append("NC")
    # nd and sa are mutually exclusive in real licenses; nd wins if both seen.
    if "nd" in code:
        parts.append("ND")
    elif "sa" in code:
        parts.append("SA")
    return "-".join(parts)


# creativecommons.org/licenses/<path>/<ver>  or  /publicdomain/zero/<ver>
# Allow stray spaces that docling/OCR injects inside the URL.
URL_RE = re.compile(
    r"creativecommons\s*\.\s*org\s*/\s*"
    r"(?:licenses\s*/\s*([a-z\-]+)|publicdomain\s*/\s*zero)"
    r"\s*/\s*(\d\s*\.\s*\d)",
    re.IGNORECASE,
)

PHRASE_RE = re.compile(
    r"creative\s+commons\s+attribution"
    r"(?P<mods>[-\s]*(?:non[-\s]*commercial|no[-\s]*deriv\w*|share[-\s]*alike|nc|nd|sa)"
    r"(?:[-\s]*(?:non[-\s]*commercial|no[-\s]*deriv\w*|share[-\s]*alike|nc|nd|sa))*)?"
    r"[^.]{0,60}",
    re.IGNORECASE,
)

VER_NEAR_RE = re.compile(r"\b(\d)\s*\.\s*(\d)\b")


def spdx_from_urls(text: str):
    """Return list of (spdx, snippet) from creativecommons URLs."""
    out = []
    for m in URL_RE.finditer(text):
        ver = re.sub(r"\s+", "", m.group(2))
        path = m.group(1)
        if path is None:  # publicdomain/zero
            spdx = f"CC0-{ver}"
        else:
            fam = family_from_flags(re.sub(r"\s+", "", path))
            if not fam:
                continue
            spdx = f"CC-{fam}-{ver}"
        snip = text[max(0, m.start() - 60): m.end() + 20].replace("\n", " ")
        out.append((spdx, re.sub(r"\s+", " ", snip).strip()))
    return out


def family_from_phrase(mods: str | None) -> str:
    if not mods:
        return "BY"
    mods = mods.lower()
    parts = ["BY"]
    if "nc" in mods or "noncommercial" in mods.replace("-", "").replace(" ", ""):
        parts.append("NC")
    flat = mods.replace("-", "").replace(" ", "")
    if "nd" in mods or "noderiv" in flat:
        parts.append("ND")
    elif "sa" in mods or "sharealike" in flat:
        parts.append("SA")
    return "-".join(parts)


def phrases(text: str):
    """Return list of (family, version_or_None, snippet) from prose."""
    out = []
    for m in PHRASE_RE.finditer(text):
        fam = family_from_phrase(m.group("mods"))
        window = text[m.start(): m.end() + 40]
        vm = VER_NEAR_RE.search(window)
        ver = f"{vm.group(1)}.{vm.group(2)}" if vm else None
        snip = re.sub(r"\s+", " ", text[max(0, m.start() - 10): m.end() + 20]).strip()
        out.append((fam, ver, snip))
    return out


def load_bib_index():
    """filename -> list of {key, journal} from the .bib (file field basename)."""
    txt = BIB.read_text(encoding="utf-8", errors="replace")
    index = {}
    licensed = set()
    # split into entries on '@type{key,'
    for m in re.finditer(r"@(\w+)\s*\{\s*([^,]+),", txt):
        start = m.end()
        nxt = txt.find("\n@", start)
        body = txt[start: nxt if nxt != -1 else len(txt)]
        key = m.group(2).strip()
        fm = re.search(r"(?im)^\s*file\s*=\s*\{([^}]+)\}", body)
        jm = re.search(r"(?im)^\s*journal\s*=\s*\{([^}]+)\}", body)
        lm = re.search(r"(?im)^\s*license\s*=\s*\{([^}]+)\}", body)
        journal = jm.group(1).strip() if jm else ""
        if fm:
            fname = fm.group(1).strip()
            index.setdefault(fname, []).append({"key": key, "journal": journal})
            if lm:
                licensed.add(fname)
    return index, licensed


def tier(spdx_url_hits, phrase_hits):
    url_spdx = {s for s, _ in spdx_url_hits}
    if url_spdx:
        if len(url_spdx) == 1:
            return "HIGH", next(iter(url_spdx))
        return "CONFLICT", sorted(url_spdx)
    # phrase only
    fams = {f for f, _, _ in phrase_hits}
    vers = {v for _, v, _ in phrase_hits if v}
    if len(fams) == 1:
        fam = next(iter(fams))
        if len(vers) == 1:
            return "MEDIUM", f"CC-{fam}-{next(iter(vers))}"
        return "MEDIUM", f"CC-{fam}-?"
    return "CONFLICT", sorted(fams)


def main():
    bib_index, licensed = load_bib_index()
    results = []
    for meta_path in sorted(DOCS.glob("*/metadata.json")):
        doc = meta_path.parent
        text_path = doc / "text.json"
        if not text_path.exists():
            continue
        try:
            text = json.loads(text_path.read_text(encoding="utf-8", errors="replace")).get("text", "")
        except Exception:
            continue
        low = text.lower()
        if "creativecommons" not in low.replace(" ", "") and "creative commons" not in low:
            continue
        meta = json.loads(meta_path.read_text(encoding="utf-8", errors="replace"))
        fname = meta.get("filename", "")

        url_hits = spdx_from_urls(text)
        phrase_hits = phrases(text)
        conf, verdict = tier(url_hits, phrase_hits)

        bib_matches = bib_index.get(fname, [])
        results.append({
            "doc_id": doc.name,
            "filename": fname,
            "confidence": conf,
            "license": verdict,
            "canonical_url": canonical_cc_url(verdict) if isinstance(verdict, str) else "",
            "already_licensed": fname in licensed,
            "bib_keys": [b["key"] for b in bib_matches],
            "journal": bib_matches[0]["journal"] if bib_matches else meta.get("journal", ""),
            "in_bib": bool(bib_matches),
            "url_snippets": [s for _, s in url_hits][:4],
            "phrase_snippets": [s for _, _, s in phrase_hits][:4],
        })

    order = {"HIGH": 0, "MEDIUM": 1, "CONFLICT": 2}
    results.sort(key=lambda r: (order.get(r["confidence"], 3), str(r["license"])))
    REPORT.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    from collections import Counter
    by_conf = Counter(r["confidence"] for r in results)
    print(f"candidates: {len(results)}   ->  {REPORT}")
    for c in ("HIGH", "MEDIUM", "CONFLICT"):
        print(f"  {c:9} {by_conf.get(c,0)}")
    print(f"  already have license field: {sum(r['already_licensed'] for r in results)}")
    print(f"  not matched to a bib entry: {sum(not r['in_bib'] for r in results)}")
    print("\nHIGH-confidence license distribution:")
    dist = Counter(r["license"] for r in results if r["confidence"] == "HIGH")
    for lic, n in dist.most_common():
        print(f"  {lic:16} {n}")


if __name__ == "__main__":
    main()
