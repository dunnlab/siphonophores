#!/usr/bin/env python3
"""Write license / licenseurl fields into siphonophores.bib.

Merges two conservative evidence sources and applies only where they are
unambiguous:

    TEXT     - build/licenses_report.json (docling full-text sweep).
               HIGH = a creativecommons URL in the article body.
               MEDIUM = CC-BY prose; version may be explicit or "-?" (unknown).
               CONFLICT = multiple families (usually reused-figure credits).
    CROSSREF - build/crossref_licenses.json. status "ok" == a version-of-record
               Creative-Commons license registered on the DOI (authoritative).

Decision per entry (skip any that already has a license field):

    * Crossref ok AND text agree on family  -> apply Crossref (vor, versioned).
    * Crossref ok, families DIFFER from text -> manual (do not guess).
    * Crossref ok, no text signal            -> apply Crossref.
    * Text HIGH (URL in body), Crossref not vor/absent -> apply text
      (the published article itself states the CC URL).
    * Text explicit-version prose, no Crossref -> apply text.
    * Text version-unknown ("-?") and Crossref didn't resolve -> manual.
    * Text CONFLICT with no Crossref vor      -> manual.

    python scripts/apply_licenses.py --dry-run   # show plan, write nothing
    python scripts/apply_licenses.py             # apply + write bib
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_licenses import canonical_cc_url  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
TEXT_REPORT = REPO / "build" / "licenses_report.json"
CROSSREF = REPO / "build" / "crossref_licenses.json"
OUT = REPO / "build" / "licenses_applied.json"


def family_of(spdx: str) -> str:
    if not isinstance(spdx, str):
        return ""
    if spdx.startswith("CC0"):
        return "CC0"
    m = re.match(r"CC-((?:BY)(?:-NC)?(?:-SA|-ND)?)", spdx)
    return m.group(1) if m else ""


def parse_entries(text: str):
    out = {}
    for m in re.finditer(r"@(\w+)\s*\{\s*([^,]+),", text):
        key = m.group(2).strip()
        start = m.end()
        nxt = text.find("\n@", start)
        end = nxt if nxt != -1 else len(text)
        body = text[start:end]
        lm = re.search(r"(?im)^\s*license\s*=\s*\{", body)
        fm = re.search(r"(?im)^[ \t]*file\s*=\s*\{[^}]*\},?[ \t]*\n", body)
        out[key] = {"start": start, "end": end, "has_license": bool(lm),
                    "file_rel": fm.start() if fm else None}
    return out


def license_block(spdx: str) -> str:
    return (f"  license    = {{{spdx}}},\n"
            f"  licenseurl = {{{canonical_cc_url(spdx)}}},\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    text = BIB.read_text(encoding="utf-8")
    entries = parse_entries(text)
    cr = json.loads(CROSSREF.read_text()) if CROSSREF.exists() else {}
    report = json.loads(TEXT_REPORT.read_text())

    # text signal keyed by bib key
    tx = {}
    for r in report:
        if not r["in_bib"]:
            continue
        tx[r["bib_keys"][0]] = {"spdx": r["license"], "conf": r["confidence"]}

    planned, manual = [], []
    keys = set(entries) | set(cr) | set(tx)
    for key in keys:
        e = entries.get(key)
        if not e or e["has_license"]:
            continue
        c = cr.get(key, {})
        cr_ok = c.get("status") == "ok"
        cr_spdx = c.get("license") if cr_ok else None
        cr_notvor = c.get("status") == "cc_but_not_vor"
        t = tx.get(key)
        t_spdx = t["spdx"] if t else None
        t_conf = t["conf"] if t else None
        t_fam = family_of(t_spdx) if t_spdx else ""

        if cr_ok:
            # PDF wins over Crossref on any disagreement (family OR version) when
            # the PDF states a concrete license; Crossref only fills unknowns.
            t_concrete = (isinstance(t_spdx, str) and t_conf in ("HIGH", "MEDIUM")
                          and not t_spdx.endswith("-?"))
            if t_concrete and t_spdx != cr_spdx:
                planned.append((key, t_spdx, "text-over-crossref"))
            elif t_spdx and t_conf != "CONFLICT" and t_fam and t_fam != family_of(cr_spdx):
                # PDF family disagrees but PDF version unknown -> can't honor "prefer PDF"
                manual.append((key, f"text {t_spdx} vs Crossref {cr_spdx} family differ, PDF version unknown"))
            else:
                src = "crossref+text" if (t_spdx and t_conf != "CONFLICT") else "crossref"
                planned.append((key, cr_spdx, src))
            continue

        # no authoritative Crossref vor license
        if t_conf == "HIGH":                      # URL in the article body itself
            planned.append((key, t_spdx, "text-url" + ("/cr-am" if cr_notvor else "")))
        elif t_conf == "MEDIUM" and isinstance(t_spdx, str) and not t_spdx.endswith("-?"):
            planned.append((key, t_spdx, "text-prose-version"))
        elif t_conf in ("MEDIUM", "CONFLICT"):
            reason = "version unknown" if (isinstance(t_spdx, str) and t_spdx.endswith("-?")) else "conflict"
            extra = f"; Crossref cc-non-vor {c.get('all_cc_urls')}" if cr_notvor else ""
            manual.append((key, f"text {t_spdx} ({reason}){extra}"))
        elif cr_notvor:
            manual.append((key, f"Crossref CC only as non-vor: {c.get('all_cc_urls')}"))

    # apply bottom-up so offsets stay valid
    for key, spdx, _s in sorted(planned, key=lambda p: entries[p[0]]["start"], reverse=True):
        e = entries[key]
        block = license_block(spdx)
        if e["file_rel"] is not None:
            ins = e["start"] + e["file_rel"]
        else:
            close = text.rfind("}", e["start"], e["end"])
            ins = text.rfind("\n", e["start"], close) + 1
        text = text[:ins] + block + text[ins:]

    from collections import Counter
    print(f"planned: {len(planned)}   manual: {len(manual)}")
    print("license distribution:")
    for lic, n in Counter(s for _, s, _ in planned).most_common():
        print(f"  {lic:16} {n}")
    print("by source:")
    for s, n in Counter(src for _, _, src in planned).most_common():
        print(f"  {s:22} {n}")
    print("\n-- manual review --")
    for key, why in sorted(manual):
        print(f"  ? {key:30} {why}")

    OUT.write_text(json.dumps(
        {"applied": [{"key": k, "license": s, "source": src} for k, s, src in sorted(planned)],
         "manual": [{"key": k, "reason": w} for k, w in sorted(manual)]}, indent=2))

    if args.dry_run:
        print("\n[dry-run] bib not written")
        return
    BIB.write_text(text, encoding="utf-8")
    print(f"\nwrote {BIB}")


if __name__ == "__main__":
    main()
