#!/usr/bin/env python3
"""Apply bib↔PDF orphan matches with publication-year reconciliation.

Pipeline:
    1. Re-run the same scoring logic as :mod:`reconcile_bib_orphans` against
       the live bib.
    2. Filter to score >= ``--threshold`` (default 130) and apply a sanity
       check: surname score >= 50, or a DOI agreement, or filename has no
       year (in which case bib is the only year signal).
    3. For each surviving candidate pair, classify based on the PDF's page-1
       year text:

        Tier 1  bib year == filename year         (no year ambiguity)
        Tier 2  bib year != filename year, but page 1 has a clear winner
        Tier 3  page 1 has no year / disagrees with both / is ambiguous

    4. Apply Tiers 1 and 2 automatically: edit the bib's ``file =`` slot,
       update ``year =`` if needed, and rename the PDF on disk if its
       filename year is wrong.
    5. Tier 3 is *not* applied — it's reported in the audit log.

Output: ``logs/year_resolutions.log``. Pass ``--dry-run`` to inspect what
would happen without changing anything.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import unicodedata
from collections import Counter
from pathlib import Path

import bibtexparser
from pypdf import PdfReader

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BIB = REPO / "siphonophores.bib"

OUT = LOGS / "year_resolutions.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("apply_with_year_resolution")

sys.path.insert(0, str(REPO / "scripts"))
from reconcile_bib_orphans import (  # noqa: E402
    fold,
    first_author_surname,
    parse_pdf_filename,
    first_page_title_lines,
    score_pair,
)

YEAR_RE = re.compile(r"\b(1[6-9]\d{2}|20[0-3]\d)\b")
FILENAME_YEAR_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")


def page_one_text(pdf: Path) -> str:
    try:
        return PdfReader(str(pdf)).pages[0].extract_text() or ""
    except Exception as exc:
        log.warning("could not read %s: %s", pdf.name, exc)
        return ""


def detect_pub_year(text: str, *, candidates: list[int]) -> tuple[int | None, dict]:
    """Pick the publication year from page-1 text.

    Returns ``(year_or_none, evidence)`` where ``evidence`` carries the year
    counts on page 1 so callers can audit the choice.
    """
    if not text:
        return None, {"counts": {}, "reason": "empty-text"}
    counts = Counter(int(m.group(0)) for m in YEAR_RE.finditer(text))
    evidence = {"counts": dict(counts.most_common(8)), "reason": ""}
    if not counts:
        evidence["reason"] = "no-years"
        return None, evidence

    cand_set = set(candidates)
    cand_present = [(c, counts[c]) for c in cand_set if c in counts]

    if len(cand_present) == 1:
        evidence["reason"] = "one-candidate-on-page"
        return cand_present[0][0], evidence

    if len(cand_present) >= 2:
        cand_present.sort(key=lambda kv: -kv[1])
        # If the leader has at least 2x the runner-up, take it.
        if cand_present[0][1] >= 2 * cand_present[1][1]:
            evidence["reason"] = "candidate-dominant"
            return cand_present[0][0], evidence
        evidence["reason"] = "candidates-tied"
        return None, evidence

    # No candidate on page 1 at all — fall back to the most-frequent year on
    # the page IF it dominates, but flag it.
    most = counts.most_common(1)[0]
    if most[1] >= 3:
        evidence["reason"] = "page-year-not-in-candidates"
        return None, evidence  # don't trust it without a candidate match

    evidence["reason"] = "no-clear-signal"
    return None, evidence


def collect_candidates(threshold: int) -> list[tuple[int, dict, dict, dict]]:
    with BIB.open() as f:
        db = bibtexparser.load(f)
    file_less = [e for e in db.entries if not (e.get("file") or "").strip()]
    referenced = {(e.get("file") or "").strip() for e in db.entries if (e.get("file") or "").strip()}

    orphan_pdfs = []
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and (rel.parts[0].startswith("Z") or rel.parts[0] == "orphans"):
            continue
        if p.name in referenced:
            continue
        orphan_pdfs.append(p)

    dois = json.loads((BUILD / "dois.json").read_text()) if (BUILD / "dois.json").exists() else {}
    pdf_sigs = []
    for path in orphan_pdfs:
        author_chunk, year = parse_pdf_filename(path.stem)
        sig = {
            "path": path, "rel": str(path.relative_to(LIBRARY)),
            "stem": path.stem, "author_chunk": author_chunk, "year": year,
            "doi": None, "title": None, "page_lines": None,
        }
        rec = dois.get(path.name) or {}
        if rec.get("doi"):
            sig["doi"] = rec["doi"]
        if rec.get("title"):
            sig["title"] = rec["title"]
        if sig["title"] is None:
            sig["page_lines"] = first_page_title_lines(path)
        pdf_sigs.append(sig)

    pairs = []
    for entry in file_less:
        for sig in pdf_sigs:
            score, bd = score_pair(entry, sig)
            if score < threshold:
                continue
            # sanity: surname >= 50 OR DOI match
            if bd["doi"] >= 200 or bd["surname"] >= 50:
                pairs.append((score, bd, entry, sig))
    pairs.sort(key=lambda r: -r[0])

    # Mutual best: each entry & each PDF claimed at most once.
    claimed_e: set[str] = set()
    claimed_p: set[str] = set()
    chosen: list[tuple[int, dict, dict, dict]] = []
    for score, bd, entry, sig in pairs:
        if entry["ID"] in claimed_e or sig["rel"] in claimed_p:
            continue
        claimed_e.add(entry["ID"])
        claimed_p.add(sig["rel"])
        chosen.append((score, bd, entry, sig))
    return chosen


def replace_year_in_filename(stem: str, old_year: int | None, new_year: int) -> str:
    """Return a new stem with the year replaced or appended."""
    if old_year is None:
        # Append the year before the suffix-like '_etal'? Keep simple: append.
        # Better: insert before any trailing '_<descriptor>' we don't know.
        return f"{stem}{new_year}"
    new_stem, n = FILENAME_YEAR_RE.subn(lambda m: str(new_year) if int(m.group(0)) == old_year else m.group(0), stem, count=1)
    if n == 0:
        return f"{stem}{new_year}"
    return new_stem


def insert_file_field(text: str, key: str, fname: str) -> str:
    pattern = re.compile(
        rf"(@article\{{{re.escape(key)},\n(?:  [^\n]*\n)*?)(\}})",
        flags=re.MULTILINE,
    )
    new_text, n = pattern.subn(rf"\1  file = {{{fname}}},\n\2", text, count=1)
    if n != 1:
        raise RuntimeError(f"failed to insert file for {key} ({n} hits)")
    return new_text


def update_year_field(text: str, key: str, new_year: int) -> str:
    block_re = re.compile(
        rf"(@article\{{{re.escape(key)},)((?:.|\n)*?)(\n\}})",
    )

    def repl(m):
        body = m.group(2)
        new_body, n = re.subn(r"\n  year = \{\d{4}\},", f"\n  year = {{{new_year}}},", body, count=1)
        if n == 0:
            new_body = body + f"\n  year = {{{new_year}}},"
        return m.group(1) + new_body + m.group(3)
    new_text, n = block_re.subn(repl, text, count=1)
    if n != 1:
        raise RuntimeError(f"failed to update year for {key} ({n} hits)")
    return new_text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=int, default=130)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    chosen = collect_candidates(args.threshold)
    log.info("candidate pairs (score >= %d, sanity-checked): %d", args.threshold, len(chosen))

    audit: list[str] = [
        f"# year_resolutions.log — apply_with_year_resolution.py {'(dry-run)' if args.dry_run else ''}",
        f"# {len(chosen)} candidate pairs at threshold {args.threshold}",
        "",
    ]

    tier1: list[dict] = []  # year-clean
    tier2: list[dict] = []  # year-discrepancy resolved from page 1
    tier3: list[dict] = []  # ambiguous

    for score, bd, entry, sig in chosen:
        bib_year = None
        if entry.get("year"):
            m = re.search(r"\d{4}", entry["year"])
            if m:
                bib_year = int(m.group(0))
        pdf_year = sig["year"]
        candidates = [y for y in (bib_year, pdf_year) if y is not None]

        page_text = page_one_text(sig["path"])
        pub_year, evidence = detect_pub_year(page_text, candidates=candidates)

        info = {
            "score": score, "breakdown": bd, "entry": entry, "sig": sig,
            "bib_year": bib_year, "pdf_year": pdf_year,
            "pub_year": pub_year, "evidence": evidence,
        }

        title_score = bd.get("title", 0)
        # Threshold for auto-applying year corrections / rename-or-update.
        # Below this, the title isn't agreeing strongly enough to trust the
        # PDF as the same paper as the bib record — sent to T3 for review.
        STRONG_TITLE = 60  # 0.7 * 86 = strong token-set match

        if bib_year and pdf_year and bib_year == pdf_year:
            tier1.append(info)
        elif (
            pub_year
            and (pub_year == bib_year or pub_year == pdf_year)
            and title_score >= STRONG_TITLE
        ):
            tier2.append(info)
        elif pub_year is None and pdf_year is None and bib_year is not None:
            if (
                page_text
                and bib_year
                and re.search(rf"\b{bib_year}\b", page_text)
                and title_score >= STRONG_TITLE
            ):
                info["pub_year"] = bib_year
                info["evidence"]["reason"] = "bib-year-on-page-no-pdf-year"
                tier2.append(info)
            else:
                tier3.append(info)
        else:
            tier3.append(info)

    audit.append(f"\n=== TIER 1 (year-clean): {len(tier1)} ===\n")
    audit.append(f"\n=== TIER 2 (year-discrepancy, resolved): {len(tier2)} ===\n")
    audit.append(f"\n=== TIER 3 (ambiguous, NOT applied): {len(tier3)} ===\n")

    bib_text = BIB.read_text()
    pdf_renames: list[tuple[Path, Path]] = []
    bib_year_updates: list[tuple[str, int]] = []

    def append_audit(tier: str, rec: dict, action: str) -> None:
        e = rec["entry"]
        s = rec["sig"]
        audit.append(
            f"[{tier}] score={rec['score']:>3}  {e['ID']:<35} ({rec['bib_year']}) "
            f"<-> {s['rel']} (filename year={rec['pdf_year']})"
        )
        audit.append(f"    pub_year={rec['pub_year']}  evidence={rec['evidence']}")
        audit.append(f"    -> {action}")
        audit.append("")

    # ----- TIER 1: just add file= -----
    for rec in tier1:
        e = rec["entry"]
        s = rec["sig"]
        bib_text = insert_file_field(bib_text, e["ID"], s["path"].name)
        append_audit("T1", rec, f"insert file = {{{s['path'].name}}}")

    # ----- TIER 2: rename / update year as needed -----
    for rec in tier2:
        e = rec["entry"]
        s = rec["sig"]
        pub_year = rec["pub_year"]
        actions = []
        new_pdf_path = s["path"]

        if rec["pdf_year"] is not None and rec["pdf_year"] != pub_year:
            # Rename PDF on disk so its filename year = pub_year.
            new_stem = replace_year_in_filename(s["path"].stem, rec["pdf_year"], pub_year)
            new_pdf_path = s["path"].with_name(new_stem + s["path"].suffix)
            if new_pdf_path != s["path"] and new_pdf_path.exists():
                actions.append(f"SKIP-rename (target exists: {new_pdf_path.name})")
                tier3.append({**rec, "evidence": {**rec["evidence"], "reason": "rename-collision"}})
                continue
            pdf_renames.append((s["path"], new_pdf_path))
            actions.append(f"rename {s['path'].name} -> {new_pdf_path.name}")

        bib_text = insert_file_field(bib_text, e["ID"], new_pdf_path.name)
        actions.append(f"insert file = {{{new_pdf_path.name}}}")

        if rec["bib_year"] is not None and rec["bib_year"] != pub_year:
            bib_year_updates.append((e["ID"], pub_year))
            bib_text = update_year_field(bib_text, e["ID"], pub_year)
            actions.append(f"update bib year {rec['bib_year']} -> {pub_year}")

        append_audit("T2", rec, "; ".join(actions))

    # ----- TIER 3: report only -----
    for rec in tier3:
        append_audit("T3", rec, "no action — manual review")

    if not args.dry_run:
        # Apply PDF renames
        for old, new in pdf_renames:
            log.info("renaming %s -> %s", old.name, new.name)
            shutil.move(str(old), str(new))
        BIB.write_text(bib_text)
        log.info(
            "applied: tier1=%d, tier2=%d (renames=%d, year-edits=%d); tier3=%d",
            len(tier1), len(tier2), len(pdf_renames), len(bib_year_updates), len(tier3),
        )
    else:
        log.info(
            "DRY RUN: tier1=%d, tier2=%d (renames=%d, year-edits=%d); tier3=%d",
            len(tier1), len(tier2), len(pdf_renames), len(bib_year_updates), len(tier3),
        )

    OUT.write_text("\n".join(audit) + "\n")
    log.info("wrote %s", OUT)


if __name__ == "__main__":
    main()
