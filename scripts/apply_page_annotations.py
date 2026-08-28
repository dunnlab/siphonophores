#!/usr/bin/env python3
"""Write page annotations into ``siphonophores.bib``.

Reads ``build/page_annotations.json`` (produced by the annotation pass described
in ``prompts/annotate_pages.md``) and sets ``keeppages`` / ``doclang`` /
``pagemap`` on the matching bib entries, deriving ``ocrlang`` from ``doclang``.

``siphonophores.bib`` is the source of truth and is edited in place, so this uses
``bibio``: entries that gain nothing are emitted byte-for-byte as they were read,
and an entry that gains a field differs by exactly the lines added. Run with
``--dry-run`` first and read the diff.

Modelled on ``scripts/apply_licenses.py``, including its central habit: **never
overwrite a field that is already set.** A value in the bib is a human decision
or an earlier reviewed pass; this script only fills blanks unless told otherwise
with ``--overwrite``.

Why ``ocrlang`` is derived here rather than in the pipeline
-----------------------------------------------------------
``doclang`` is a BCP-47 tag — a fact about the paper. ``ocrlang`` is a list of
Tesseract pack names — an instruction to the OCR engine, and the only one of the
two that corpus reads. Resolving at annotation time rather than at run time means:

* the bib states literally which packs each paper will get, with no table to
  simulate when reading it;
* ``doclang`` stays inert, so correcting a language label costs nothing;
* ``ocrlang`` stays a directly-fingerprinted literal, so a later improvement to
  the mapping table does nothing until this script is re-run — which rewrites the
  bib, and therefore invalidates visibly. Deriving at run time instead would
  leave the table itself outside the fingerprint, and improving it would silently
  fail to reprocess anything (see corpus#215).

Usage::

    python scripts/apply_page_annotations.py --dry-run
    python scripts/apply_page_annotations.py
    python scripts/apply_page_annotations.py --check     # report drift, write nothing
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bibio import BibFormatError, parse_bib, render_bib  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"
BUILD = REPO / "build"
ANNOTATIONS = BUILD / "page_annotations.json"
# One file per document, for a parallel pass — see load_annotations.
ANNOTATION_DIR = BUILD / "annotations"
EVIDENCE = BUILD / "page_evidence.json"

FIELDS = ("keeppages", "doclang", "pagemap")

# ---------------------------------------------------------------------------
# BCP-47 -> Tesseract pack names
# ---------------------------------------------------------------------------
# TEMPORARY LOCAL COPY. corpus owns this mapping (`_ISO_TO_TESSERACT` in
# pipeline/scan.py) and is adding a public `bcp47_to_tesseract` resolver —
# corpus#215. When that lands, delete everything between here and
# `bcp47_to_tesseract` and import it instead. Keeping a second copy indefinitely
# is how the two drift.
#
# Tesseract's pack names are not an ISO namespace: `chi_sim`, `deu_latf`,
# `jpn_vert` and `srp_latn` have no ISO equivalent at all, which is why the
# mapping has to exist rather than being a string transformation.
_LANG_TO_PACKS: dict[str, tuple[str, ...]] = {
    # Latin-script European
    "en": ("eng",), "de": ("deu",), "fr": ("fra",), "la": ("lat",),
    "it": ("ita",), "es": ("spa",), "pt": ("por",), "nl": ("nld",),
    "pl": ("pol",), "sv": ("swe",), "no": ("nor",), "da": ("dan",),
    "fi": ("fin",), "ca": ("cat",), "cs": ("ces",), "hu": ("hun",),
    "ro": ("ron",), "sk": ("slk",), "sl": ("slv",), "hr": ("hrv",),
    "et": ("est",), "lv": ("lav",), "lt": ("lit",), "tr": ("tur",),
    # Cyrillic
    "ru": ("rus",), "uk": ("ukr",), "bg": ("bul",), "mk": ("mkd",),
    # Greek — note `grc` is reachable only by naming it; corpus's ISO table
    # cannot express Ancient Greek at all (corpus#215).
    "el": ("ell",), "grc": ("grc",),
    # Other scripts
    "ar": ("ara",), "he": ("heb",), "hi": ("hin",), "th": ("tha",),
    "ja": ("jpn",), "ko": ("kor",),
}
# Script subtags. This is the half plain ISO cannot do, and the reason the
# annotation pass records BCP-47: `de-Latf` means "German set in Fraktur", which
# selects a genuinely different OCR model. Without deu_latf, 19th-century German
# scans OCR to whitespace.
_SCRIPT_TO_PACKS: dict[str, tuple[str, ...]] = {
    "de-latf": ("deu_latf", "deu"),
    "zh-hans": ("chi_sim",),
    "zh-hant": ("chi_tra",),
    "sr-latn": ("srp_latn",),
    "sr-cyrl": ("srp",),
    # langdetect's legacy region spellings, accepted so evidence copied straight
    # from a detection result still resolves.
    "zh-cn": ("chi_sim",),
    "zh-tw": ("chi_tra",),
}

# `eng` is appended to every pin that does not already name it. corpus does NOT
# append it to an honored pin (deliberately — the pin is the exact `-l` value),
# so if we want it, we say it here, where it stays a literal in the bib and a
# directly-fingerprinted input.
#
# Not because English is on every page, and not because `eng` is a better
# model — alone it loses to `swe` (0.751 vs 0.837) and `por` (0.850 vs 0.931).
# Because Tesseract arbitrates per word between the models it is given, so a
# second, complementary one covers words the first gets wrong. Measured
# against the gold transcriptions:
#
#     lat -> lat+eng   0.562 -> 0.624
#     nld -> nld+eng   0.789 -> 0.818
#     por -> por+eng   0.931 -> 0.944
#     swe -> swe+eng   0.837 -> 0.833
#
# Three gains and one wash. The condition used to be "no Latin-script pack
# present", which fired for `rus` and `chi_sim` and never for `fra`, `swe` or
# `por` — so 482 papers were pinned to a single pack where detection would
# have used two. See corpus dev_docs/OCR_LANGUAGES.md.
_LATIN_FALLBACK = "eng"
_LATIN_PACKS = {
    "eng", "deu", "deu_latf", "fra", "lat", "ita", "spa", "por", "nld", "pol",
    "swe", "nor", "dan", "fin", "cat", "ces", "hun", "ron", "slk", "slv",
    "hrv", "est", "lav", "lit", "tur", "srp_latn",
}


def bcp47_to_tesseract(tag: str) -> list[str]:
    """Tesseract pack names for a BCP-47 tag. Empty list if unknown."""
    t = (tag or "").strip().lower().replace("_", "-")
    if not t:
        return []
    if t in _SCRIPT_TO_PACKS:
        return list(_SCRIPT_TO_PACKS[t])
    if t in _LANG_TO_PACKS:
        return list(_LANG_TO_PACKS[t])
    # "de-DE" or "fr-CA": a region subtag carries no OCR meaning, so fall back
    # to the base language rather than failing.
    base = t.split("-")[0]
    if base in _LANG_TO_PACKS:
        return list(_LANG_TO_PACKS[base])
    return []


def parse_doclang(doclang: str) -> list[str]:
    """Split a ``doclang`` value into BCP-47 tags, dominant first.

    A comma-separated list, because a document is routinely more than one
    language: 271 of this library's papers have a pagemap naming two or more,
    most often a Spanish or French article with an English title and abstract.
    A single tag could not say that, so `derive_ocrlang` could only ever emit
    one language's packs and the English on page 2 was read by a Spanish-only
    model.

    Order is meaningful and preserved — Tesseract takes the first pack as
    primary — so the dominant language of the body comes first.
    """
    return [t.strip() for t in (doclang or "").split(",") if t.strip()]


def derive_ocrlang(doclang: str) -> Optional[str]:
    """The ``ocrlang`` value for a ``doclang``, or None if unresolvable.

    Every tag contributes its packs, in order, deduplicated. Two models
    covering two languages that are genuinely on the page is the case
    Tesseract's per-word arbitration is good at; measured against the gold
    transcriptions, `por+eng` beats both `por` (0.931) and `eng` (0.850) at
    0.944. What does *not* pay is a model with nothing to contribute — seven
    packs on a monolingual Latin text scored below one — which is why this
    derives from what the annotator observed rather than adding packs
    speculatively. See corpus dev_docs/OCR_LANGUAGES.md.
    """
    packs: list[str] = []
    for tag in parse_doclang(doclang):
        for pack in bcp47_to_tesseract(tag):
            if pack not in packs:
                packs.append(pack)
    if not packs:
        return None
    # Vertical CJK is exclusive: `jpn_vert` alone scores 0.574 on vertically
    # set pages where `jpn_vert+eng` scores 0.176, because the two models
    # compete for the same glyphs. Never widen such a pin.
    if any(p.endswith("_vert") for p in packs):
        return "+".join(p for p in packs if p.endswith("_vert"))
    if _LATIN_FALLBACK not in packs:
        packs.append(_LATIN_FALLBACK)
    return "+".join(packs)


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

_RANGE_RE = re.compile(r"^\s*(\d+)\s*(?:--\s*(\d+)?)?\s*$")


def parse_keeppages(value: str, n_pages: Optional[int] = None) -> list[int]:
    """Expand a keeppages value to sorted unique page numbers.

    Raises ValueError on anything malformed. The point is to fail here, at
    annotation time, rather than to discover it during a corpus run — a bad
    range that silently resolves to nothing would drop an entire paper.
    """
    pages: set[int] = set()
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = _RANGE_RE.match(chunk)
        if not m:
            raise ValueError(f"malformed range {chunk!r}")
        lo = int(m.group(1))
        if lo < 1:
            raise ValueError(f"page numbers are 1-based, got {chunk!r}")
        if "--" not in chunk:
            pages.add(lo)
            continue
        hi_s = m.group(2)
        if hi_s is None:  # open-ended "40--"
            if n_pages is None:
                raise ValueError(f"open-ended {chunk!r} needs a known page count")
            hi = n_pages
        else:
            hi = int(hi_s)
        if hi < lo:
            raise ValueError(f"reversed range {chunk!r}")
        pages.update(range(lo, hi + 1))
    if not pages:
        raise ValueError("selects no pages")
    if n_pages is not None:
        over = sorted(p for p in pages if p > n_pages)
        if over:
            raise ValueError(
                f"selects page(s) {over[:5]} beyond the document's {n_pages}")
    return sorted(pages)


def _records_from(payload) -> list[dict]:
    """Normalise the three shapes an annotation file can take.

    A single record (what a per-document file holds), a bare list, or a
    ``{"documents": [...]}`` wrapper. The single-record case is the one the
    prompt actually asks for, so getting it wrong reads every file as empty.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "documents" in payload:
            return payload["documents"]
        if "file" in payload:
            return [payload]
    return []


def load_annotations(src: Path) -> tuple[list[dict], set[str]]:
    """Read annotations from a file, or from every ``*.json`` in a directory.

    The directory form is what makes a parallel pass safe. Several annotators
    appending to one shared JSON file would interleave and lose records; one
    file per document collides with nothing, needs no locking, and makes
    resumption trivial — a document is done when its file exists.

    Returns the records plus the set of basenames annotated more than once,
    which is a real possibility once work is split and worth reporting rather
    than silently resolving.
    """
    if src.is_dir():
        paths = sorted(src.glob("*.json"))
        if not paths:
            raise SystemExit(f"no *.json files in {src}")
    else:
        paths = [src]

    records: list[dict] = []
    seen: set[str] = set()
    dupes: set[str] = set()
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            # One malformed shard must not cost the whole run.
            raise SystemExit(f"{path}: not valid JSON — {e}")
        for rec in _records_from(payload):
            name = (rec.get("file") or "").strip().lower()
            if name:
                if name in seen:
                    dupes.add(name)
                seen.add(name)
            records.append(rec)
    return records, dupes


def load_page_counts() -> dict[str, int]:
    """Basename -> page count, from the evidence file if it exists."""
    if not EVIDENCE.exists():
        return {}
    ev = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    return {d["file"].lower(): d.get("pages", 0) for d in ev.get("documents", [])}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--annotations", type=Path, default=ANNOTATIONS,
                    help="A JSON file, or a directory of per-document JSON "
                         f"files (default: {ANNOTATIONS.name}, falling back to "
                         f"{ANNOTATION_DIR.name}/ if that is absent). Use the "
                         "directory form for a parallel pass.")
    ap.add_argument("--bib", type=Path, default=BIB)
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change; write nothing.")
    ap.add_argument("--check", action="store_true",
                    help="Report doclang/ocrlang disagreement already in the bib "
                         "and exit. Writes nothing, reads no annotations.")
    ap.add_argument("--overwrite", action="store_true",
                    help="Replace fields that are already set. Off by default: an "
                         "existing value is a decision someone already made.")
    ap.add_argument("--no-ocrlang", action="store_true",
                    help="Write doclang but do not derive ocrlang.")
    args = ap.parse_args()

    if not args.bib.exists():
        raise SystemExit(f"no bib at {args.bib}")
    doc = parse_bib(args.bib.read_text(encoding="utf-8"))
    by_file = doc.by_file()

    # --check: audit what is already in the bib, independent of annotations.
    if args.check:
        bad = 0
        for entry in doc.entries:
            dl, ol = entry.get("doclang"), entry.get("ocrlang")
            if not dl or not ol:
                continue
            want = derive_ocrlang(dl)
            if want and want != ol:
                bad += 1
                print(f"{entry.key}: doclang={dl} implies {want}, but ocrlang={ol}")
        print(f"\n{bad} entr{'y' if bad == 1 else 'ies'} where ocrlang does not "
              f"match doclang.")
        return 1 if bad else 0

    if args.annotations == ANNOTATIONS and not args.annotations.exists() \
            and ANNOTATION_DIR.is_dir():
        args.annotations = ANNOTATION_DIR
    if not args.annotations.exists():
        raise SystemExit(
            f"no annotations at {args.annotations}\n"
            "Run the pass in prompts/annotate_pages.md over "
            "build/page_evidence.json first."
        )
    records, dupes = load_annotations(args.annotations)
    if dupes:
        print(f"warning: {len(dupes)} file(s) annotated more than once; the "
              f"last record for each wins: {', '.join(sorted(dupes)[:5])}\n")
    page_counts = load_page_counts()

    stats: Counter[str] = Counter()
    problems: list[str] = []
    changes: list[str] = []

    for rec in records:
        fname = (rec.get("file") or "").strip()
        if not fname:
            problems.append("annotation with no `file`")
            continue
        stats["records"] += 1
        entry = by_file.get(Path(fname).name.lower())
        if entry is None:
            problems.append(f"{fname}: no bib entry references this file")
            stats["unmatched"] += 1
            continue

        if rec.get("needs_review"):
            stats["flagged_needs_review"] += 1

        updates: dict[str, str] = {}

        kp = (rec.get("keeppages") or "").strip()
        if kp:
            n = page_counts.get(fname.lower())
            try:
                sel = parse_keeppages(kp, n)
            except ValueError as e:
                problems.append(f"{fname}: keeppages {kp!r} — {e}")
                stats["bad_keeppages"] += 1
            else:
                if n and len(sel) == n:
                    # Selecting the whole document is a no-op that still
                    # fingerprints, so it would force a reprocess for nothing.
                    stats["keeppages_covers_all_skipped"] += 1
                else:
                    updates["keeppages"] = kp

        dl = (rec.get("doclang") or "").strip()
        if dl:
            packs = derive_ocrlang(dl)
            if packs is None:
                problems.append(f"{fname}: doclang {dl!r} maps to no Tesseract pack")
                stats["bad_doclang"] += 1
            else:
                updates["doclang"] = dl
                if not args.no_ocrlang:
                    updates["ocrlang"] = packs

        pm = (rec.get("pagemap") or "").strip()
        if pm:
            updates["pagemap"] = pm

        for field, value in updates.items():
            current = entry.get(field)
            if current and not args.overwrite:
                if current != value:
                    stats[f"{field}_already_set_differs"] += 1
                    problems.append(
                        f"{fname}: {field} already = {current!r}, annotation says "
                        f"{value!r} (kept existing; use --overwrite to replace)")
                continue
            if current == value:
                continue
            try:
                entry.set_field(field, value)
            except BibFormatError as e:
                problems.append(f"{fname}: {e}")
                stats["rejected"] += 1
                continue
            stats[f"set_{field}"] += 1
            changes.append(f"{entry.key:<28} {field:<10} = {value}")

    print(f"annotations read : {stats['records']}")
    for k in sorted(stats):
        if k != "records":
            print(f"  {k:<34} {stats[k]}")
    if changes:
        print(f"\nfields to write ({len(changes)}):")
        for line in changes[:40]:
            print("  " + line)
        if len(changes) > 40:
            print(f"  ... and {len(changes) - 40} more")
    if problems:
        print(f"\nproblems ({len(problems)}):")
        for line in problems[:30]:
            print("  " + line)
        if len(problems) > 30:
            print(f"  ... and {len(problems) - 30} more")

    dirty = [e for e in doc.entries if e.dirty]
    if not dirty:
        print("\nnothing to write.")
        return 0
    if args.dry_run:
        print(f"\n--dry-run: {len(dirty)} entries would change; nothing written.")
        return 0

    out = render_bib(doc)
    # Guard against a parser regression silently truncating the bibliography.
    before = args.bib.read_text(encoding="utf-8")
    if out.count("@") != before.count("@"):
        raise SystemExit("refusing to write: entry count changed during render")
    args.bib.write_text(out, encoding="utf-8")
    print(f"\nwrote {args.bib} ({len(dirty)} entries changed)")
    print("Review with: git diff siphonophores.bib")
    return 0


if __name__ == "__main__":
    sys.exit(main())
