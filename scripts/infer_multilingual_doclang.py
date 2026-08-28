#!/usr/bin/env python3
"""Widen ``doclang`` where the pagemap already says the paper is multilingual.

The annotation pass recorded structure in prose — "1--26 complete spanish
article; page 2 contains an english title and abstract" — and then derived
``ocrlang`` from a single ``doclang`` tag, so the English on page 2 was read by
a Spanish-only model. 271 papers here have a pagemap naming two or more
languages; 169 of them carry a single-pack ``ocrlang``.

This reads that prose back and writes the languages into ``doclang`` as a
comma-separated list, dominant first, then re-derives ``ocrlang``.

**Page ranges are respected.** A pagemap segment carries its own range, and a
language named only in a segment ``keeppages`` drops is not on any page corpus
will OCR — `Alekseev1984` is "1--6 russian original; 7--14 english
translation" with ``keeppages = {1--6}``, and pinning `eng` there would add a
model for text that is not in the document.

**Existing ``*_vert`` pins are never touched.** Vertical setting is typesetting,
which BCP-47 cannot express, so derivation cannot reproduce those and would
silently replace `jpn_vert` with `jpn+eng` — worth 0.574 against 0.246 on the
pages concerned.

Usage::

    python scripts/infer_multilingual_doclang.py --dry-run
    python scripts/infer_multilingual_doclang.py --only Alekseev1984.pdf
    python scripts/infer_multilingual_doclang.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from apply_page_annotations import derive_ocrlang, parse_doclang  # noqa: E402
from bibio import parse_bib, render_bib  # noqa: E402

BIB = REPO / "siphonophores.bib"

# Language names as the annotation prose writes them, to BCP-47. Deliberately
# a closed list: the pagemap is free text and an open-ended matcher would read
# "latinate" or "englishman" as a language.
_NAME_TO_TAG = {
    "english": "en", "french": "fr", "german": "de", "latin": "la",
    "italian": "it", "spanish": "es", "portuguese": "pt", "dutch": "nl",
    "russian": "ru", "swedish": "sv", "danish": "da", "norwegian": "no",
    "polish": "pl", "czech": "cs", "croatian": "hr", "japanese": "ja",
    "chinese": "zh", "greek": "el",
}
# A mention that is *about* a language rather than evidence of text in it.
_NOT_EVIDENCE = re.compile(
    r"(?:translated\s+(?:from|into)|no\s+\w+\s+text|latin\s+(?:binomial|name|"
    r"epithet|authorit)|latin[- ]titled|latin\s+script|latin\s+letter)", re.I)

# A whole clause the annotator wrote to *discount* a language signal. These are
# the cases where the prose says the detector fired on taxonomic names or OCR
# noise rather than on text — `Bonnemains_Carre1991` is a French article whose
# pagemap ends "isolated Spanish and Latin detector scores reflect names", and
# taking those at face value adds two models with nothing to contribute. Seven
# packs on a monolingual Latin text scored below one pack; this is that failure
# in miniature.
_DISCLAIMER = re.compile(
    r"(?:reflect\s+(?:taxonomic\s+)?names|detector\s+(?:score|hit|artefact|"
    r"artifact)|rather\s+than\s+(?:a\s+)?(?:translation|\w+\s+text)|"
    r"not\s+a\s+translation|noisy\s+ocr|apparent\s+\w+\s+language)", re.I)

_SEG_RANGE = re.compile(r"^\s*(\d+)\s*(?:--\s*(\d+))?")


def _kept_pages(keeppages: str | None):
    """The pages corpus will actually OCR, or None for 'all of them'."""
    if not keeppages:
        return None
    pages = set()
    for chunk in keeppages.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = re.match(r"^(\d+)\s*(?:--\s*(\d+)?)?$", chunk)
        if not m:
            return None                      # malformed: do not guess
        lo = int(m.group(1))
        if "--" not in chunk:
            pages.add(lo)
        elif m.group(2):
            pages.update(range(lo, int(m.group(2)) + 1))
        else:
            pages.update(range(lo, lo + 500))   # open-ended
    return pages


def languages_on_kept_pages(pagemap: str, keeppages: str | None):
    """Ordered BCP-47 tags for languages present on the pages that survive."""
    kept = _kept_pages(keeppages)
    found: list[str] = []
    for seg in pagemap.split(";"):
        text = " ".join(seg.split())
        if not text:
            continue
        m = _SEG_RANGE.match(text)
        if m and kept is not None:
            lo = int(m.group(1))
            hi = int(m.group(2)) if m.group(2) else lo
            if not (set(range(lo, hi + 1)) & kept):
                continue                     # this stretch is not the paper
        if _DISCLAIMER.search(text):
            continue                         # the annotator discounted this
        body = _NOT_EVIDENCE.sub(" ", text.lower())
        for name, tag in _NAME_TO_TAG.items():
            if re.search(rf"\b{name}\b", body) and tag not in found:
                found.append(tag)
    return found


def widen(doclang: str | None, pagemap: str, keeppages: str | None):
    """New doclang, or None if nothing is added.

    The existing tag stays first and keeps its script subtag: `de-Latf` is a
    fact about the letterforms that "german" in prose does not carry.

    **Only widens an existing label; never creates one.** 513 papers here have
    a pagemap but no `doclang`, and inferring one from prose would move them
    from detection to a pin — a strategy change, decided on the thinnest
    evidence in the file, and not what this script is for. They are reported
    instead.
    """
    existing = parse_doclang(doclang or "")
    if not existing:
        return None
    have_bases = {t.split("-")[0] for t in existing}
    extra = [t for t in languages_on_kept_pages(pagemap, keeppages)
             if t not in have_bases]
    if not extra:
        return None
    return ", ".join(existing + extra)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bib", type=Path, default=BIB)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default=None, help="one file = {...} value")
    a = ap.parse_args()

    entries = parse_bib(a.bib.read_text(encoding="utf-8"))
    changed = skipped_vert = unlabelled = 0
    for entry in entries.entries:
        fname = (entry.get("file") or "").strip()
        if not fname or (a.only and fname != a.only):
            continue
        pagemap = entry.get("pagemap")
        if not pagemap:
            continue
        if not entry.get("doclang"):
            unlabelled += 1
            continue
        new = widen(entry.get("doclang"), pagemap,
                    entry.get("keeppages"))
        if not new:
            continue
        ocrlang = (entry.get("ocrlang") or "")
        if "_vert" in ocrlang:
            # Derivation cannot reproduce a vertical pin and would undo it.
            skipped_vert += 1
            print(f"  {fname:34s} doclang widened, ocrlang left as {ocrlang!r}")
            entry.set_field("doclang", new)
            changed += 1
            continue
        derived = derive_ocrlang(new)
        print(f"  {fname:34s} {entry.get('doclang')!s:14s} -> {new:22s} "
              f"ocrlang {ocrlang or '-'} -> {derived}")
        entry.set_field("doclang", new)
        if derived:
            entry.set_field("ocrlang", derived)
        changed += 1

    print(f"\n{changed} entr(y|ies) widened"
          + (f"; {skipped_vert} kept a vertical ocrlang" if skipped_vert else ""))
    if unlabelled:
        print(f"{unlabelled} entr(y|ies) have a pagemap but no doclang and were "
              f"left alone —\n  giving them one would pin documents that "
              f"currently use detection, which is a\n  strategy change rather "
              f"than a correction.")
    if a.dry_run:
        print("dry run, nothing written")
        return 0
    a.bib.write_text(render_bib(entries), encoding="utf-8")
    print(f"wrote {a.bib}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
