#!/usr/bin/env python3
"""Emit the next transcription job, sized to a page budget.

The gold-standard transcription runs ONE agent at a time, deliberately: agents
are interrupted by session limits, and a serial chain means an interruption
costs at most the single job in flight. Everything else is already on disk,
because the protocol requires writing each page as it is finished.

Jobs are document-scoped wherever possible — keeping one document with one
agent keeps its transcription conventions consistent — and only spill into a
second document when the first is smaller than the budget.

Usage:
    next_job.py [--budget 25]
"""
import argparse
import json
import pathlib

# Resolve the data root from this file's location so the tree works in any
# checkout. Layout: <data>/scripts/<this file> and <data>/<stem>/page_NNN.txt
DATA = pathlib.Path(__file__).resolve().parent.parent


def document_dirs():
    """Every transcribed document directory, in name order.

    Filters on containing page files rather than excluding `scripts/` by name,
    so a future sibling directory cannot silently be treated as a document.
    """
    return sorted((p for p in DATA.iterdir()
                   if p.is_dir() and any(p.glob("page_*.txt"))),
                  key=lambda p: p.name)

ROOT = DATA
CLASS = DATA / "page_class.json"

# MODEL POLICY: OPUS ONLY. Sonnet was trialled on job 22 (Vanhoeffen1906
# pp. 14-31 + Beklemishev1969 pp. 1-7) and failed on the one rule a gold
# standard cannot bend: it INVENTED content. 26 `[FIGURE]` blocks across 8 pages
# carried English descriptions of what the figure depicts — e.g.
#   [FIGURE] Fig. 31. [full colony of Agalmopsis elegans Sars, showing the row
#            of swimming bells and, below, the stem groups]
# — none of which is printed anywhere on the page. The 13 Opus-transcribed pages
# of the SAME document, under the same protocol, produced zero such inventions
# and emitted only the printed label. Same document, same instructions, so this
# is a tier difference, not a protocol ambiguity. The 8 affected pages were
# deleted and re-queued for Opus.
#
# The cross-check reasoning below was sound in principle but rested on a false
# premise: that a weaker transcriber's errors would be *detectable* by diffing
# against poppler. Invented prose that is fluent and plausible does not diff
# as an error against a text layer that simply lacks it — it reads as the
# transcriber having recovered something the OCR missed, which is exactly what
# we want the gold standard to do on hard pages. The failure mode is invisible
# to the verification design, so the tier cannot be used.
#
# Kept below for the record. Do not re-enable without a check that specifically
# detects invention.
_HISTORICAL_ONLY__whether_the_poppler_cross_check_can_catch_an_error = True

# Model tier per document. The deciding factor is NOT raw difficulty but
# whether the poppler cross-check can actually catch a transcription error.
# Where the PDF's baked-in text layer is decent (born-digital, or a scan whose
# third-party OCR is sound), a diff against it is a real safety net and a
# cheaper transcriber is safe. Where that layer is garbage (roman OCR over
# Fraktur scores 9k chars/page and disagrees everywhere, triaging nothing) or
# absent entirely, pass-1 quality IS the gold standard and errors are permanent
# and unmeasurable. Adjudication of disagreements is always the stronger model.
OPUS_ONLY = {
    # Fraktur — baked-in layer is roman-over-Fraktur garbage
    "Eschscholtz1825", "Keferstein_Ehlers1860", "Chun1882c", "Olfers1824",
    # no text layer at all — zero cross-check signal
    "Hjortberg1769", "LoBianco1909", "Adanson1757",
    # CJK: vertical RTL reading order, Kangxi-radical traps
    "Kawamura1911a", "Yamamori2014", "Chenetal2015",
    # Cyrillic homoglyph boundary (А В Е К М Н О Р С Т Х vs Latin)
    "Bernstein1934", "Stepanjants2014", "Stepanjants_Dianov1997",
    "Margulis1976a_Atlantictr", "Margulis1984b_Lcamptr",
    # broadsheet tables, plates, very low-res scans
    "Linnaeus1735", "Quoy_Gaimard1834Plates", "DeHaan1827", "Tilesius1814",
    "Lery1594", "Barrere1741",
    # Added after review: the French half (pp. 1-21) carries TWO overlaid OCR
    # passes (Acrobat 5 from 2005 + Acrobat 8.1 from 2020), so poppler's text is
    # doubled and doubly corrupted and the cross-check cannot catch an error;
    # four of those pages yield near-zero characters (p13 = 0). Its English half
    # (pp. 22-39) would qualify for sonnet, but per-half routing is not worth the
    # complexity for 18 pages — over-serving them is the cheaper mistake.
    "Carre1969_Nanomia_tr",
}

# DEFERRED: reproducibly blocked by an API output content filter, twice, on p1.
# Jobs 30 and 31 both died mid-document with
#   API Error: 400 Output blocked by content filtering policy
# each time while transcribing Mańko p1. The page is the first page of a 2020
# Progress in Oceanography paper on gelatinous zooplankton in the Fram Strait —
# there is nothing objectionable on it, so this is a false positive in the
# filter, not a property of the source. But because a blocked page kills the
# whole job, leaving this document in the queue cost two full 25-page jobs.
#
# RESOLVED 2026-08-24: the block did not reproduce. Given p1 ALONE, it
# transcribed cleanly on the first attempt. The remedy prescribed above — a
# dedicated single-page attempt — had never actually been run; both failures were
# 25-page jobs that happened to contain p1, and because DEFERRED moved the
# document to the END of the ordering rather than removing it, this planner kept
# offering p1 as budget filler at the tail of large jobs, risking 25 pages to
# test one. DEFERRED is now empty: its only purpose was to stop p1 poisoning
# batches. See groundtruth/SECOND_OPINION_QUEUE.md.
#
# If a document ever needs deferring again, note the sharp edge: entries here are
# reordered, NOT excluded, so a deferred document still reaches jobs as filler.
DEFERRED = set()

# Hardest first: these carry the most information per page about whether an
# extractor is improving, so they should exist even if the run is abandoned.
PRIORITY = [
    "Olfers1824", "Chun1882c", "Eschscholtz1825", "Keferstein_Ehlers1860",
    "Hjortberg1769", "LoBianco1909", "Linnaeus1735", "Tilesius1814",
    "Quoy_Gaimard1834Plates", "Chenetal2015", "Stepanjants_Dianov1997",
    "Stepanjants2014", "Bernstein1934", "Kawamura1911a", "Margulis1976a_Atlantictr",
    "Carre1969_Nanomia_tr", "Carre1968_Hippopodius_tr", "DeHaan1827",
    "Vanhoeffen1906", "Beklemishev1969", "Chun1898b", "Mackie1966",
    "Gasca_Suarez1993tr", "Totton1965b", "Benasso_Stroiazzo1976",
    "Candeias1932", "Hosiaetal2024", "Ahuja_etal2026", "Mańko_et al2020",
    "Totton1965a",
]


def fmt_ranges(nums):
    if not nums:
        return ""
    out, start, prev = [], nums[0], nums[0]
    for x in nums[1:]:
        if x == prev + 1:
            prev = x
            continue
        out.append(f"{start}" if start == prev else f"{start}-{prev}")
        start = prev = x
    out.append(f"{start}" if start == prev else f"{start}-{prev}")
    return ",".join(out)


def outstanding(stem, npages):
    d = ROOT / stem
    todo = []
    for i in range(1, npages + 1):
        f = d / f"page_{i:03d}.txt"
        if not (f.exists() and f.stat().st_size > 0):
            todo.append(i)
    return todo


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=25)
    a = ap.parse_args()

    cls = json.loads(CLASS.read_text())
    npages = {k[:-4] if k.endswith(".pdf") else k: v["pages"] for k, v in cls.items()}
    order = ([s for s in PRIORITY if s not in DEFERRED]
             + [s for s in sorted(npages) if s not in PRIORITY and s not in DEFERRED]
             + sorted(DEFERRED))

    job, used = [], 0
    for stem in order:
        if stem not in npages:
            continue
        todo = outstanding(stem, npages[stem])
        if not todo:
            continue
        take = todo[: max(1, a.budget - used)]
        job.append((stem, take))
        used += len(take)
        if used >= a.budget:
            break

    if not job:
        print("ALL PAGES COMPLETE")
        return

    tier = "opus"   # see MODEL POLICY at the top of this file
    print(f"# next job — {used} pages — model: {tier}")
    for stem, pages in job:
        m = "opus"  # see MODEL POLICY at the top of this file
        print(f"{stem}\t{fmt_ranges(pages)}\t({len(pages)} pp)\t{m}")


if __name__ == "__main__":
    main()
