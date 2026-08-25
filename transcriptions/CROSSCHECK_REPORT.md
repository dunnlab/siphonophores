# Phase 1: poppler cross-check — result

**Date:** 2026-08-24 · **Input:** 761 gold pages vs `_provenance/poppler/`
**Output:** `confidence.json` (per-page metrics) · **Script:** `scripts/crosscheck.py`

The comparison layer is `pdftotext -layout` (poppler 24.02.0), regenerable with
`scripts/crosscheck.py --regen-provenance`. **The `-layout` flag is part of the
result, not an incidental detail.** It preserves the page's spatial arrangement,
so a two-column page is emitted with its columns interleaved line by line — which
depresses the order-sensitive `similarity` score while saying nothing whatever
about the gold page. Dropping the flag raises `similarity` on 532 of 761 pages
(e.g. Ahuja p2: 0.48 → 0.98) without changing a single page's *status*. Failure
mode 2 below is largely a consequence of this choice, and it is the reason the
order-insensitive `coverage` and `recall` measures were added beside it.

## Headline

**The cross-check found no gold omissions.** Every page whose score suggested
disagreement was traced to a failure of the *poppler* layer, not of the gold
transcription. The funnel:

| stage | pages |
|---|---|
| all pages | 761 |
| scorable at all (excludes no-signal and script-lost) | 672 |
| low sequence similarity | 83 |
| ...after removing reading-order artifacts | 73 |
| ...after removing pages where gold correctly has *more* | 54 |
| ...after removing poppler noise-hallucination | 16 |
| ...after removing OCR damage to words the gold already has | **9** |
| **confirmed gold omissions after inspecting all 9** | **0** |

## Why the signal is weak here, in five distinct failure modes

Each was found by inspecting pages, and each would have sent a second pass to
re-transcribe text that is already correct.

1. **The text layer is garbage.** Roman OCR over Fraktur scores ~0.05–0.15 and
   disagrees everywhere: Chun1882c, Olfers1824, Eschscholtz1825,
   Keferstein_Ehlers1860. Nothing can be triaged in these documents.
2. **Reading order is scrambled, content is fine.** `SequenceMatcher` punishes
   reordering as hard as wrong content. Hosiaetal2024 scores 0.59 on sequence
   and **0.99 on order-insensitive coverage** — poppler interleaves its two
   columns. 21 pages that would have been false positives. Mackie1966: 0.36 vs
   0.97.
3. **The gold correctly contains MORE than the text layer.** Whole-page figures
   and engraved plates carry lettering that exists only as image. Ahuja_etal2026
   p8: **1007 gold words vs 54 in poppler**, which has only the caption.
   Quoy_Gaimard's plates likewise. Flagging these penalises the gold for being
   better than the extractor.
4. **Poppler hallucinates text from image texture.** Linnaeus1735 p1 is the
   binding's marbled front board; the gold correctly records the 6 words of
   gilt spine lettering, and poppler's "70 words" are noise
   (`¿rit 1H äL «•f ^H`).
5. **OCR damage to words the gold already has.** The largest class. DeHaan1827
   renders `Vdica integra ; bnchia bafi ramofa t aequllia` where the gold
   correctly reads `Veſica integra ; brachia baſi ramoſa , aequalia`.
   **Long-s typography guarantees this**: poppler reads `ſ` as `f`, so nearly
   every word of an 18th-century text mismatches. Stepanjants2014 shows the
   Cyrillic form of the same thing — `fюнофор` for *сифонофор*, `сификаuии`
   for *классификации*.

The 9 finalists were inspected individually. Their "novel" words are all OCR
salad — `coluloia`, `niiiurilm`, `capajdla`, `auluaunc`, `vlviparona` — including
`clfholtz`, which is *Eschscholtz* mangled past any matching threshold.

## Metrics in `confidence.json`, and how to read them

Per page: `similarity` (order-sensitive), `coverage` (gold∩poppler / gold),
`recall` (gold∩poppler / poppler), `excess_wordlike`, `excess_novel`,
`volume_ratio`, `uncertain`, `illegible`, `status`.

**`recall` is the one that matters for finding omissions** — it asks how much of
what poppler saw is *missing from the gold*. That is the shape of a real
omission, and it is what Mańko p9 looked like when a session limit killed the
agent mid-page and both prose columns were lost. `coverage` asks the opposite
question and is lowest precisely where the gold is best.

Statuses that mean "not comparable" rather than "disagreement":

- `no_signal` (77 pages) — poppler returns <50 chars. Includes all of
  Adanson1757, Hjortberg1769 and LoBianco1909, which sit at 1 char/page.
- `script_lost` (12 pages) — the page's writing system is absent from the text
  layer. **Kawamura1911a pp.14–22** is the case that matters: the document is
  **bilingual**, an English translation followed by the original 1911 vertical
  Japanese, and poppler reads the kanji as roman noise (`H5-h>^ -^pg-f`). Its
  English half cross-checks at 0.998. This is a *per-page* property; a
  document-level exclusion list misses it.

## What this means for phase 2

**Do not drive the second pass from these scores.** They contain no confirmed
targets, and each of the five failure modes above generates false positives that
would cost a re-transcription and return identical text.

The real targeting inputs are:

1. **`SECOND_OPINION_QUEUE.md`** — pages the transcribers flagged themselves,
   including pages that used *no* markers but were worked at the legibility
   limit and so appear in no grep.
2. **The 77 `no_signal` + 12 `script_lost` pages**, which have no mechanical
   check of any kind and rest entirely on pass 1.
3. **NOT** marker-dense plate pages. Checked against the physical volume
   2026-08-24: Totton1965a p282/p304's unresolved lettering is small type poorly
   reproduced *in the original 1965 printing*. The uncertainty markers are
   correct and final. A marker-dense plate page is often recording a defect in
   the source, and re-transcribing it reproduces the same markers at the same
   cost.

## Caveats this cannot address

The cross-check **cannot detect invention** — fluent invented prose does not diff
as an error against a layer that simply lacks it; it reads as recovery of
something the OCR missed. That is why pass 1 was Opus-only rather than relying on
this diff as a safety net, and it remains the reason the model policy cannot be
relaxed.
