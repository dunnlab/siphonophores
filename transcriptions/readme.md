# Transcriptions

Verbatim, page-by-page transcriptions of documents in this library, made
independently of any software extraction. One directory per document.

**Coverage: 35 documents, 761 pages, spanning 1594–2026 and 13 languages.** The
library holds 1,808 bib records, so this is a small and deliberately chosen
slice — picked to span the collection's hardest axes of variation (Fraktur,
Cyrillic, CJK, vertical Japanese, plate-only atlases, documents with no text
layer at all, bilingual translations). It is expected to grow: adding a document
means adding one `<stem>/` directory.

The first purpose was measuring whether PDF text-extraction software is getting
better or worse, and the transcriptions are still good for that. But they are a
general-purpose reading of the documents themselves and are not tied to that use.

**Status: first pass complete for all 35 documents, 761/761 pages
(2026-08-24). No page has yet had a second independent reading** — see *Known gaps*.

Everything here depends on the independence guarantee below.

## The independence guarantee

Every page was transcribed **from a rendered image of that page only**. The
protocol forbids the transcriber from opening `_provenance/`, `summaries/`, or any
software extraction of the page it is working on. No page in this set was produced
by correcting, editing, or reviewing an extractor's output.

This is what makes the set usable as ground truth: an extractor evaluated against
it is not being compared to a cleaned-up version of itself. **Anyone editing these
files must preserve that property** — do not "fix" a gold page by consulting a
tool's output for the same page.

## What is here

| path | what it is |
|---|---|
| `<stem>/page_NNN.txt` | **authoritative.** One file per page, `NNN` = 1-based PDF page index |
| `<stem>.txt` | derived concatenation, regenerate with `scripts/assemble.py` |
| `manifest.json` | page counts, per-page SHA-256, per-document totals |
| `confidence.json` | phase-1 cross-check metrics, per page |
| `CROSSCHECK_REPORT.md` | what the cross-check found, and why its signal is weak here |
| `SECOND_OPINION_QUEUE.md` | pages flagged for review, per job, with reasons |
| `TRANSCRIPTION_PROTOCOL.md` | the rules every transcriber followed |
| `RUNBOOK.md` | how the work was run; failure modes worth knowing |
| `sources.json` | which PDF in this repo each `<stem>/` transcribes, with its sha256 |
| `scripts/` | tooling: `status.py`, `assemble.py`, `crosscheck.py`, `render.py`, `crop.py`, `next_job.py` |

`sources.json` is not decoration. Most sources sit in `library/<LETTER>/`, but
some are in `nonlibrary/others/`, `nonlibrary/translations/` or
`library/orphans/`. Resolve a stem through it rather than guessing a shelf; the
recorded sha256 pins the exact file.

**The case that makes this necessary: `Lery1594.pdf` exists twice in this repo,
under the same name, holding *different pages of the same book*.**

| file | pages | content |
|---|---|---|
| `nonlibrary/others/Lery1594.pdf` | title leaf + ff. 357–358 | the siphonophore passage |
| `library/L/Lery1594.pdf` | title leaf + ff. 395–399 | Chap. XXII, the return voyage |

**The transcription here is of the `nonlibrary/others/` file, and that is
deliberate** — folio 358 carries the observation this document is in the corpus
for: `Immondicitez rouges nageâs ſur mer`, red floating things "faites de la
meſme façon que la creſte d'vn coq", venomous enough that touching them left the
hand red and swollen. That is *Physalia*, and at 1594 it is the oldest record in
the collection. The `library/L/` excerpt does not contain it.

The bib entry agrees (`[pp. 357-358 scanned]`), even though its `file` field
resolves by shelf convention to the `library/L/` copy — a filing inconsistency in
the library, not in this transcription. Do not "correct" `sources.json` to point
at `library/L/`; that would silently substitute a passage with no siphonophore
content.

**Not committed:** `<stem>/_provenance/poppler/` — poppler's own extraction of
each page, used only as a cross-check input. It is derived from PDFs already in
this repo, so it is gitignored. Regenerate with:

```bash
python3 scripts/crosscheck.py --regen-provenance   # needs `git lfs pull` first
```

Verify a concatenation still matches its pages:

```bash
python3 scripts/assemble.py --check
```

Both of the above run from this directory. The scripts locate the data relative
to themselves, so any checkout works.

## Method

1. **Render** each page to PNG at **300 dpi** with `pdftoppm` (poppler 24.02.0).
2. **Transcribe** from the image under `TRANSCRIPTION_PROTOCOL.md`: verbatim,
   preserving original orthography (long-s `ſ`, ligatures, historical spellings,
   the author's own typos), marking uncertainty rather than guessing, and writing
   each page to disk the moment it is finished.
3. **Cross-check** (phase 1) against `pdftotext` output for the same page →
   `confidence.json`.
4. **Assemble** (phase 3) → `<stem>.txt` + `manifest.json`.

**Model policy: Opus only.** A cheaper tier was trialled and **invented content** —
26 `[FIGURE]` blocks across 8 pages carried English descriptions of what the figure
depicts, none of it printed on the page. Those pages were deleted and re-done. The
failure is invisible to the cross-check, because invented prose does not diff as an
error against a text layer that simply lacks it. Do not relax this without a check
that specifically detects invention.

## Markup

Minimal and mechanical: `[PAGE n]`, `[RUNNING HEAD]`, `[FIGURE]`/`[/FIGURE]`,
`[PLATE]`/`[/PLATE]`, `[TABLE]`/`[/TABLE]`, `[FOOTNOTE]`, `[MARGIN]`, `[STAMP]`,
`[HANDWRITTEN]`, `[?reading]`, `[illegible]`, `[NOTE: ...]`. Full rules in the
protocol. Corpus totals: **348 figure, 76 plate, 65 table blocks; 422 `[?reading]`,
314 `[illegible]`** over 139 pages.

Four things a consumer needs to know:

1. **Captions are in-line**, inside the `[FIGURE]` block at the figure's position
   in reading order. Only *printed* text goes inside — never a description of what
   an illustration depicts.
2. **Caption and figure are in the same file only when the source prints them on
   the same page.** Totton1965a's plate section (pp. 250–314) uses facing leaves:
   the caption leaf carries the `FIG. n.` entries as ordinary text with **no
   `[FIGURE]` block**, and the plate leaf carries the `[PLATE]` block. Joining them
   requires the odd/even leaf pairing, mapped in `SECOND_OPINION_QUEUE.md`.
3. **Three syntactic variants of the opening tag exist** and a parser must handle
   all three: `[FIGURE]` alone on its line (329 blocks); `[FIGURE] <caption>` on
   one line then `[/FIGURE]` (~19, in Vanhoeffen1906 and Beklemishev1969); and
   open+content+close on a single line (1, Bernstein1934 p1).
4. **`[PAGE n]` records the PRINTED FOLIO, not the PDF index.** DeHaan1827's PDF
   page 9 opens `[PAGE 496]`. Address pages by filename or by the `<<<PAGE pdf=N>>>`
   delimiter in the concatenations.

## Known gaps and limits

- **No page is missing.** All 761 are transcribed; there are no documented gaps.
- **89 pages have no mechanical cross-check.** 77 `no_signal` (poppler returns
  <50 chars — this is all of Adanson1757, Hjortberg1769 and LoBianco1909, at
  1 char/page) and 12 `script_lost` (the page's writing system is absent from the
  text layer). These rest entirely on pass 1.
- **Kawamura1911a is bilingual** — an English translation followed by the original
  1911 vertical Japanese from p14. Poppler reads the kanji as roman noise, so the
  Japanese half has no cross-check while the English half agrees at 0.998.
- **The cross-check found zero gold omissions**, and its scores should not be used
  to target review. See `CROSSCHECK_REPORT.md` for the five poppler failure modes
  that generate false positives.
- **422 `[?reading]` and 314 `[illegible]` markers are deliberate.** They record
  what could not be read with confidence. Totton1965a p282/p304 were checked
  against the physical volume on 2026-08-24: the lettering is small type poorly
  reproduced *in the original 1965 printing*, so those markers are correct and
  final. **Marker density is often a property of the source, not of the
  transcription.**
- **Source errors are preserved, not corrected**, each with a `[NOTE:]` — e.g.
  Mańko_et al2020's reference list spells the same author `Beszczynska-Möller` and
  `Beszczyńska-Möller` on one page, and prints `Texas AandM Press`. An extractor
  that silently normalises these is destroying signal this corpus exists to
  measure.
- **Not yet done:** phase 2, the targeted second pass. No page here has been read
  by a second independent transcriber.

## Tool versions

poppler **24.02.0** (`pdftoppm`, `pdftotext`) · renders at **300 dpi** ·
Python 3.12 · scripts in `scripts/` (`render.py`, `crop.py`,
`status.py`, `next_job.py`, `crosscheck.py`, `assemble.py`).
