# Gold-standard transcription protocol

You are producing a **gold-standard verbatim transcription** of scanned pages of
scientific literature, from page images. This is reference data that will be
used for years to measure whether text-extraction software is getting better or
worse. Accuracy matters far more than speed, and a wrong character is worse than
a marked uncertainty.

## Absolute rules

1. **Work from the page image only.** Do not read any other transcription of
   these pages — no `_provenance/` directory, no `summaries/`, no software
   output of any kind. Your transcription's value comes from being independent.
   If you have already seen another rendering of a page, say so in the page's
   `[NOTE: ...]` line rather than pretending otherwise.
2. **Transcribe what is printed, not what should have been printed.** Preserve
   the original orthography exactly: long-s (`ſ`), ligatures (`æ`, `œ`, `ﬁ`),
   historical spellings (`tems`, `efpece`, `Cöleneraten`), abbreviations,
   macrons (`côme`), and the author's own typos. Do **not** modernise, correct,
   expand, normalise, or translate anything.
3. **Never invent.** If you cannot read something, mark it. Do not guess a word
   because it is the plausible one. A gold standard with `[?]` in it is useful;
   a gold standard with a confident wrong reading is actively harmful.
4. **Transcribe every page in full**, including front matter, running heads,
   page numbers, footnotes, plate lettering, table contents, marginalia,
   library stamps and handwriting. If a page is blank, write
   `[BLANK PAGE]`.
5. **Damaged type is not orthography.** Where a sort is broken, under-inked or
   ink-filled so that the glyph images as some other letter, transcribe the word
   the compositor set and record the defect in a `[NOTE:]`. **Ruled 2026-08-25**
   on Carre1969 p. 326, where `Agalmidae` carries a broken `m` that images as
   `ın`: the correct transcription is `Agalmidae`.

   This does **not** loosen rule 2. A word the author or compositor *set* wrongly
   stays wrong — `Pseudoculanus`, `Ostrucoda`, `wamr`, `Texas AandM Press` are all
   transcribed as printed. The line is between **what the type says and what the
   ink did**: a misspelling is in the type, a broken sort is damage to this
   impression, and another copy of the same edition would show the letter intact.

   Rationale, so the reasoning survives the ruling: the `[NOTE:]` preserves the
   defect under either convention, so reading for intent loses no information —
   whereas transcribing the literal glyph (`Agalınidae`) makes the word
   unfindable by anyone searching the text. When you cannot tell damage from a
   genuine variant spelling, that is what `[?reading]` is for.

## Markup — keep it minimal and mechanical

Use only these markers, so the files stay close to plain text:

| marker | for |
|---|---|
| `[PAGE n]` | first line of every file — the printed folio if there is one, else `[PAGE n: unnumbered]` |
| `[RUNNING HEAD] text` | running head / caption title at head of page |
| `[FIGURE]` … `[/FIGURE]` | a figure; put its printed label and caption verbatim inside |
| `[PLATE]` … `[/PLATE]` | a full-page plate; list the printed figure numbers on it |
| `[TABLE]` … `[/TABLE]` | tabular matter; preserve columns with single tab characters |
| `[FOOTNOTE] text` | a footnote, placed where it appears on the page (foot), not inline in the sentence it refers to |
| `[MARGIN] text` | marginalia in the outer margin |
| `[?reading]` | uncertain reading — your best guess, flagged |
| `[illegible]` | a word or short run you cannot read at all |
| `[illegible: N lines]` | a larger unreadable region |
| `[STAMP] text` / `[HANDWRITTEN] text` | library stamps, accession marks, pencil annotations |
| `[NOTE: ...]` | anything a later reader needs to know: rotation, damage, bleed-through, a second article beginning mid-page |

Otherwise: plain paragraphs separated by blank lines. **Preserve reading order.**
For multi-column pages transcribe the full left column, then the full right
column — never interleave them. Do not preserve the original line breaks inside
a paragraph; join wrapped lines into continuous prose and **silently remove the
hyphen** from words broken across a line (`Sipho-\nnophora` → `Siphonophora`).
Keep line breaks where they are semantically real: verse, table rows, lists,
reference-list entries, headings.

## When the source itself uses square brackets — MANDATORY note

Several documents in this corpus use `[...]` in their own printed text, which
collides with this protocol's marker syntax. Totton1965a does it throughout —
`[? Praya maxima Ggbr.]`, `[? Prayidae]`, a bare `[?]` — as the author's own
interpolated editorial queries. Carre1969's English translator does the same
beside terms he could not resolve.

**Whenever a page carries printed square brackets, add a `[NOTE: ...]` listing
where they occur**, e.g.

```
[NOTE: printed square brackets are the author's own on this page: "[? Prayidae]"
after Nectopyramis, and a bare "[?]" in the third paragraph. They are not
transcriber markers.]
```

This is not optional bookkeeping. Without it, a later reader counting
`[?reading]` / `[illegible]` to measure transcription confidence cannot tell our
uncertainty from the author's, and the per-page note is the only thing that
separates them. Transcribe the printed brackets exactly as printed — never alter
or annotate inside them.

## Special cases

- **Rotated pages.** Note the rotation, then transcribe as if correctly
  oriented. `[NOTE: page printed landscape, rotated 90° CW]`
- **Plates with no text.** Still record every engraved number and letter you can
  see, in reading order, inside `[PLATE]`. These are often the only labels that
  exist. If a plate carries no text at all, say `[PLATE] [no lettering] [/PLATE]`.
- **Non-Latin scripts.** Transcribe in the original script — Cyrillic as
  Cyrillic, Han as Han, Greek as Greek. Do not transliterate. For vertical
  Japanese, transcribe in the correct reading order (top-to-bottom,
  right-to-left) and note `[NOTE: vertical RTL type]`.
- **Fraktur / blackletter.** Transcribe to the modern Latin letters the
  blackletter glyphs *represent* (so `ſ`→`ſ` is kept, but `ﬅ` is `ſt`; the
  Fraktur letter that looks like `f` but is `s` must be transcribed `s`). This
  is the single most error-prone case in this corpus — go slowly.
- **A page holding more than one article.** Transcribe all of it, and mark the
  boundary with `[NOTE: new article begins here: <its title>]`.

## Where to write

One file per page:

```
transcriptions/<stem>/page_NNN.txt
```

`NNN` is the 1-based **PDF** page index, zero-padded to 3 (`page_007.txt`), not
the printed folio. Write each page's file as soon as you finish that page — the
work is resumable and interruptions are expected, so never hold several pages
in memory before writing.

**If a page file already exists and is non-empty, skip that page.** Someone
else did it.

## Rendering pages

```bash
python3 transcriptions/scripts/render.py <stem> /tmp/gold/<stem> "1,4,7-9"
```

Then view each emitted PNG with the `Read` tool. Renders are 300 dpi via
poppler's `pdftoppm`. Re-rendering is free — existing PNGs are reused. If type
is too small to read confidently, crop and magnify rather than guessing:

```bash
python3 transcriptions/scripts/crop.py <png> <out.png> 0.0 0.4 1.0 0.75
```

(fractional left, top, right, bottom of the page — the example takes a
horizontal band across the middle.)

## Finishing

When your assigned pages are all written, reply with: the pages you completed,
a count of `[?reading]` and `[illegible]` markers you used, and any page you
think needs a second opinion. Flagging your own low-confidence pages is
expected and valued — it is how the second pass gets targeted.
