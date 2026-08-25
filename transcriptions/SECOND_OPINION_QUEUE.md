# Pages flagged by pass-1 transcribers for a second opinion

Appended as each job reports. These feed the phase-3 targeted second pass
(always Opus), together with pages the poppler cross-check flags. A transcriber
flagging its own low-confidence page is expected and valued — it is the main
input to targeting.

## Job 14 (2026-08-09)

- **`Bernstein1934/page_010.txt`** (printed 42) — *highest priority*. Right edge
  destroyed by scan shadow at the gutter of the p.42–43 table spread. Could not
  decide whether traces inside the shadow are a genuine ninth data column
  (station 13's shallowest horizon) or a sliver of the facing page caught in the
  scan. Recorded as a trailing `[illegible]` column with a `[NOTE:]`. A rescan or
  a different render of that edge would settle it. 26 of the document's 30
  `[illegible]` markers are on this page.
- **`Bernstein1934/page_004.txt`** (printed 5) — rotated station map; island
  lettering at the limit of legibility at 300 dpi. Four labels flagged
  `[?reading]`; unclear whether periods after `О` are printed.
- **`Bernstein1934/page_008.txt`** (printed 40) — station 4's latitude has a lost
  leading digit. Also worth confirming the deviant spellings `Pseudoculanus` /
  `Ostrucoda` / `digitule` on this page, since the same words are set normally
  two pages later.

### Source defects recorded, not transcription problems

- **Bernstein1934 is not page-continuous in this PDF.** Printed folios run
  1 (title), 4, 5, then jump to 8, 9, then to 27, then 40–55. Printed pages 6–7
  and 10–26 are absent from the file. Noted in the affected page files. Any
  extractor reporting continuous pagination for this document is wrong.
- Original typos preserved rather than silently corrected: author line prints
  **`Т. БЕРНШТЕЙП`** (П, not Н); `Pseudoculanus`, `Ostrucoda`,
  `Aglantha digitule` (p.40) against standard spellings on pp.41–44;
  `Microlanus pygmaeus`, `forgende`, `Im Iahre`, `Euchaeta glacjalis`,
  `Homoeonema platigonon`; Kramp's page range set as `3 15—384`; a line-break
  hyphen set as a comma in Lohmann's `Bezie,/hungen`; a sentence-final stop set
  as a hyphen after `Metridia longa`.
- Bernstein's Таблица 5 is a **two-page spread**: p.42 carries the species stubs,
  p.43 carries only numbers. p.43's rows were left unlabelled as printed, in the
  same species order.
- **Stepanjants2014 p.354**: the reference list mixes Latin `P.`/`S.` with a
  Russian page abbreviation appearing as `С.`/`с.`. Lowercase `643 с.` is
  decisive for Russian «страницы», so Cyrillic `С/с` was used there and Latin for
  `P.`/`S.`, with a `[NOTE:]` recording the decision.

## Job 18 — Carre1969_Nanomia_tr pp. 2-26 (2026-08-10)

- **`page_013.txt`** (Planche III) — the worst page in the document: nine halftone
  panels, the author's small-caps signature broken past the point where accents
  resolve, one leader truncated by the block edge (`[?End.]`), one panel number
  swallowed by a black area (`[?9]`). Confirm whether the ninth panel is in fact
  numbered.
- **`page_012.txt`** — the Fig. 5 scale bar reads `60 µ` in blobby bold; the
  leading digit could conceivably be a 4. Transcribed as 60 *unflagged*, at ~85%
  confidence — so this one will not show up in a `[?reading]` grep.
- **`page_002.txt`** — "autres Agalmidae" in the Metchnikoff quotation is printed
  with a **broken `m`** that images as `ın`. Transcribed as the intended
  `Agalmidae` on the reading that ink damage is not orthography, with the defect
  documented in place. A reviewer who takes the opposite view would want the
  literal glyph sequence instead. Worth a policy decision for the whole corpus.

### Methodological hazard — do not grep `[?` naively

`page_022.txt` contains **six `[?]` strings that are the translator's own printed
marks**, not transcriber uncertainty — the English translation prints bracketed
question marks beside terms its author could not resolve ("Prenant's trichromic
[?]", "pH3 [?]", "Unna Brachet reaction [?]"). They are transcribed literally and
flagged in that page's `[NOTE:]`. Any script counting transcriber uncertainty must
distinguish `[?reading]` / `[illegible]` from a bare `[?]`.

### Structural facts recorded

- **French/English boundary confirmed at p21/p22**, from the pages rather than
  assumed: the French article ends on PDF p21 (printed folio 341, with the
  Zusammenfassung, bibliography and abbreviation list); the English translation
  begins on PDF p22 with its own title block and **restarts pagination at 1**.
- **The French half carries two independent numbering systems**: the journal
  folios 326-341, and a lone typeset digit at the foot of each *text* page
  (2, 3, 4 … 17) which is the compilation's leaf count. The leaf digits are
  absent from every plate page, which is why **printed folio 337 never appears in
  this PDF**. Transcribed on their own line with a note.
- **Four plate/caption separations**, marked in place rather than reunited:
  Planche II on p10 / caption p11; Planche III on p13 / caption **p12** (caption
  precedes figure); Fig. 7 on p17 / caption p16; Planches I and IV carry their own.
- **Figure duplication confirmed and preserved.** Figs. 1-7 and Planches I-IV each
  appear twice, once per half, from the same source raster. In the English half the
  figures are scans of the French plates, so **their internal lettering is still
  French** while the caption beneath is the translator's English — noted on pp24-25.
  Nothing was deduplicated.
- Printed errors preserved rather than corrected, on both sides. French:
  `caracrisée` for *caractérisée* (p20), unaccented capital `A` in "A maturité",
  `PH 3` on p2 vs `pH 3` on p18, German summary misprints `Entwicktungs`,
  `vervandelt`, `zelen`, `vorteren`. English: `Gube's fuchsin paraldehyde` (for
  Gabe's), `glychémalun` vs `glychémaun` in adjacent paragraphs, `The have been
  maintained`, `Halistemma picta` then `Halistemma pictum`, `twentyish oocytes`,
  `vascular internal cells` (for *vacuolisées*), `none other that`, and
  `Pl. II, 2,3` where the French reads `Pl. II, 1, 2, 3`.

## Job 24 — Beklemishev1969 pp. 19-43 (2026-08-11)

- **`page_035.txt`** — *highest risk in this batch*. Three marginalia clusters run
  off the scanned left edge; `[?lacenta]` is probably "placenta" but its opening
  letters are physically absent, and a third cluster is unreadable loose cursive.
  Only the physical book resolves this. 8 of the batch's 20 `[illegible]` are here.
- **`page_038.txt`** — the three-line pencil note at the foot: `[?However]
  [?plaques]` / `Salps mediate` / `[?behavioural] signals`. "Salps mediate" is
  solid; lines 1 and 3 are not.
- **`page_033.txt`** — worth confirming against the original leaf that the loss is
  genuinely unrecoverable rather than an artifact of this render's page box.

All 7 `[?reading]` markers in this batch are on *handwritten* marginalia, none on
printed type.

### Open convention question — hyphenated compounds

The protocol says to remove the hyphen from words broken across a line
(`Sipho-\nnophora` → `Siphonophora`). This transcriber removed line-end hyphens
inside single words (`coelomo-ducts` → `coelomoducts`) but **kept** them in
two-word compounds the book genuinely hyphenates (`strictly-defined`,
`newly-formed`, `highly-colonial`), on the reading that the rule covers broken
words, not hyphenated compounds. That is the right call, but the protocol does not
say so explicitly and **pp. 1-18 of this same document were done by a different
agent** — worth a consistency check, and worth writing the distinction into the
protocol.

### Traps for automated heuristics, recorded

- **Printer's signature marks are not folios.** A bare `32` sits at the foot of
  p22 and `33` at the foot of p38, in folio position but signature marks, not page
  numbers. An obvious false positive for folio detection.
- **p33 carries no printed folio at all** (recorded `[PAGE 33: unnumbered]`), while
  pp. 19-43 otherwise map continuously to printed folios 464-488.
- **Fig. 211's caption straddles pp. 21-22**, breaking mid-word at `1—` with a
  printed `[continued opposite`; the remainder sits below a rule at the foot of p22
  with no figure on that page. Both halves transcribed where they physically appear.
- **p33 is amputated on BOTH sides**, not just the right as previously recorded:
  the right end of every caption line is gone *and* all but a sliver of panel A is
  cut away at the left, leaving only its labels `1, 2, 3`.
- Printer's/author's errors preserved as printed, each flagged in place so a later
  reader does not score them as transcription errors: `recontruction` (p20, from a
  broken `re-contruction`) and the genus `Tripostega` (p19; modern spelling is
  *Trypostega*).

### Tooling note

`pdftoppm` renders Beklemishev p33 landscape; it needs a **90° clockwise** turn to
read. PIL's `rotate(90)` turns the wrong way — use `rotate(-90)` or
`transpose(Image.ROTATE_270)`.

## Content-filter blocks — Mańko_et al2020 (2026-08-12 → 2026-08-24)

**The block did not reproduce. p1 transcribed cleanly on an isolated attempt,
with no content-filter error at any point.**

History: two consecutive jobs (30 and 31) died with

```
API Error: 400 Output blocked by content filtering policy
```

each time while transcribing **Mańko p1**, once reported as failing on "the
introduction columns and footer". The page is the opening page of *Footprints of
Atlantification in the vertical distribution and diversity of gelatinous
zooplankton in the Fram Strait (Arctic Ocean)*, Progress in Oceanography 189
(2020) 102414 — a routine zoology paper with nothing objectionable on it. That
diagnosis was right: **a false positive in the output filter, not a property of
the source.**

What was missed for twelve days is that the prescribed remedy — a *dedicated
single-page* attempt — was never actually performed. Both failures were 25-page
jobs that happened to contain p1, and because `DEFERRED` moves a document to the
END of the planner's ordering rather than removing it, `next_job.py` kept
offering p1 as budget filler at the tail of large jobs. Each attempt therefore
risked 25 pages to test one. **Given p1 alone, it succeeded on the first try**
(3 ordinary appends, 0 `[?reading]`, 1 `[illegible]` — the masthead cover
thumbnail).

**Lesson worth keeping:** a transient API-level failure was recorded as a
property of the document, and the record then justified not retrying it. When a
failure is diagnosed as a false positive, the cheap isolated retry is the thing
to actually run, not to schedule and leave.

Consequence: `DEFERRED` has been removed from `scripts/next_job.py`, since
its whole purpose was to stop p1 poisoning batches. No page of this document is a
documented gap.

### CORRECTION (later the same day): the block is NOT p1-specific

**It reproduced a third time**, on the job covering pp. 9-17, while the agent was
working around **pages 13-14** — nowhere near p1. So the earlier conclusion that
the p1 failures were "transient rather than anything in the source" was too
strong on one point: the *page* attribution was wrong, but the *diagnosis* holds.
Corrected reading of all three events:

| # | job | reported location | page left truncated? |
|---|---|---|---|
| 1 | job 30 | p1, "introduction columns and footer" | unknown |
| 2 | job 31 | p1, same | unknown |
| 3 | pp. 9-17 job | pp. 13-14 | **no** — p13 verified complete, both columns |

p1 itself transcribes cleanly and repeatedly. So this is **not** a property of any
one page: it is an output-filter false positive that this paper's subject matter
(Arctic gelatinous zooplankton, fish stomach contents, reproduction and spawning)
can trigger at more than one point. Nothing in the source is objectionable.

**Operational consequences, which are what matter:**

1. **A block can strike any page of this document**, so it cannot be routed around
   by deferring one page. Per-page writes are the only real mitigation — they cap
   the loss at the page in hand.
2. **Always verify the page the agent died on.** After block #3 the status script
   reported p13 as done, and it *was* genuinely complete — but after the earlier
   session-limit kill on this same document, p9 was reported done and was missing
   both prose columns. Same symptom, opposite truth. Render the page and compare;
   do not infer from file size or from the agent's dying words.
3. **Do not work around the filter** — no obfuscation, encoding, or splitting to
   slip text past it. If a page blocks reliably, record it as a documented gap
   with the reason.

### SECOND CORRECTION: it tracks job size, not page identity

Two further blocks (4 total) resolved the pattern, and it is **not** about which
page is being transcribed:

| job shape | pages | outcome |
|---|---|---|
| batch | pp. 9-17 (9 pp) | **blocked** around p13-14 (p13 complete) |
| batch | pp. 14-17 (4 pp) | **blocked** writing p14, no file created |
| isolated | p1 alone | **succeeded**, 0 markers |
| isolated | p14 alone | **succeeded**, 0 markers, no block at any point |

**Isolated single-page jobs: 2 for 2. Multi-page jobs: 0 for 4.** Pages 1 and 14
each killed a batch job and then transcribed cleanly on their own — so the page
content is not the trigger. The correlation is with how much output the agent has
already emitted in its session, which fits a filter false-positive that becomes
more likely as generated volume accumulates.

**Operational rule for this document: transcribe it one page per agent.** It is
slower and it works. Do not conclude from a batch failure that a page is poisoned
— retry it alone before recording any gap.

## Job 32 — Totton1965a pp. 9-33 (2026-08-12)

**Protocol flaw found, and it matters most in the largest document.** Totton uses
`[...]` freely in his own printed text for interpolated editorial queries. Five
instances in this 25-page range alone: `[? Praya maxima Ggbr.]` (p019), a bare
`[?]`, `[? Prayidae]`, `[? diphyids]` (p022), `[? physonula]` (p023). These
collide with this project's `[?reading]` / `[illegible]` marker syntax, so
`grep '\[?'` over Totton will conflate the author's uncertainty with the
transcriber's.

This is the **second** occurrence of the class — Carre1969 p22 has six printed
`[?]` marks from its translator — but Totton is 314 pages, so it will recur
throughout. I judged a migration to a collision-proof marker syntax **not worth
the risk** across 438 already-written pages, since the consequence is only that a
naive grep over-selects pages for the second pass (noisy, not harmful). Instead
the protocol now **requires** a per-page `[NOTE: ...]` listing printed brackets
wherever they occur, which makes the distinction recoverable page by page. Any
tool that measures transcription confidence must read those notes, not just grep.

### Pages wanting a second opinion

- **`page_025.txt`** — the FIG. 2 scale-bar label reads as `0 4 m m` at 300 dpi
  with no decimal point visible, transcribed `[?0·4mm]`. Its gastrozooid labels
  are printed `Gz.5 / Gz.4 / Gz.3` (dot + digit, each with a leader dash) while
  the caption uses `Gz²⁻⁵`; transcribed as printed with a note. A higher-resolution
  scan should settle whether the dots are type or leader artefacts.
- **`page_029.txt`** — FIG. 7 is a five-panel plate with ~45 lettering tokens
  across four widely separated panels. Each was verified at 3.5-4×, but the token
  count makes this the likeliest place for an omission.
- **`page_030.txt`, `page_033.txt`** — two-column pages where a figure and caption
  occupy a narrow left column and prose the right. Transcribed left-then-right per
  protocol with a note; a checker may prefer the opposite order.

### Established facts

- **Printed folio = PDF index − 5**, constant with no drift across pp. 9-33
  (p009 = folio 4 … p033 = folio 28).
- **Every in-text figure label in this range is printed `FIG. n`** — small-cap
  `FIG.` plus an old-style numeral, FIGS. 1-12, with **no** printed oddity to
  preserve. Worth recording, because corpus's own extraction of this document
  renders the same labels as `Fic.`, `F1G.`, `F16.`, `Fi1G.` and consequently
  loses 114 figure numbers. The gold standard now establishes that the *source*
  is clean and the corruption is entirely downstream.
- Printed errors preserved: `Cystons or Anal Vescicles` (p017 heading; spelled
  `Vesicles` on the preceding page), and a stray italic trailing `V.` in
  `Chuniphyes multidentata L. & v. R. V.` (p019 table entry 19), both confirmed at
  4-6× magnification.
- Printer's signature marks `2` (p014) and `3` (p030) sit at the page foot — again
  in folio position but not folios.

## Job 43 — Totton1965a pp. 202-226 (2026-08-14)

**Reference-list boundary pinned.** The systematic section ends on PDF 226 =
printed 221 (last page of Family 15 Abylidae, type filling only the top quarter).
**`REFERENCES` begins on PDF page 227 = printed p222**, opening
`AGASSIZ, A. 1863.` followed by an em-rule repeat-author line `—— 1865.` The
transcriber checked one page beyond its range specifically to establish this, so
the next worker starts with the boundary known rather than inferred. The expected
−5 → −4 offset shift is therefore still ahead, after the references.

**Folio offset held at −5** with no drift across the whole range. Printer's
signature marks at the foot of PDF 206 (`14`) and PDF 222 (`15`) — again in folio
position, again not folios.

### Pages wanting a second opinion

- **`page_222.txt`** — a fig. 149 label read `[?Sc n.]` with **no counterpart in
  the keys of figs. 141/142/147** that its caption refers to. Low resolution and
  nothing to check it against.
- **`page_212.txt`** — a very faint pencil annotation in the fig. 141 caption, read
  `[?left ventral tooth omitted]`. The transcriber's own note is worth quoting: the
  reading "is contextually plausible, which is precisely why it should be checked
  by someone who has not seen my guess."
- **`page_217.txt`** — `[?holotype]` under a heavy ink strike-through; "of the" is
  legible beneath the strike, the noun is not.
- **`page_216.txt`** — the only genuinely awkward layout in the range: two separate
  two-column bands with full-measure text between them. Worth confirming the
  reading order.

### Established facts

- **Totton's printed square brackets do not appear at all in pp. 202-226.** His
  editorial queries here are printed *bare* (`? Thalassophyes Moser, 1925`) or in
  round parentheses (`(? straight)`). Every page in the range carries a `[NOTE]`
  saying so explicitly, which is exactly what the protocol's mandatory-note rule
  is for — the absence is recorded as deliberately as the presence.
- **Hand-lettered scale-bar numerals were the main reading hazard**: the drawn `5`
  closely resembles a lowercase `s`. Flagged on p203 (`5.mm.`), then *resolved* by
  comparison against p217 (5 divisions), p218 and p220 (unambiguous flat top-bar).
  The p203 note documents the resolution rather than leaving an open question.
- **Ink corrections in this physical copy**, recorded as `[HANDWRITTEN]` with the
  printed text transcribed as printed: p205 ("and" struck, "a" written beside),
  p208 (phrase struck), p211 (bare insertion caret), p212, p217.
- Printer's errors transcribed as printed: `anterior nectophorea` (p204),
  `haeckli` for *haeckeli* (p216), `Sears figure 15B` then `Sear's figure 15B` on
  the same page (p220), `eschscholtzi` in a heading vs `eschscholtzii` in the
  synonymy plus `Agalaisma` vs `Aglaismoides` (p223), `Cuboides` on two consecutive
  synonymy lines (p225), an unclosed parenthesis (p215).
- Where figure lettering runs out of alphabetical sequence (figs. 141, 142, 145,
  147, 148, 150) the `[NOTE]` records the visual order the labels are listed in.

## Job 48 — Totton1965a pp. 266-290 (2026-08-24)

The whole range is the back-of-monograph plate section: alternating caption
leaves (roman type, easy) and the plates themselves, Plates XVI-XXVIII. Markers
used: **2 `[?reading]`, 6 `[illegible]`** — verified by grep, not just
self-reported.

- **`page_282.txt`** (Plate XXIV) — *highest priority*. All 6 `[illegible]`
  markers in the job are here. The Kölliker-derived figs. 3, 4, 7, 8, 9 are
  printed on a grey halftone ground with reference letters of roughly 1 px
  stroke; unresolvable at 300 dpi even magnified. A higher-dpi render would
  probably settle them, and is the cheapest possible win in this document.
- **`page_272.txt`** (Plate XIX) — both `[?reading]`s: the superscript digits on
  fig. 4's `S.` and `Go.` labels. The facing caption leaf (p271) implies S¹-S³
  and Go¹, Go², **but the transcriber deliberately did not let the caption
  dictate what it could not see.** That is the correct call under the protocol;
  a second pass should resolve it from the image, not from the caption.
- **`page_268.txt`** (Plate XVII) — no markers used, so *this page will not show
  up in a marker grep*. The ring of letters around fig. 9 was at the legibility
  limit and the letter read as `h` at lower right may be `k`; noted in the file.
- **`page_266.txt`** (Plate XVI) — orientation call rather than a reading
  problem: the plate is printed landscape rotated 90° CCW while its "PLATE XVI"
  heading sits upright portrait. Worth confirming the rotation description reads
  unambiguously.
- **`page_274.txt`** (Plate XX) — fig. 1's letters `a`-`i` recur at many points
  down the drawing. Listed in top-to-bottom order with a caveat rather than
  pairing each letter to a structure, which would have required interpreting the
  anatomy.

### Notes on this range

- **Totton's printed square brackets do not occur anywhere in pp. 266-290** — his
  editorial queries are in the systematic text, not the plate legends. So every
  `[` in these 25 files is a transcriber marker. Unlike Job 43, the absence was
  recorded in the job report rather than in a per-page `[NOTE]` on all 25 pages;
  the protocol's mandatory-note rule is triggered by brackets being *present*, so
  this is compliant, but it is a different convention from pp. 202-226 and is
  recorded here so the second pass reads the absence as **verified**, not
  unchecked.
- Printed anomalies transcribed as-is, each with a `[NOTE:]`: `FIG 4.` missing
  its full point (p275), and `tentilum` with a single medial *l* (p281) where the
  adjacent paragraph sets `tentillum`.
- `page_288.txt` is only 160 bytes and **trips the runbook's truncation check as a
  false positive** — Plate XXVII genuinely carries almost no lettering (figs. 1-3
  unlettered, fig. 4 three letters). Its `[PLATE]` block is complete. Do not
  delete it.

### Format defect found and fixed during this job

All 25 files were written with a bare `[PAGE unnumbered]` first line, dropping the
page index that the rest of the corpus — including pp. 260-265 of this same
document — carries as `[PAGE <n>: unnumbered]`. Repaired in place, index derived
from each filename. This matters because runbook phase 3 concatenates pages into
`<stem>.txt` using the `[PAGE n]` header, and the header is the only
in-file record of which page a file holds. One older stray with the placeholder
copied literally (`Beklemishev1969/page_004.txt`, `[PAGE n: unnumbered]`) was
fixed at the same time. **Worth a glance at the end of each future job.**

## Job 49 — Totton1965a pp. 291-314 (2026-08-24) — DOCUMENT COMPLETE

**Totton1965a is now 314/314.** This range was *not* back matter as expected: it
continues the plate section with Plates XXIX-XL, alternating caption leaf (odd
PDF pages, running head `A SYNOPSIS OF THE SIPHONOPHORA`) and plate leaf (even
pages). **The monograph ends on the Plate XL leaf (PDF 314) with no index, no
errata and no colophon** — worth recording, because an extractor that reports an
index for this document is hallucinating one.

Plate mapping: XXIX 291/292, XXX 293/294, XXXI 295/296, XXXII 297/298,
XXXIII 299/300, XXXIV 301/302, XXXV 303/304, XXXVI 305/306, XXXVII 307/308,
XXXVIII 309/310, XXXIX 311/312, XL 313/314.

Markers used: **9 `[?reading]`, 3 `[illegible]`** — verified by grep.

- **`page_304.txt`** (Plate XXXIV) — *highest priority*. All 3 `[illegible]`s plus
  3 `[?reading]`s. Three leader labels on fig. 2 are unreadable at 300 dpi, and
  the three scale-bar legends are read `[?1 mm]` **by inference from shape
  alone**. A higher-dpi render would settle all six at once. Pair this with
  `page_282.txt` from job 48 — same remedy, same document.
- **`page_298.txt`** (Plate XXXII) — fig. 5's two `R. V.` superscripts cannot be
  distinguished (1 vs 2); one label read `[?No.]` may be `So.`; fig. 8's
  right-hand `R.` appears truncated at the plate edge.
- **`page_314.txt`** (Plate XL) — the two labels at the foot of fig. 5 are read
  `Go.[?♂]` / `Go.[?♀]` **from the facing page-313 legend**; the glyphs alone
  could be digits. Same caution as job 48's page_272: the caption should not be
  allowed to dictate what the image does not show.
- **`page_306.txt`** (Plate XXXVI) — **no markers used**, so it will not appear in
  a marker grep, but every label on the plate is printed extremely faintly and the
  transcriber was near the limit throughout. A run of pale lettering in fig. 7 was
  recorded as illegible *in a note* rather than as a marker.
- **`page_296.txt`** (Plate XXXI) — `To.L.[?¹]` on fig. 12; also near the limit on
  the fig. 3 and fig. 8 lettering.

### Notes on this range

- **No author's printed `[?]` brackets anywhere in 291-314.** The single
  bracket-like printed mark is on page 304, where figs. 1, 4 and 5 each carry a
  drawn scale mark shaped like a square bracket; a `[NOTE:]` there records that it
  is part of the drawing, not a transcriber marker. Combined with job 48, **the
  whole plate section (266-314) is free of the author's editorial brackets** —
  they belong to the systematic text.
- **Sub/superscript disagreement between plate and caption**: plates 300, 302 and
  314 set their figure lettering with **subscripts** where the facing caption
  leaf uses **superscripts**. Noted per page. This is a genuine source
  inconsistency, not a transcription choice.
- Page 297 ends `V¹, R.V² = ventral ridges` — the author's own asymmetry,
  verified by crop and transcribed as printed.
- Page 312 carries a label `Pl.m.` that the page-311 legend never explains.
- Page 308's figure numbers run in the printed reading order **3, 1, 4, 2** — the
  plate is not laid out in numerical sequence.

### Truncation-check false positives — do not delete

`page_292.txt` (187 b), `page_308.txt` (135 b) and `page_310.txt` (145 b) trip the
runbook's `-size -200c` check, as `page_288.txt` did in job 48. All four are
genuine: sparse plates whose only text is the plate number and the engraved figure
numerals, each with a complete `[PLATE]`/`[/PLATE]` block. **The plate section
makes that check noisy by nature** — in this document, expect to adjudicate rather
than act on it.

## Figure/caption markup — corpus-wide audit (2026-08-24)

Captions are **in-line**: inside `[FIGURE]`/`[/FIGURE]` at the point the figure
occupies in the page's reading order. Only *printed* text goes inside — label,
caption, and lettering set as type (axis labels, panel letters, in-plot taxon
labels). Never a description of what the figure depicts. All `[FIGURE]`,
`[PLATE]` and `[TABLE]` blocks in all 757 files are properly closed.

**Two things a consumer of this data needs to know:**

1. **Caption and figure are in the SAME file only when the source prints them on
   the same page.** Totton1965a's plate section (pp. 250-314) uses facing leaves:
   the caption leaf (odd PDF page) carries the plate number, species name and
   `FIG. n.` entries as ordinary printed text with **no `[FIGURE]` block at all**,
   because that leaf holds no figure; the plate leaf (even PDF page) carries the
   `[PLATE]` block with the engraved numerals. Joining caption to image requires
   the leaf pairing — mapping for XXIX-XL is in the Job 49 entry, XVI-XXVIII in
   Job 48.
2. **Three syntactic variants exist** and a parser must handle all three:
   - `[FIGURE]` alone on its line, caption on following lines — 329 blocks, the
     canonical form.
   - `[FIGURE] <caption on the same line>` then `[/FIGURE]` — ~19 blocks, in
     Vanhoeffen1906 (6 files) and Beklemishev1969 (6 files).
   - open + content + close all on one line — 1 block, Bernstein1934 p1.
   Worth normalising at phase-3 concatenation, or documenting in the README.

### One protocol deviation to fix in the second pass

**`Bernstein1934/page_001.txt` line 18** puts a *description of image content*
inside a `[FIGURE]` block — the only such case in the corpus:

```
[FIGURE] [publisher's device: an octagonal vignette showing a polar landscape
with aurora, a hut with a mast, three figures and a dog sledge; no lettering]
[/FIGURE]
```

The device carries no text, so the protocol's prescribed form is
`[FIGURE] [no lettering] [/FIGURE]`, with any observation about the vignette in a
`[NOTE:]` outside the block. It is mild — transparently marked as a description
rather than passed off as printed text, and it correctly records "no lettering" —
but it is the same *category* of content that got the sonnet trial thrown out, so
it should not stay inside a `[FIGURE]` block where a consumer would read it as
transcribed text. **Do not simply delete it**: the observation is useful, it just
belongs in a note.

## Mańko_et al2020 p15 — reference list, 48 entries (2026-08-24)

Solid reference list, two columns, small type. **0 `[?reading]`, 0 `[illegible]`**
— verified by grep, and the 48-entry count independently confirmed. Every line was
read on a magnified crop (9 bands per column plus ~15 targeted zooms). The
column-spanning entry (De Lafontaine & Leggett 1989) is correctly joined onto one
line; the last entry (Kosobokova et al. 2011) is deliberately left incomplete
because it continues onto p16.

### Two entries worth a reviewer's eye

- **Frost et al. 2010** carries `https://doi.org/10.1007/978-90-481-9541-1_8` — a
  book-chapter-shaped DOI on what is printed as a *Hydrobiologia* article.
  Magnified twice and transcribed exactly as printed, so this is a **source
  error, not a transcription error**. Flagged because it is the single string in
  these four pages most likely to be "corrected" by a later reader.
- **`Bogeber, M.`** in Condon et al. 2013 — verified at high magnification. The
  real-world spelling is Bogeberg, so the source is missing a letter. Transcribed
  as printed.

### Source inconsistencies preserved verbatim (not corrected)

This page is unusually rich in them, which makes it **good test material for
citation extraction** — an extractor that silently normalises any of these is
losing information:

- `America1 10, 1000–1005.` — stray digit (Condon et al. 2013).
- `https://doi.org./10.1029/...` — stray period after `org` (Cottier et al. 2005).
- **The same author spelled two ways on one page**: `Beszczynska-Möller` (no ń) in
  two entries vs `Beszczyńska-Möller` in Gluchowska 2017a; `Kwasniewski` (no ś) in
  Hop et al. 2006 vs `Kwaśniewski` in six other entries.
- `Acuna` without tilde (Brodeur 2008) beside `Quiñones` with one (Condon 2013).
- `Gili, J. M., and Boero, F. 2006.` — spaced initials, spelled-out `and`, no comma
  before the year, all against the list's own house style.
- `Zarayskaya Y.,` no comma after surname; `Mayer, L. A.` spaced (Jakobsson).
- `Wiktor, jr.,` lower-case, no initial (Hop 2019).
- `(2010-2014)` hyphen vs `(2001–2014)` en dash.
- `UICN` for IUCN; unaccented French title (Dallot et al. 1988); `world's` with a
  straight apostrophe where Bouillon uses a curly one.
- **DOI present on most entries, absent from nine** (named in the page's closing
  note); three DOIs printed in black rather than as links.

No printed square brackets occur on the page — recorded in a `[NOTE:]` that also
lists the round-parenthesis constructions which might be mistaken for them
(`(in French)`, `(Eds.)`, `(Will)`, `(IBCAO)`).

## Mańko_et al2020 p16 — reference list, 55 entries (2026-08-24)

**0 `[?reading]`, 0 `[illegible]`.** 1 continuation fragment + 55 entries beginning
on the page (26 left column, 29 right). No block.

### The p15→p16 join is verified correct

This is the fragile point in the document and it holds:

- p15 ends mid-entry: `Kosobokova, K., Hopcroft, R.R., Hirche, H.-J., 2011. Patterns
  of zooplankton diversity`
- p16 opens: `through the depths of the Arctic's central basin. Mar. Biodivers. 41,
  29–50. https://doi.org/10.1007/s12526-010-0057-9.`

Checked both ways: the continuation text does not appear in p15, and the head does
not appear in p16 except as a quotation inside the `[NOTE:]` documenting the join.
Nothing duplicated, nothing dropped. `Pagès et al. 2001` spans the column break and
is correctly joined to one line.

### Two entries worth a reviewer's eye

- **`Polyakov, I.B.`** — unambiguous at magnification, but the initial is `I.V.`
  everywhere else in the literature. A reviewer may want to confirm the glyph was
  read rather than the name recalled. This is the right kind of flag: the
  transcriber distrusted its own familiarity with the name.
- **`Pugh, P.R., 1974 … United Kingdom 45, 25–90`** — legible and transcribed as
  printed, but volume/page pairs are the one field where a printed error and a
  transcription error are indistinguishable from the page alone.

### Source inconsistencies preserved verbatim (each confirmed on a dedicated crop)

`Kramp, 1961.` with no initials; `Hirche, H.J.` here vs `H.-J.` on p15 — **the same
author punctuated two ways across adjacent pages**; `Nöthing`; `Laakman`;
`Cyddipid`; a title ending `is it only Mertensia ovum.` with a full stop where a
question mark is expected; `wamr` for warm; `2007,` with a comma before the vegan
title; `https://doi.org/ 10.11646/` with an **interior space**, set in black;
`Polyakov, I.B.`; `Texas AandM Press` (the ampersand lost in conversion);
`Texas pp. 395–402` with no comma; `journal. pone.0120204` with an interior space;
`Russell, F.S., 1938` beside `Russel, F.S., 1953` — one l, same author;
`R Core Team.` with its year buried as `2016 URL`.

**17 of the 55 complete entries carry no DOI**, counted mechanically rather than by
eye (Schuchert excluded as incomplete). With p15's nine, that is 26 DOI-less
entries across the two pages — useful ground truth for any extractor that assumes a
DOI is always present.

No printed square brackets on the page; recorded in a `[NOTE:]` as required.

## Mańko_et al2020 p17 — final page of the corpus (2026-08-24) — PASS 1 COMPLETE

**0 `[?reading]`, 0 `[illegible]`.** 1 continuation line + **27 complete entries**
(Schuchert 2019 → Zelickman 1972). No block.

### The p16→p17 join is verified correct

- p16 ends mid-entry: `Schuchert, P., 2007. The European athecate hydroids and their
  medusae (Hydrozoa,`
- p17 opens: `Cnidaria): Filifera part 2. Rev. Suisse Zool. 114, 195–396.
  https://doi.org/10.5962/bhl.part.80395.`

Checked both ways, nothing duplicated or dropped. `Vecchione et al. 2015` spans the
column break and is joined to one line.

**Last reference entry in the document**: `Zelickman, E.A., 1972` — Barents Sea
pelagic hydromedusae, siphonophores and ctenophores, Mar. Biol. 17, 256–264,
`10.1007/BF00366301`.

**What follows the list: nothing.** No supplementary-data note, copyright/licence
line, colophon, advertisement or uncited-references block — the list simply stops
and the lower half of the page is blank white paper, **recorded explicitly as blank
rather than left untranscribed**. An extractor reporting trailing matter for this
document is inventing it.

### One point for a reviewer

**`D'Agostino, L.`** (Wickham et al. 2019) — at 12× the apostrophe reads as a
straight tapered vertical mark rather than a comma-shaped curly quote, so U+0027
was transcribed and the decision flagged. A reviewer may read it as U+2019. This is
the right way to handle a character-level judgement call: make it, record it, and
say what the alternative reading would be.

### Inconsistencies catalogued (verified on dedicated crops, none corrected)

**Again the same author spelled two ways on one page**: `Beszczynska-Moller`
(Svendsen 2002) vs `Beszczyńska-Möller` (Trudnowska 2016) — the third such pair in
three pages. Missing diacritics in `Gluchowska`, `Agusti`, `Sornes`; Stine 1995
volume printed as a bare `4`; Tsuruta 1963 pages `13–214`; **DOI suffix
capitalisation varying** (`bf00403086`, `2007gl029974` vs `BF00366301`); erratic
journal abbreviation including `Helgoländer Meeresun.`; Wickström 2019 carrying a
DOI but no volume or pages; `et al.,` embedded mid-author-list (Wickham 2019);
`fiord` vs `fjord`; an em dash in `J. Geophys. Res.—Oceans`; hyphenated initials
`Winther, J.-G.`. Separate notes cover the hyperlinked entries, DOI line-wraps (no
interior spaces on this page), and every end-of-line hyphen resolution.

---

# PASS 1 COMPLETE — 761/761 pages, 2026-08-24

Verified at completion, not merely reported:

- `status.py`: **761/761, 100.0%**; 761 page files on disk, matching expected.
- **Headers**: all 761 carry a well-formed `[PAGE …]` first line; no bare
  `[PAGE unnumbered]`, no literal `[PAGE n:` placeholder.
- **Block balance**: every `[FIGURE]`, `[PLATE]` and `[TABLE]` block in all 761
  files is closed.
- **Truncation check**: 4 files under 200 bytes, all adjudicated as genuine sparse
  plates in Totton's plate section (pp. 288, 292, 308, 310), each with a complete
  `[PLATE]` block. **Do not delete them.**
- **Uncertainty markers corpus-wide: 422 `[?reading]`, 314 `[illegible]`** across
  761 pages. (An earlier figure of 439 was wrong: the `grep -r --include='page_*.txt'`
  that produced it also matched `_provenance/poppler/page_*.txt`, whose OCR text
  contains 17 stray `[?` sequences. Anything counting markers must exclude
  `_provenance`.) — the targeting input for the second pass, together with the
  per-document flags above.

The four remaining phases are listed under "Remaining phases after pass 1" in
RUNBOOK.md and have **not** been started: poppler cross-check → `confidence.json`,
targeted second pass, concatenation + `manifest.json`, and `readme.md`.

## RESOLVED — Totton1965a plate lettering (p282, p304): uncertainty is correct, CLOSED

Checked against the physical volume by C. Dunn, 2026-08-24. The unresolved
lettering on these plates is **very small type that the original volume itself
reproduced poorly** — the limit is in the 1965 printing, not in the 300 dpi render
and not in the transcription.

**There will be no rescanning, and these pages are not second-pass candidates.**
The `[?reading]` and `[illegible]` markers on p282 (6) and p304 (3 + three
inferred scale legends) are the correct and final reading. Leave them.

This closes the item flagged as "the cheapest concrete win in the corpus" in the
Job 48 and Job 49 entries above — it was not a win, because better input does not
exist. **Do not re-open it.** More generally: a marker dense page in a plate
section is often recording a defect in the source, and re-transcribing it will
reproduce the same markers at the same cost.

---

# Inspected against the physical volumes — C. Dunn, 2026-08-25

Three of the seven pages put forward for human inspection are now settled. All
three were resolved by looking at the printed page, which no amount of
re-transcription would have achieved.

## RESOLVED — `Bernstein1934/page_010.txt` (printed p.42): eight columns, no ninth

**The table has eight data columns after the species stub, the last headed
`100—25`.** The heavy gutter shadow may conceal a further column — the table is a
two-page spread continuing on p. 43, and station 13 would take a third horizon by
analogy with stations 14 and 15 — but **nothing of any such column is visible.**

Pass 1 had recorded a ninth column of `[illegible]` cells. That was wrong in a
specific and instructive way: **it asserted the existence of a column nobody can
see.** `[illegible]` means "there is something here I cannot read", and using it
for "there might be something here" manufactures data. The column has been
removed from all 26 table rows; the station row now reads 15/15/15, 14/14/14,
13/13, and the note records the possibility without encoding it.

Illegible markers on the page: **28 → 2** (only station 13's cut-off longitude
remains, which is a genuine partial reading of a visible value).

**Generalises to:** do not use `[illegible]` for suspected content. If the page
edge is lost, say so in a `[NOTE:]` and transcribe what is there.

## RESOLVED — `Carre1969_Nanomia_tr/page_012.txt` (printed p.334): the scale bar is `60 µ`

Confirmed correct. This one matters less for the answer than for what it exposed:
pass 1 transcribed it **without an uncertainty marker**, at a self-assessed ~85%
confidence, having judged the blobby leading digit could be a `4`. So a figure
numeral that the transcriber privately doubted appeared in **no** marker count and
in none of the audits — it was surfaced only because the agent mentioned it in
prose in its job report.

A `[NOTE:]` now records the reading and its verification.

**Generalises to:** the marker counts understate uncertainty. `[?reading]` is
applied to glyphs that cannot be resolved, but not reliably to *readings the
transcriber is merely unsure of*. Figure numerals, scale bars and volume/page
numbers deserve more suspicion than the counts imply.

## RESOLVED — `Carre1969_Nanomia_tr/page_002.txt` (printed p.326): `Agalmidae`

The broken `m` in "autres Agalmidae" (last line of the Metchnikoff quotation),
which images as `ın`, is **damage to this impression, not orthography**. The
correct transcription is `Agalmidae`.

This became **protocol rule 5**, since it decides a whole class: broken sorts,
dropped serifs and ink-fill are common across the 16th–19th century material here.
The rule does not loosen rule 2 — a word set wrongly stays wrong. The line is
between what the type says and what the ink did.

## CLOSED — the whole Totton plate-lettering class (2026-08-25)

`Totton1965a/page_262.txt` (Plate XIV, 63 flagged letters, the densest page in the
corpus) was confirmed as already settled by the p282/p304 decision. **The entire
plate-lettering class is now closed** and is not a second-pass candidate:

`Totton1965a` pp. **250, 258, 262, 282, 298, 304, 314**, and the
`Quoy_Gaimard1834Plates` plate leaves.

The reasoning, once, for all of them: these are minute engraved reference letters
that the original printing reproduced poorly. The limit is in the source, not in
the 300 dpi render and not in the transcription, and no rescanning is planned.
**A marker-dense plate page is usually recording a defect in the source**;
re-transcribing it reproduces the same markers at the same cost.

## Still open — needs a reader of Japanese, not a second transcription

4. `Kawamura1911a/page_011.txt` — 32 flagged figure labels. Reviewed 2026-08-25;
   **not resolvable by the current reviewer, who does not read Japanese.** This is
   a language-competence gap, not a legibility problem, so re-transcribing by the
   same means will not close it.

   Two observations that narrow it for whoever does look, neither of which is
   applied to the transcription (rule 3 — no guessing from plausibility):

   - **The labels look iroha-ordered.** Four of them read `ロ ハ ニ ホ`, which are
     iroha positions 2, 3, 4 and 5. If the sequence is iroha, then the one label
     recorded as a bare `[?]` — sitting before `ロ` — is almost certainly `イ`,
     position 1. That is an inference from ordering, not a reading, so it is
     recorded here rather than in the page.
   - **Katakana is the expected script for figure keys**, which is itself evidence
     against the kanji members of the confusable pairs (口, 力, 工, 二). It raises
     confidence in the existing readings without confirming them.

   What would settle it: someone who reads Japanese looking at the rendered page
   once. It is 32 single characters and would take minutes.

## Still open — other
6. `Quoy_Gaimard1834Plates/page_016.txt` — is plate 5 really present three times
   in that PDF? Affects what correct extraction means for the document.
7. `Mańko_et al2020/page_016.txt` — `Polyakov, I.B.` as printed vs `I.V.` in the
   literature: source typo, or misread glyph?
