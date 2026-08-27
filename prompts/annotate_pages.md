# Annotating page ranges and languages for the siphonophore library

You are making a first-pass assessment of the scanned PDFs in this library, so
that downstream processing knows what each file actually contains before it tries
to read it. Two questions carry the work:

- **What is the paper?** A file often holds more than the work its bib entry
  describes — a library or vendor wrapper, a bound volume's title page, an
  appended translation, blank padding.
- **What language is it in?** Which decides how the pages are read, and for
  scanned material that decision is most of the extraction quality.

They are one job rather than two, because the errors compound. Forty pages of
front matter skew the automatic language detection, and the language it lands on
decides how the whole document is OCR'd — so a wrapper left in place can corrupt
the text of the paper behind it. Answering both from the same look at the same
pages is the point of doing this by hand at all.

A third output records what you saw, so the first two can be reviewed by someone
who has not opened the file.

For each document you decide three things and write them into a JSON record:

| field | what it is | what it causes |
|---|---|---|
| `keeppages` | which physical PDF pages are the paper | read by the pipeline — every page you leave out is discarded |
| `doclang` | what language the paper is in, as a BCP-47 tag | not read directly, but `ocrlang` is derived from it, and that pins OCR |
| `pagemap` | free-text description of the document's structure | nothing — documentation only |

Read the whole of this document before starting. The first two fields cause
irreversible things to happen, and the failure modes are silent.

### How `doclang` reaches OCR

You never write `ocrlang`, and you should not try to. `doclang` is a fact about
the paper in a standard vocabulary; `ocrlang` is a list of Tesseract pack names,
which is a different vocabulary and an instruction to a specific tool. When
`scripts/apply_page_annotations.py` writes your annotations into the bib, it
resolves one to the other and writes both:

```
doclang = {de-Latf}   ->   ocrlang = {deu_latf+deu}
doclang = {ja}        ->   ocrlang = {jpn+eng}
doclang = {ru}        ->   ocrlang = {rus+eng}
```

Two consequences worth holding onto:

- **`doclang` is inert on its own.** Nothing in the pipeline reads it. It is the
  derived `ocrlang` that has teeth. So the care you take is about what your tag
  resolves to, not about the tag sitting in the file.
- **A `doclang` you leave blank produces no `ocrlang` at all**, and the pipeline
  detects the language itself, exactly as it does today. That is the safe
  default, and it is why omitting is cheap.

## The one rule that matters most

**When you are not sure, emit nothing and flag the document.**

A wrong `keeppages` silently deletes real content. Every later stage sees only the
pages you selected; nothing downstream can tell that a page was withheld, so the
loss is invisible and permanent until someone re-reads the original PDF.

A wrong `doclang` resolves to a pinned Tesseract pack that overrides *all three*
automatic language signals at once — langdetect, Tesseract's script detection, and
the OCR sampling probe. Pinning the wrong language produces confidently garbled
text, which is worse than no annotation at all. The pin is what the pipeline acts
on, so a wrong tag is not merely a wrong label.

Omitting a field costs nothing: the pipeline behaves exactly as it does today.
**Expect to leave roughly 15% of documents unannotated. That is a success, not a
gap.** Do not reach for a plausible answer to avoid a blank.

## Input

### Where things are

Every path in this document is **relative to the root of the `siphonophores`
repository**, and every command assumes that is your working directory. Nothing
here uses an absolute path, because the repository is not always checked out in
the same place.

```
siphonophores/
  siphonophores.bib     the bibliography; you do not edit it directly
  library/              the PDFs, in surname shelves — library/A/ … library/H_J/ … library/U_Z/
  build/
    page_evidence.json    your input
    page_annotations.json your output
    contact_sheets/       rendered pages, written by --sheets
  scripts/              inspect_pages.py, apply_page_annotations.py
  prompts/              this file
```

You never open `siphonophores.bib` and never edit it. The bib facts you need
(`bib_key`, `title`, `year`, `bhl_title`, any existing annotations) are already
joined into each evidence record's `bib` object, and
`scripts/apply_page_annotations.py` is what writes your output into the bib
afterwards.

### The evidence file

`build/page_evidence.json`, produced by `scripts/inspect_pages.py`. One record per
PDF.

**The evidence is a starting point, not the whole world.** It exists to do two
things: tell you which documents need attention at all, and surface the one
distinction that text extraction cannot make (see `kind`, below). It is not a
complete description of any document, and its schema was fixed before anyone
looked at the document in front of you.

So: **when a question matters and the evidence does not answer it, open the PDF.**
The PDF is on local disk at the record's `path` (repo-relative, e.g.
`library/H_J/Ilyin1900.pdf`). Read more text, render a page and look at it, check
a page the evidence summarised away. Regenerating one
document's record takes a fraction of a second:

```bash
# the record, as JSON
python scripts/inspect_pages.py --pdf Leloup1934btr.pdf

# rendered pages, 20 per grayscale JPEG in build/contact_sheets/ — the only
# evidence there is for the 118 documents with no text layer
python scripts/inspect_pages.py --pdf Leloup1934btr.pdf --sheets
```

Guessing from an incomplete field is never the right move — the fields you write
are binding, and the cost of looking is seconds.

The fields you will use:

```
file            basename, e.g. "Ilyin1900.pdf"
path            repo-relative path, e.g. "library/H_J/Ilyin1900.pdf"
pages           total page count
bib             bib priors: bib_key, title, year, bhl_title, existing annotations
text_layer      "full" | "none"
wrappers        vendor wrapper markers found (see below)
imprints        publisher branding found (see below)
cover_shaped    page 1 looks like a cover — low precision, a pointer only
langs           distinct per-page language guesses seen
boundaries      pages where the script or language changed, with a text head
review_reasons  why text alone cannot settle this document (see Triage below)
needs_images    shorthand for "review_reasons is non-empty"
page[]          per page: n, chars, img_cov, max_img, kind, scripts, lang, markers, head
```

### `kind` is the field to read first

Each page is classified `text`, `image`, `blank`, or `sparse`.

**`image` means a substantial raster page, not proof that the page is empty or
proof that it belongs to the paper.** This is the single most dangerous confusion
available to you. A page with zero characters in its text layer may be the entire
article rendered as a bitmap, a scanned blank leaf, a binding, or a cover —
identical in extracted text, very different in meaning. `kind` tells you that a
raster must be looked at; it cannot decide what the raster depicts.

`Bennett1860.pdf` is the first case to hold in mind: 23 pages, of which pages 3–23
all report zero characters. Every one of them is the paper. `keeppages =
{3--23}`, and a naive "drop the empty pages" reading would have deleted the entire
document. `Leuckart1851c.pdf` is the counterexample: its pages 53–79 are image
pages because they are scans, but rendering shows blank leaves; page 80 is the
physical back cover. The evidence tells you to look, not what to decide.

- `blank` — no text, no significant image. Padding. Safe to drop.
- `image` — no text, but a substantial bitmap. Usually content, but sometimes a
  scanned blank leaf, binding, or cover. **Render it before dropping it.**
- `sparse` — a shelfmark, a page number, a stamp.
- `text` — a real text layer. Note this includes OCR'd scans, so `text` with
  `img_cov` near 1.0 means "a scan someone has already OCR'd".

## What counts as front matter to drop

**Vendor wrappers** — `wrappers` in the evidence. These add pages that are not the
paper:

- **BHL** — a banner page 1 (`biodiversitylibrary.org`, "Page(s):", "Contributed
  by:", "Generated <date>") usually followed by "This page intentionally left
  blank" on page 2. The paper starts at page 3.
- **JSTOR** — a single cover page 1 carrying `links.jstor.org` and "Your use of the
  JSTOR archive". Paper starts page 2.
- **Google Books** — two or three pages, often the German "Über dieses Buch"
  boilerplate, sometimes followed by a shelfmark page.
- **ResearchGate** — "See discussions, stats, and author profiles for this
  publication at: https://www.researchgate.net/...".
- **ProQuest** — a per-page banner rather than a cover.

**Journal and volume title pages** — the most common front matter in this library
and the one with *no* marker string at all. A bound volume was scanned starting from
its own title page, so the article is preceded by one or more pages reading like
`PROCEEDINGS ... BOSTON SOCIETY OF NATURAL HISTORY. VOL. IX. 1863.` The
`cover_shaped` flag points at ~330 candidates, but it also fires on genuine article
title pages, so it is a prompt to look, never a verdict.

**Blank runs** — leading or trailing `blank` pages. `leading_blank` and
`trailing_blank` count only true blanks; `leading_nontext` / `trailing_nontext`
include `image` pages and must **not** be used by themselves to decide what to
drop. Render image-only runs.

**Plates and figures are part of the paper.** Do not drop a page because it is an
image with no text — see above.

### What is NOT front matter

`imprints` in the evidence — Springer footers, ScienceDirect headers, Wiley links,
JSTOR "This content downloaded" running lines, "Downloaded by ...". These are
printed on the article's *own* pages. **Never drop a page for carrying one.** 183
documents here carry an imprint string; dropping page 1 on that basis would remove
the first page of each paper.

## Translations

Many documents pair a foreign-language original with an English translation. The
rule for this library:

> **Keep the original. Drop the translation.**

The reasoning: the corpus's retrieval model is cross-lingual, so an English query
already finds Russian and German passages, and the original is the citable
artifact. The translation adds little and would duplicate the paper's content.

### Filename prior

Files whose stem ends in a translation suffix — matching
`(?:[_-])?p?(?:tr|trans)$`, case-insensitively — are the 76 known cases. Treat this
as a prior, **never as ground truth**:

- It has false positives on stems that merely end in "tr" for other reasons.
- It misses real translations. About 53 documents show a foreign-language head and
  an English tail without any suffix; most are bilingual abstracts, but a few are
  genuine unlabelled translations — `Soto2010.pdf`, `Vogt1854.pdf`,
  `Legare1961.pdf` are known examples.

So: check the `boundaries` and `langs` evidence on **every** document, not only the
suffixed ones.

### Three shapes, and the rule differs for each

**Appended** — the common case, roughly 65 of the 76. Original runs from the front,
English translation occupies the tail, typically about a third of the pages.
`keeppages` is the head. `Carre1969_Nanomia_tr.pdf`: French pages 1–20, a German
`Zusammenfassung` on 21, English translation from 22. Answer: `{1--21}`.

**Interleaved** — the translation is broken into chunks placed between sections of
the original. `Leloup1934btr.pdf`: French 1–13, English 14–16, French 17–39,
English 40–41, French 42–59, English 60–64, then French again on 65–86. This needs
a non-contiguous selection, which the syntax supports:
`{1--13,17--39,42--59,65--86}`. Verify every range against the evidence rather
than assuming two halves.

**Translation-only** — no original is present. Known: `Leloup1954tr.pdf`,
`Leloup1941btr.pdf`, `Stepanjants1963_Ndiomedeae_trans.pdf`. Keep the translation,
set `doclang = {en}`, and say so in `pagemap`. Do not leave these empty.

### Telling where the translation starts

`boundaries` in the evidence gives you candidate pages with a text head. Two
corroborating signals:

- The translation section usually **restarts page numbering at 1**, and the number
  is often in the text layer.
- It usually opens with a citation header — `Schneider (1898)`, or
  `Cahiers de Biologie Marine 10, 325-341; 1969`.

The per-page `lang` guesses are a shallow stopword score. They are noisy on
OCR-garbled text — a mangled French page can score as Spanish or Latin — so use
them to locate a boundary and then confirm it from the `head` text or a rendered
page. Never copy a per-page `lang` into `doclang`.

## `doclang`

A BCP-47 tag describing the language of the pages you kept.

```
ru          Russian
de          German
de-Latf     German set in Fraktur          -> selects the deu_latf OCR pack
fr          French
la          Latin
grc         Ancient Greek
zh-Hant     Traditional Chinese
ja          Japanese
```

**`de-Latf` is worth special attention.** Fraktur is a blackletter typeface, and
without the matching OCR pack 19th-century German scans come out as whitespace.
Automatic detection can only ever report `de` — it reads the text layer, which
cannot describe a typeface. Recognising Fraktur from a rendered page is a judgment
only this pass can make, and it is high value. If the page images show blackletter,
write `de-Latf`.

Rules:

- One tag. If the kept pages are genuinely bilingual throughout, leave `doclang`
  empty and flag — a single pin cannot describe two languages.
- Do not set `doclang` for born-digital documents that will never be OCR'd (a clean
  text layer, `img_cov` at or near 0 on every page). It would have no effect and
  would force a needless reprocess. Record the language in `pagemap` instead.
- Do not guess from the title or the author's nationality. Use the page evidence.

## Out of scope — record, do not fix

**Mid-page contamination.** Some scans bleed a neighbouring article into a page of
the paper. `Huxley1852b.pdf` page 3 opens with French text about steam boilers from
the adjacent article; `Chun1881_tr.pdf` page 2 opens with the end of the preceding
item and page 6 runs into an unrelated herpetology paper. `keeppages` selects whole
pages and cannot fix this. **Do not drop a page that is half real content.** Note
it in `pagemap` and set `needs_review`.

**Splitting a bound volume into several papers.** One PDF is one document; two bib
entries pointing at the same file collide and cannot be processed separately. If a
scan holds several distinct papers, select the one the bib entry describes and note
the rest in `pagemap`.

## Output

Write **one file per document** to `build/annotations/<stem>.json`, where
`<stem>` is the PDF basename without its extension — so `Ilyin1900.pdf` becomes
`build/annotations/Ilyin1900.json`. The file holds a single object:

```json
{
  "file": "Ilyin1900.pdf",
  "keeppages": "4--6",
  "doclang": "de",
  "pagemap": "1 BHL banner; 2 blank notice; 3 bound-volume title page; 4--6 article (image-only, no text layer); 6 also begins an unrelated paper",
  "confidence": "high",
  "needs_review": true,
  "notes": "Body pages carry no text layer; page 6 is kept because it contains the end of the target article."
}
```

- `keeppages` — physical 1-based page positions. `--` for ranges, commas between
  them: `3--20`, `2,4,8--20`, `40--` for "to the end". **Never printed page
  numbers.** Omit the field entirely if the whole document is the paper — a
  selection covering every page is noise, and it makes the pipeline reprocess for
  no reason.
- `doclang` — one BCP-47 tag, or omit.
- `pagemap` — always write one, even when you annotate nothing else. It is what
  makes the other two reviewable, and it costs nothing to be wrong in.
  **No braces** — the value goes into a BibTeX field.
- `confidence` — `high` / `medium` / `low`. Anything below `high` on `keeppages`
  or `doclang` means you should probably have omitted that field.
- `needs_review` — true whenever a human should look. Contamination, an ambiguous
  boundary, a bound volume, a language you could not settle.

**One file per document, never a shared one.** Several annotators may be working
at once, and appending to a single JSON file would interleave and lose records.
Separate files collide with nothing and need no coordination. It also makes the
work resumable: a document is done when its file exists, so

```bash
# documents still to do
comm -23 \
  <(python -c "import json;print('\n'.join(sorted(d['file'][:-4] for d in json.load(open('build/page_evidence.json'))['documents'])))") \
  <(ls build/annotations 2>/dev/null | sed 's/\.json$//' | sort)
```

lists what is left, and re-running an interrupted pass costs nothing.

`scripts/apply_page_annotations.py` reads the whole directory and merges it, so
nothing else needs to change.

## Triage: what `review_reasons` does and does not catch

Each record carries `review_reasons`, a list of why text alone cannot settle the
document. Across the library:

| reason | documents | what it means |
|---|---|---|
| `multilingual` | 412 | more than one language scored — the translation signal, and noisy |
| `cover_shaped` | 329 | page 1 looks like a cover; also fires on real title pages |
| `trailing_nontext` | 221 | non-text run at the end — check `kind` before dropping anything |
| `leading_nontext` | 175 | non-text run at the front |
| `mid_doc_nontext_run` | 152 | a gap away from the ends — inserted plates, a bound-in second paper |
| `long_document` | 127 | over 100 pages; may be a volume rather than a paper |
| `no_text_layer` | 118 | nothing to read; rendered pages are the only evidence |
| `wrapper` | 42 | a vendor wrapper string was found |
| `lang_inconclusive` | 18 | text present but unscoreable — garbled OCR, or an unlisted language |

**An empty `review_reasons` is not a clean bill of health.** The reasons are
computed from page measurements alone, and three things this library cares about
are invisible to them. Check these yourself on every document, flagged or not:

- **Translation-suffixed filenames.** The `_tr` convention is a property of this
  library's naming, not of the pages, so no measurement can see it. Four
  suffixed files carry no review reason at all.
- **Fraktur.** A German document set in blackletter has a normal text layer and
  scores as plain `de`. Nothing short of looking at a rendered page can tell you,
  and this is where `de-Latf` pays off most. 22 unflagged documents score as
  German — treat every one as a Fraktur candidate until you have looked.
- **Age.** 42 unflagged documents are pre-1900. Old scans carry bound-in title
  pages, plates and library furniture that the measurements happen to miss.
  Publication year comes from the bib, not the pages, so it can never be a
  reason.

## Working order

1. Read the record's `review_reasons`. If it is empty *and* none of the extra
   checks below apply, the document is almost certainly the whole paper: write a
   `pagemap` saying so, omit `keeppages`, move on. That is about 890 of 1775, and
   time not spent on them is time spent on the 885 that need it.
2. If `wrappers` is non-empty, or `cover_shaped` is set, look at pages 1–3: their
   `head` text, and a rendered page if the text layer is absent.
3. Check `langs` and `boundaries` for a translation, whatever the filename says.
4. Check the head and tail for blank runs. A `blank` classification is strong
   evidence; an `image` classification requires rendering because a scanned blank
   leaf or binding is still an image.
5. Open the PDF whenever step 2, 3 or 4 leaves a question the evidence cannot
   settle. This is expected, not a failure of the evidence — it is why the PDFs are
   on local disk.
6. Decide. When two readings are defensible, take the one that keeps more pages.

Keeping a page of front matter is a small, correctable cost. Dropping a page of the
paper is a silent, permanent loss. The asymmetry should decide every close call.

## Model evaluation record — 27 August 2026

We evaluated OpenAI GPT-5.6 Sol, Terra and Luna for this annotation task before
starting the library-wide pass. All runs used medium reasoning, the same prompt,
local PDF inspection tools, isolated output directories, and blinded agents that
could not read the reference annotations or one another's output.

This is a repository-specific operational evaluation, not a general model
benchmark. OpenAI's model documentation on the evaluation date described Sol as
the frontier model for complex professional work, Terra as the intelligence/cost
balance, and Luna as the cost-sensitive high-volume model. See the
[OpenAI model catalog](https://developers.openai.com/api/docs/models).

### Round 1: ten hand-characterised documents

The test set was `tests/fixtures/page_annotations.groundtruth.json`. It includes
vendor wrappers, image-only scans, bound-volume furniture, contamination,
appended and interleaved translations, a translation-only file, and a clean
born-digital article.

The evaluation itself exposed stale reference decisions for `Ilyin1900.pdf`,
`Leuckart1851c.pdf`, `Chun1881_tr.pdf`, and `Leloup1934btr.pdf`. Those PDFs were
visually re-audited, the fixture and examples above were corrected, and all model
outputs were rescored against the corrected reference. That correction is part of
the result: a benchmark must not be treated as ground truth merely because it is
named ground truth.

An exact binding decision means that both `keeppages` and `doclang` match in
presence and value for a document. `pagemap` wording was reviewed semantically but
was not included in the exact score.

| model | exact binding decisions | observed failure mode |
|---|---:|---|
| GPT-5.6 Sol | 10/10 | none in this small test |
| GPT-5.6 Terra | 8/10 | dropped a contaminated target page; missed an interleaved translation block |
| GPT-5.6 Luna | 7/10 | dropped end matter, selected the wrong German OCR typeface, retained a vendor wrapper, and retained a bound-volume title page |

The Luna and Terra errors matter more than their raw counts suggest. A wrong
`keeppages` or `doclang` is exactly the silent, binding failure this pass exists to
prevent, and several incorrect records were nevertheless labelled high confidence.

### Round 2: twelve fresh stratified documents

A second set sampled new image-only files, wrappers, translation suffixes, old
German/possible-Fraktur material, multilingual boundaries, and clean controls.
This round had no prewritten ground truth, so it is an operational check rather
than a numerical accuracy benchmark.

- Sol completed all 12 records and correctly exercised the revised raster rule,
  including retaining plates while excluding scanned blank versos, and
  distinguishing roman German type from Fraktur.
- Terra produced no output shards in the 12-document run. A retry reduced to six
  documents also produced no shards before the run was stopped. Record this as a
  failure of that evaluation run, not a universal claim that Terra cannot perform
  the task.

### Production policy resulting from the evaluation

- Use **GPT-5.6 Sol with medium reasoning** for production annotations.
- Run several Sol agents concurrently on disjoint, small, resumable batches. Each
  agent writes one JSON file per document, as required above.
- Do not use Luna to write `keeppages` or `doclang`.
- Do not accept a model's `high` confidence as review evidence by itself.
- Validate JSON syntax and page ranges mechanically, then manually review all
  `needs_review` records and a stratified sample of high-confidence binding
  decisions before applying annotations to the bibliography.
- Re-run a representative evaluation when the prompt, PDFs, evidence generator,
  or available model family changes.
