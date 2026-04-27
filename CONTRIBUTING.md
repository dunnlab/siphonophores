# Contributing — siphonophores.bib

This file documents the build pipeline that turns
`AASCANNED LITERATURE.docx` (Phil Pugh's curated reference list) plus the
PDFs in `library/` into `siphonophores.bib`.

The intent is that anyone can re-run the pipeline end-to-end and reproduce
`siphonophores.bib`. Re-runs are idempotent: every script writes a JSON
artifact under `build/` that the next stage consumes.

## Source of truth

- **`AASCANNED LITERATURE.docx`** is a **pristine artifact** — Pugh's
  curated reference list as he authored it. Do not edit it. Treat it as
  read-only input to the pipeline.
- **`siphonophores.bib`** is the live, editable bibliography. All
  reconciliations, corrections, additions, and removals happen here.
- **`library/` PDFs** may be renamed when a filename is genuinely
  malformed (typo'd year, transliteration mistake), since `siphonophores.bib`
  references PDFs by basename via the `file` field.

A consequence: the orphan logs are diagnostics about the docx-vs-library
gap as it stood when Pugh handed off the collection. They will not shrink
over time as we update `siphonophores.bib` — the bib is what evolves.

---

## Quick start

```bash
# one-time
conda env create -f environment.yaml
conda activate siphonophores

# rebuild the bib from scratch (~20 minutes, network-bound)
python scripts/parse_docx.py
python scripts/match_library.py
python scripts/extract_dois.py
python scripts/crossref_lookup.py
python scripts/verify_urls.py
python scripts/build_bib.py
```

`parse_docx.py` and `match_library.py` are fast (seconds). The middle three
hit the network and take 5–15 minutes each — pass `--resume` on a re-run to
skip work that's already in the cache:

```bash
python scripts/extract_dois.py   --resume
python scripts/crossref_lookup.py --resume
python scripts/verify_urls.py    --resume
```

`build_bib.py` is a pure function over the cached JSON — re-run it freely
to regenerate `siphonophores.bib` after editing `build_bib.py` itself.

---

## What each script does

### `scripts/parse_docx.py`

Reads `AASCANNED LITERATURE.docx` and produces `build/entries.json`: one
record per reference with parsed authors, year, year-suffix (`a`/`b`/…),
title, and journal/source breakdown. Lines that look like Pugh's section
headers (`AAA – 85`) and meta-notes are skipped.

The parser is best-effort. Unparsed fragments are kept in the `raw` field
so downstream stages can still emit something usable. Warnings from this
stage land in `logs/parse_docx.log`.

### `scripts/match_library.py`

Matches each docx entry to a PDF in `library/`. Uses two strategies:

1. **Exact key match** — folds the PDF stem (drops case / diacritics /
   non-alphanumerics) and the entry's surname-year-suffix to a normalized
   form; an equality match wins.
2. **Fuzzy fallback** — when no exact match exists, uses
   `rapidfuzz.ratio` against unused PDFs in the same year (or ±1 year for
   year-typo fallbacks). Conservative thresholds are tuned so that we
   don't spuriously match across authors.

Outputs `build/match.json`. Two orphan logs are written:

- `logs/orphans_docx.log` — entries with no matched PDF
- `logs/orphans_pdf.log` — PDFs with no matched entry

Ambiguous matches and fuzzy decisions are also logged for review.

### `scripts/extract_dois.py`

Walks every matched PDF and tries to find a printed DOI on pages 1–3 plus
the last page (the back-matter sometimes carries the DOI on a single
line). Skips papers from before 1997, since DOIs were not regularly
included in printed papers prior to that.

Each candidate is verified against the Crossref `/works/{doi}` endpoint
before being accepted. Truncated-looking DOIs (trailing `-`, `_`, etc.)
are rejected because PDF-to-text often splits long DOIs across linebreaks.

Output: `build/dois.json` keyed on PDF basename. Network failures are
logged but don't abort the run.

### `scripts/crossref_lookup.py`

For every entry — including those that already have a PDF-extracted DOI —
queries the Crossref bibliographic search for a candidate. We accept a
candidate iff:

- title-similarity (`token_set_ratio`) ≥ 80
- the entry's first-author surname appears in the candidate's author list
- year matches within ±1

`build_bib.py` does a second-pass title check (`token_sort_ratio` ≥ 80)
to weed out near-collisions like "Acalephs of the Fiji Islands" matching
"The islands and coral reefs of the Fiji Group" which both share three
salient tokens.

When a Crossref hit and a PDF-extracted DOI disagree, a `DOI mismatch`
warning is logged. `build_bib.py` decides which one to emit.

Output: `build/crossref_dois.json` keyed on entry index.

### `scripts/verify_urls.py`

Confirms that every candidate URL we plan to emit actually resolves.

For DOI URLs we use the DOI Foundation `handles` API (`responseCode == 1`)
rather than HEAD-requesting the publisher — many publishers (Wiley,
Springer, …) reject HEAD from non-browser user-agents and return 403,
which would create false negatives.

For non-DOI URLs (BHL `bibliography/`, journal homepages) we do a HEAD,
falling back to GET when the server doesn't allow HEAD.

Output: `build/url_status.json` mapping each URL to `{status, ok}`.

### `scripts/reconcile_orphans.py`

Optional, advisory pass over the unmatched docx entries × unmatched PDFs
cross-product. For each pair it scores year + surname + title agreement
(the title comes from the Crossref-verified DOI when we have one, else
from the PDF's first page) and groups suggestions into three buckets:

| Bucket | Heuristic |
| --- | --- |
| `probable` | year matches and surname agrees, OR the title agrees strongly |
| `maybe` | year off by 1–2 OR surname is fuzzy but another signal supports it |
| `uncertain` | weak signal — eyeball before acting |

Output: `logs/orphan_reconciliation.log`. The script writes nothing else —
applying a suggestion means **editing `siphonophores.bib`** (adding the
missing record, correcting a year, pointing at the right `file`) or
**renaming the PDF** when its filename is genuinely malformed. The docx
itself is never touched.

Typical wins it surfaces:

- year typos in filenames (`Conrad_etal1984.pdf` for a 1982 paper)
- author transliterations (`Pakhomov_Froneman1999.pdf` for `Pahhomov` in docx)
- filename has no year (`KingPhysalia.pdf`, `Selso_Master.pdf`) but the
  PDF's first page title exactly matches the docx entry
- filename has a different year than the docx (`Williams1964.pdf` vs
  docx `Williams 1986` — same paper)

### `scripts/build_bib.py`

Pure function over the JSON artifacts. For each entry:

- **Citation key**: prefer the matched PDF basename (Pugh's hand-picked
  key); fall back to a constructed `Surname[Surname2|Etal]YEAR[suffix]`.
- **DOI selection**: PDF and Crossref agree → use it. They disagree →
  pick the one whose reported title is closer to the entry title;
  `crossref-mismatch`/`pdf-mismatch` is logged for manual review.
- **BHL fields**: `bhl_part` and `bhl_title` are derived from any DOI
  with the `10.5962/bhl.{part,title}.<n>` shape (these are minted by
  BHL and surfaced through Crossref).
- **URL fallback**: `doi → bhl part → bhl title → crossref URL`. We drop
  any URL that `verify_urls.py` previously marked bad.
- **`file`** field: the PDF basename — directory layout under `library/`
  may shift, so we don't pin a path.

Output: `siphonophores.bib` in the repo root.

---

## Logs

`logs/` is gitignored. Each script writes one log there. The most useful
post-run logs for review:

| Log | What's in it |
| --- | --- |
| `logs/orphans_docx.log` | Docx entries with no matched PDF in `library/`. |
| `logs/orphans_pdf.log` | PDFs in `library/` not referenced by any docx entry. |
| `logs/orphan_reconciliation.log` | Suggested pairings between the two orphan sets, bucketed `probable` / `maybe` / `uncertain`. |
| `logs/no_doi_post_1997.log` | Post-1997 entries that ended up with no DOI. |
| `logs/pdf_text_no_doi.log` | Library PDFs (post-1997) where `extract_dois.py` couldn't pull a DOI from the text — most of these did get a Crossref DOI later, so cross-check against `siphonophores.bib`. |
| `logs/crossref_lookup.log` | `DOI mismatch` warnings worth reviewing by hand. |
| `logs/match_library.log` | Ambiguous and fuzzy-match decisions. |
| `logs/build_bib.log` | DOI-mismatch resolution choices. |

---

## Adding a new PDF

The docx is read-only, so additions land directly in `siphonophores.bib`.

1. Drop the PDF in the appropriate `library/<letter>/` folder using
   Pugh's naming convention (`Surname[_Coauthor]YEAR[a-z]?.pdf`).
2. Add a `@article{...}` entry to `siphonophores.bib` by hand, with at
   minimum `author`, `year`, `title`, `journal`, and `file`. Add `doi`
   and `url` if you have them.
3. (Optional) re-run `verify_urls.py` if you added a URL/DOI you'd like
   sanity-checked.

You don't need to re-run the docx-driven stages of the pipeline for a
manual addition — the bib is the source of truth from this point forward.

---

## When the pipeline gets a DOI wrong

Expected mismatches happen. The two common modes:

1. **Crossref title-collision** — short, generic titles match across
   different papers. The fix is to add the entry's `journal` to the
   acceptance test in `crossref_lookup.py`, or tighten the
   title-similarity floor in `build_bib.py`'s second-pass check.

2. **PDF DOI is the *referenced* paper, not the article** — `extract_dois.py`
   sometimes picks up a DOI from the references section. The fix is
   already partially applied: we prefer DOIs whose Crossref title matches
   the entry title.

For one-off corrections, edit `siphonophores.bib` directly. (The cached
JSON under `build/` is *intermediate* state — it gets overwritten on the
next pipeline run, so don't park hand-edits there.)

---

## Environment

`environment.yaml` pins the conda environment used by the scripts. The
runtime dependencies are intentionally light: `python-docx`, `pypdf`,
`requests`, `rapidfuzz`. No tex toolchain is required to *build* the bib
file — only to *use* it.
