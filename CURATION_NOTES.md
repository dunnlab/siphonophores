# Library curation notes

## 2026-09-17: corrections from the literature audit

The changes in this pass were checked against local source PDFs, using rendered pages for author lists, titles, dates, page ranges and license statements. Existing citation keys and original PDFs were retained. Unchanged bibliography entries remain byte-identical. This is a source-data correction; the served Corpus bundle needs a rebuild before it reflects these changes.

### Bibliography evidence

Physical PDF pages are one-based; printed pagination is identified separately.

| Records | Correction and source |
|---|---|
| `Sutherlandetal2019b` | PDF p. 1 lists Sutherland, Brad J. Gemmell, Sean P. Colin and Costello separately; restore four authors and Gemmell's initials. |
| `Panasiuketal2014` | PDF p. 1 lists Panasiuk-Chodnicka, Żmijewska and Mańko separately; restore three authors. |
| `Churchetal2014` | PDF pp. 1–2 separate the title from Dunn's author name. The bioRxiv cover supplies the preprint DOI and CC BY-NC-ND 4.0 license. Preserve this preprint's identity, distinct from the 2015 journal article. |
| `MunroEtal2018` | PDF p. 1 lists Stefan Siebert and Felipe Zapata as separate authors; restore ten authors. |
| `AlvarinoLeira1986` | PDF pp. 1–2 identify María José Leira Ambrós; preserve the compound surname. The journal is named in full on PDF p. 37 / printed p. 105, with volume 3(1) on the first page. |
| `Toyokawaetal1998` | PDF p. 1 / printed p. 61 spells the first author's surname Toyokawa; retain all four authors. |
| `Yu2006b` | PDF pp. 1–2 identify a master's thesis, not a Ph.D.; pp. 1 and 8 supply the English title and university/institute. Use `mastersthesis` and `school`. Do not carry the unverified “118 pp” into a page-count field. |
| `Siebertetal2013` | PDF p. 1 states printed pages 201–232 and CC BY 3.0. |
| `Mapstone2014`, `Mapstone2015Correction` | Combined PDF pp. 1–2 are the 2015 correction; p. 3 begins the 2014 original. Restore original volume 9(2), e87737 and DOI ending 0087737; the correction has volume 10(2), e0118381 and DOI ending 0118381. Embedded license links on pp. 1 and 3 point to CC BY 4.0. See asset handling below. |
| `CarreD1974a`–`c` | Each PDF p. 1 supplies its full title, subtitle, journal and issue. The word is **nématoblastes**, not the preliminary audit's proposed “nématocytes.” Part I has pages 205–218, issue 2; parts II/III have issue 3. Part III's title orders the processes “formation, maturation et migration.” |
| `LeDanois1913a` | PDF p. 2 identifies the summer 1912 cruise; the existing title described the other PDF. Physical pp. 2–14 and 15–22 contain two installments, printed 13–25 and 27–34. The unnumbered starts 13 and 27 are inferred from the following printed 14 and 28. |
| `LeDanois1913b` | PDF pp. 2 and 9 identify the summer 1913 cruise and its continuation. Restore volume 38 and the two ranges 282–288, 304–315. |
| `Moser1925` | PDF pp. 1–2 identify volume XVII, Zoologie IX, and the full title; PDF p. 541 is printed p. 541. Remove the concatenated Moss citation and restore Moser's own locators. `Moss1878` already exists and was not duplicated or changed. |
| `DuTertre1654` | PDF p. 1 identifies Jean Baptiste Du Tertre, the book title and the two Langlois publishers in Paris. “R. P.” is a religious title, not the author's initials. Use `book`; move the scan description to `note`. PDF pp. 2–4 contain printed 281–283. Do not assert the unverified full-book page count from the old malformed field. |
| `Zhang2005b` | PDF p. 9 / printed p. 526 gives Zhang Fang, Yang Bo and Zhang Guang-Tao, plus the full English title. PDF p. 1 gives volume 36(6). Correct the page-map language description to Chinese with an English abstract. |
| `Mooreetal1953` | PDF p. 1 supplies the four authors including T. Dow, the full Part III title, journal name and issue 2. Correct the author delimiter and move subtitle text out of the journal field. |
| `TottonFraser1955a`–`f` | Each first page prints “Conseil International pour l’Exploration de la Mer.” Correct only that spelling, preserving distinct sheets. |
| `HaddockCase1999`, `JustOlesen2014`, `Robson1973` | Correct `.PDF` references to the actual `.pdf` basenames. These were the only blocking inventory failures before this pass. No PDF renames were needed. |

### Mapstone asset handling

`library/M/Mapstone2014.pdf` remains byte-for-byte unchanged, SHA-256 `45b0092b04a74568f3c72e8c4476a77b59a602a2d3c3c6bdb5a51fbc04cfe0dc3` (see the verification note below if using a different checkout). Its original-article record now selects physical pages `3--39` via `keeppages`. The `pagemap` explains the excluded correction, and both records link the other's DOI in their notes.

`library/M/Mapstone2015Correction.pdf` is a new two-page copy of physical pages 1–2 from the combined PDF. Extracted text and rendered page pixels were compared page-by-page and matched the source. This gives the correction its own PDF identity without replacing or destructively splitting the original asset.

### Lexicon and guidance

- `somatocyst`: Mapstone (2009), printed p. 73 / PDF p. 80 defines the structure as a blind-ending gastrovascular diverticulum extending into the mesogloea of some calycophoran nectophores; printed p. 10 provides additional context.
- `gonophore`: Mapstone (2009), printed p. 70 / PDF p. 77 describes a sexual medusoid producing either eggs or sperm. The former use of “dioecious” at zooid level was removed.
- `instructions.md` now explains source context, keys, counts, corrections and figure credits without introducing new taxonomic equivalences. Audit-specific limitations are tied to the old build and Corpus issue links.
- `readme.md` now describes comma-separated `doclang` tags in dominant-language order, matching the existing parser. No language fields were bulk-rewritten.

### Remaining review

- Historical single-/double-i spelling pairs and Pagès/Hissmann authority strings need nomenclatural/source-authority review. No taxonomy archive or synonym table was changed.
- Broader lexicon review remains a separate domain-review task; this pass corrects the two identified definitions.
- Source PDF text-layer defects in Boysen-Ennen and Mapstone still need extraction recovery or a reviewed replacement/derivative. Original assets were preserved.
- Figure-specific metadata for the Hosia third-party artwork depends on the Corpus override contract (#302); no unsupported metadata field was invented.
- The bound Moss volume says 1879, but that alone does not establish the issue-level article date. Retain `Moss1878` pending dating evidence.
- Filename style and initial punctuation were not normalized merely for consistency.

### Verification

The bibliography validator passed after corrections. Targeted checks verified author counts, record types, distinct Mapstone identities, bibliography parse/render preservation and YAML parsing. All 1,784 untouched original bibliography entries were compared byte-for-byte. The generated library statistics were refreshed using the repository validator's `--emit-readme` workflow.

The full PDF scan passed for all 1,774 files: no unreadable PDFs, hash errors or duplicate-content groups. Nonblocking warnings remain (38 records without local PDFs, one shared-DOI group and incomplete source/identifier metadata). The validator currently counts only `journal` for its source-field warning, so properly structured book/thesis records using `publisher`/`school` are included in that warning.
