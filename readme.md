# Siphonophores

This is a collection of siphonophore manuscripts. The vast majority were painstakingly curated by Phil Pugh. He made high quality scans of many of the older papers, and curated metadata.

His original reference list is preserved here as `AASCANNED LITERATURE.docx`. A
machine-readable bibliography derived from it lives in
[`siphonophores.bib`](siphonophores.bib); see [`CONTRIBUTING.md`](CONTRIBUTING.md)
for how it's built and how to regenerate it.

PDFs sit under `library/`, sharded by surname-letter shelves (`library/A`,
`library/B`, …). Two subdirectories are special:

- `library/orphans/` — PDFs we want to keep but for which we have no known
  bibliographic information. They're intentionally not referenced from
  `siphonophores.bib` and are skipped by the curation scripts.
- `library/Z*` — gitignored material (excluded from both git and the
  curation scripts).

`Pugh non siphonophore papers/` (at the repo root, not under `library/`)
holds non-siphonophore PDFs Pugh kept alongside the main collection (e.g.
methods/oceanographic-context papers cited from his work). These are not
part of `siphonophores.bib` and are excluded from reconciliation.

`translations/` (also at the repo root) holds translations of papers in
the main library — companion files to bib entries rather than primary
records of their own. Excluded from reconciliation.

`others/` (at the repo root) holds alternate scans, plate-only excerpts,
and other miscellaneous PDFs that supplement entries in the main library
without being the primary record. Excluded from reconciliation.
