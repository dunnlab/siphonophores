"""Identify BHL-origin PDFs in the library.

Two signal sources:
  1. bib metadata: DOI starting with `10.5962/bhl.` or URL on biodiversitylibrary.org
  2. PDF embedded metadata: Creator = "Biodiversity Heritage Library" or
     "Digitized by the Internet Archive"; Producer mentioning "LuraDocument".

Cross-reference and bucket the results so we can see:
  - BHL-confirmed: bib AND PDF metadata both flag it
  - BHL by metadata only: PDF looks like BHL but bib has no BHL link yet
  - BHL by bib only: bib says BHL but PDF metadata doesn't show typical BHL signature
"""
from __future__ import annotations
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import bibtexparser

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / 'library'
BIB = REPO / 'siphonophores.bib'

def pdf_signals(path: Path) -> tuple[str, dict]:
    """Run pdfinfo and return BHL signal flags."""
    try:
        r = subprocess.run(['pdfinfo', str(path)], capture_output=True,
                           text=True, timeout=15)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return (str(path), {'error': 'pdfinfo failed'})
    if r.returncode != 0:
        return (str(path), {'error': f'pdfinfo exit {r.returncode}'})
    creator = ''
    producer = ''
    for line in r.stdout.splitlines():
        if line.startswith('Creator:'):
            creator = line.split(':', 1)[1].strip()
        elif line.startswith('Producer:'):
            producer = line.split(':', 1)[1].strip()
    return (str(path), {
        'creator': creator,
        'producer': producer,
        'bhl_creator': 'biodiversity heritage library' in creator.lower(),
        'ia_creator':  'internet archive' in creator.lower(),
        'lura_prod':   'luradocument' in producer.lower(),
    })

# ---- bib pass ----
parser = bibtexparser.bparser.BibTexParser(common_strings=True)
with BIB.open() as f:
    entries = bibtexparser.load(f, parser=parser).entries

bib_bhl_files: dict[str, str] = {}  # filename -> citation key
for e in entries:
    fn = (e.get('file') or '').strip()
    if not fn: continue
    doi = (e.get('doi') or '').strip().lower()
    url = (e.get('url') or '').strip().lower()
    if doi.startswith('10.5962/bhl.') or 'biodiversitylibrary.org' in url:
        bib_bhl_files[fn] = e['ID']

print(f"bib entries flagged BHL: {len(bib_bhl_files)}")

# ---- PDF pass ----
pdfs = [p for p in LIBRARY.rglob('*.pdf')
        if not (p.relative_to(LIBRARY).parts and p.relative_to(LIBRARY).parts[0] == 'orphans')]
print(f"scanning {len(pdfs)} PDFs...")

results: dict[str, dict] = {}
with ThreadPoolExecutor(max_workers=8) as ex:
    futs = {ex.submit(pdf_signals, p): p for p in pdfs}
    done = 0
    for fut in as_completed(futs):
        path, sig = fut.result()
        results[path] = sig
        done += 1
        if done % 250 == 0:
            print(f"  {done}/{len(pdfs)}")

# ---- bucket ----
bhl_by_meta: dict[str, dict] = {}     # path -> sig
for path, sig in results.items():
    if sig.get('bhl_creator') or sig.get('ia_creator') or sig.get('lura_prod'):
        bhl_by_meta[path] = sig

bhl_meta_basenames = {Path(p).name for p in bhl_by_meta}

confirmed = []         # in both bib and meta
meta_only = []         # in meta but not bib (candidates for adding BHL metadata)
bib_only = []          # in bib but not meta

for fn, key in bib_bhl_files.items():
    if fn in bhl_meta_basenames:
        confirmed.append((key, fn))
    else:
        bib_only.append((key, fn))

# Build reverse map for meta_only
for path, sig in bhl_by_meta.items():
    name = Path(path).name
    if name not in bib_bhl_files:
        meta_only.append((path, sig))

print(f"\nResults:")
print(f"  BHL-confirmed (bib + metadata):       {len(confirmed)}")
print(f"  BHL by metadata only (candidates):    {len(meta_only)}")
print(f"  BHL by bib only (older scans):        {len(bib_only)}")

# Output
out_path = REPO / 'logs' / 'bhl_origin.txt'
out_path.parent.mkdir(exist_ok=True)
with out_path.open('w') as f:
    f.write(f"BHL-origin PDF inventory ({len(confirmed) + len(meta_only) + len(bib_only)} flagged)\n")
    f.write(f"  bib + metadata agreement:  {len(confirmed)}\n")
    f.write(f"  metadata only (candidates):{len(meta_only)}\n")
    f.write(f"  bib only:                  {len(bib_only)}\n\n")

    f.write("=" * 76 + "\n")
    f.write(f"BHL-CONFIRMED ({len(confirmed)})\n")
    f.write("=" * 76 + "\n")
    for key, fn in sorted(confirmed):
        f.write(f"  {key:30s}  {fn}\n")

    f.write("\n" + "=" * 76 + "\n")
    f.write(f"METADATA ONLY — likely BHL but bib has no BHL DOI/URL ({len(meta_only)})\n")
    f.write("=" * 76 + "\n")
    for path, sig in sorted(meta_only):
        rel = Path(path).relative_to(LIBRARY)
        signals = []
        if sig.get('bhl_creator'): signals.append('BHL-creator')
        if sig.get('ia_creator'): signals.append('IA-creator')
        if sig.get('lura_prod'): signals.append('LuraDocument')
        f.write(f"  {rel}\n")
        f.write(f"     signals: {', '.join(signals)}\n")
        if sig.get('creator'):  f.write(f"     creator: {sig['creator']}\n")
        if sig.get('producer'): f.write(f"     producer: {sig['producer']}\n")

    f.write("\n" + "=" * 76 + "\n")
    f.write(f"BIB ONLY — bib says BHL but PDF metadata doesn't show signature ({len(bib_only)})\n")
    f.write("=" * 76 + "\n")
    for key, fn in sorted(bib_only):
        sig = next((s for p, s in results.items() if Path(p).name == fn), {})
        f.write(f"  {key:30s}  {fn}\n")
        if sig.get('creator'):  f.write(f"     creator: {sig['creator']}\n")
        if sig.get('producer'): f.write(f"     producer: {sig['producer']}\n")

print(f"\nWrote {out_path}")
