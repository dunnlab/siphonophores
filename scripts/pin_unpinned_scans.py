#!/usr/bin/env python3
"""Give the remaining scanned documents a ``doclang``.

34 documents in the library are scans — a majority of their pages are raster
images, so corpus will OCR them — and carried no ``doclang``, therefore no
``ocrlang``. They are the documents where a pin is worth most: OCR language
selection falls back to detecting the language from whatever text layer an
earlier OCR pass left, which on exactly these documents is the least
trustworthy. Most were skipped by the annotation pass for the same reason.

``ocrlang`` is left to ``rederive_ocrlang.py``; this script only records the
fact. Every assignment below comes from the document's own ``pagemap`` — the
annotator's reading — and the two that could have been Fraktur were checked by
rendering a page:

* ``Leuckart1854b`` — *Archiv für Naturgeschichte* 1854, and roman type, not
  Fraktur. ``de``, not ``de-Latf``.
* ``Vanhoeffen1897`` — a modern retyping, German paragraph followed by its
  English translation on the same page. ``de, en``.

The Margulis papers split on whether ``keeppages`` keeps the English matter.
Most are "Russian original; separable appended English translation" with the
translation dropped, so they are ``ru`` alone; two keep an English summary
inside the retained range and are ``ru, en``. (``ru`` derives ``rus+eng``
either way — the distinction is a record of what is on the page, not a
different pack list.)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from bibio import parse_bib, render_bib  # noqa: E402

BIB = REPO / "siphonophores.bib"

# file -> doclang, each read off that document's pagemap.
ASSIGNMENTS = {
    # "complete English document" / English translation throughout
    "Bouillonetal2004_Siphonophores.pdf": "en",
    "Copeland1968.pdf": "en",
    "Kawamura1910a.pdf": "en",
    "Kawamura1915b.pdf": "en",
    "Lenhoff_Schneiderman1959.pdf": "en",
    "Lewis_Fish1969.pdf": "en",
    "Madin1997.pdf": "en",
    "Mapstone2003_Apolemia.pdf": "en",
    "Margulis1991_Erenna_bedoti.pdf": "en",   # image-only English translation
    "Molina1808.pdf": "en",                   # English translation of the Chili history
    "Moore1955.pdf": "en",
    "Vetter_Dayton1999.pdf": "en",
    # Russian original, appended English translation dropped by keeppages
    "Margulis1972a_Atl_Caly_Physo.pdf": "ru",
    "Margulis1976b_Physos_Indian.pdf": "ru",
    "Margulis1977a_Erenna1.pdf": "ru",
    "Margulis1977b_Moseria.pdf": "ru",
    "Margulis1978_Western_NAtlantic.pdf": "ru",
    "Margulis1982b_Physos_Antarctic.pdf": "ru",
    "Margulis1987_South Pacific.pdf": "ru",
    "Margulis1988_Clausophyidaetr.pdf": "ru",
    "Margulis1989_Tropical.pdf": "ru",
    "Margulis_Alekseev1986_Lbeklem.pdf": "ru",
    "Margulis_Vereshchaka1994b.pdf": "ru",
    # Russian original whose retained pages include an English summary
    "Margulis1970_Lzenk.pdf": "ru, en",
    "Margulis1971_Atl_Lensia.pdf": "ru, en",
    # Everything else
    "delle Chiaje1822Plates.pdf": "it",       # Italian atlas title page + plates
    "delle Chiaje1841PlatesVolumes6-7.pdf": "it",
    "Fewkes1884b.pdf": "en, fr, de",          # multilingual bibliography
    "Gamulin_Krsinic2000.pdf": "hr, en",      # bilingual Croatian-English monograph
    "Leuckart1854b.pdf": "de",                # roman type, verified by rendering
    "Marion1890.pdf": "fr",
    "Pages1991.pdf": "es, en",                # bilingual Spanish-English thesis
    "Sars1877.pdf": "da, en",                 # parallel Danish and English columns
    "Vanhoeffen1897.pdf": "de, en",           # verified by rendering
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bib", type=Path, default=BIB)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    doc = parse_bib(a.bib.read_text(encoding="utf-8"))
    seen, set_count, already = set(), 0, []
    for entry in doc.entries:
        fname = (entry.get("file") or "").strip()
        if fname not in ASSIGNMENTS:
            continue
        seen.add(fname)
        if entry.get("doclang"):
            already.append(fname)
            continue
        entry.set_field("doclang", ASSIGNMENTS[fname])
        print(f"  {fname:38s} doclang = {ASSIGNMENTS[fname]}")
        set_count += 1

    missing = set(ASSIGNMENTS) - seen
    if missing:
        print(f"\n!! {len(missing)} assignment(s) matched no entry: {sorted(missing)}")
    if already:
        print(f"\n{len(already)} already had a doclang and were left alone")
    print(f"\n{set_count} doclang value(s) set. Run rederive_ocrlang.py next.")
    if a.dry_run:
        print("dry run, nothing written")
        return 0
    a.bib.write_text(render_bib(doc), encoding="utf-8")
    print(f"wrote {a.bib}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
