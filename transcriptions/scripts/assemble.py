#!/usr/bin/env python3
"""Phase 3: concatenate per-page files into one text per document, and write a
MANIFEST with page counts and checksums.

The per-page files stay the authoritative copy -- they are what the transcription
protocol writes and what `gold_status.py` counts. These concatenations are a
convenience for reading and for feeding whole documents to a consumer, and
manifest.json lets anyone prove a concatenation still matches its pages.

A note on the page delimiter. Each page file already opens with a `[PAGE ...]`
line, but that line records the **printed folio**, which is frequently not the
PDF page index -- DeHaan1827's PDF page 9 opens `[PAGE 496]`, and much of the
corpus is unfoliated (`[PAGE 12: unnumbered]`). Concatenating on the in-file
header alone would therefore lose the ability to address a page by its position
in the file, which is how every other tool here refers to pages. So each page is
preceded by an explicit machine-readable delimiter carrying the PDF index, and
the original header is left untouched directly beneath it.

Usage:
    assemble.py [--check]     # --check verifies without rewriting
"""
import argparse
import hashlib
import json
import pathlib
import re

# Resolve the data root from this file's location so the tree works in any
# checkout. Layout: <data>/scripts/<this file> and <data>/<stem>/page_NNN.txt
DATA = pathlib.Path(__file__).resolve().parent.parent


def document_dirs():
    """Every transcribed document directory, in name order.

    Filters on containing page files rather than excluding `scripts/` by name,
    so a future sibling directory cannot silently be treated as a document.
    """
    return sorted((p for p in DATA.iterdir()
                   if p.is_dir() and any(p.glob("page_*.txt"))),
                  key=lambda p: p.name)

ROOT = DATA
FULL = DATA
DELIM = "<<<PAGE pdf={n} file={f}>>>"


def sha(b):
    return hashlib.sha256(b).hexdigest()


def build(doc):
    pages = sorted(doc.glob("page_*.txt"), key=lambda p: int(p.stem.split("_")[1]))
    parts, entries = [], []
    for p in pages:
        n = int(p.stem.split("_")[1])
        raw = p.read_bytes()
        text = raw.decode("utf-8")
        header = text.split("\n", 1)[0]
        entries.append({
            "pdf_page": n,
            "file": p.name,
            "printed_header": header,
            "bytes": len(raw),
            "sha256": sha(raw),
        })
        parts.append(DELIM.format(n=n, f=p.name) + "\n" + text.rstrip("\n") + "\n")
    return "\n".join(parts), entries, pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="verify existing concatenations match their pages")
    a = ap.parse_args()

    manifest, problems = {}, []
    for doc in document_dirs():
        blob, entries, pages = build(doc)
        out = FULL / f"{doc.name}.txt"
        data = blob.encode("utf-8")

        if a.check:
            if not out.exists():
                problems.append(f"missing concatenation: {out.name}")
            elif sha(out.read_bytes()) != sha(data):
                problems.append(f"STALE: {out.name} does not match its page files")
        else:
            out.write_bytes(data)

        # Marker tallies, computed from the pages rather than restated from a
        # report, so the manifest cannot drift from the data it describes.
        joined = blob
        manifest[doc.name] = {
            "pages": len(entries),
            "concatenation": out.name,
            "concatenation_sha256": sha(data),
            "concatenation_bytes": len(data),
            "uncertain_markers": len(re.findall(r"\[\?", joined)),
            "illegible_markers": len(re.findall(r"\[illegible", joined)),
            "figure_blocks": len(re.findall(r"^\[FIGURE\]", joined, re.M)),
            "plate_blocks": len(re.findall(r"^\[PLATE\]", joined, re.M)),
            "table_blocks": len(re.findall(r"^\[TABLE\]", joined, re.M)),
            "page_files": entries,
        }

    total = sum(v["pages"] for v in manifest.values())
    doc = {
        "generated": "2026-08-24",
        "corpus": "siphonophores_smoke",
        "documents": len(manifest),
        "pages": total,
        "authoritative_copy": "the per-page files under <stem>/; the "
                              "<stem>.txt concatenations are derived and can be "
                              "regenerated with scripts/gold_assemble.py",
        "page_delimiter": DELIM,
        "delimiter_note": "the [PAGE ...] line inside each page records the "
                          "PRINTED FOLIO, which often differs from the PDF page "
                          "index (DeHaan1827 PDF page 9 opens '[PAGE 496]'); the "
                          "delimiter carries the PDF index",
        "detail": manifest,
    }

    if a.check:
        if problems:
            print("\n".join(problems))
            raise SystemExit(1)
        print(f"OK — all {len(manifest)} concatenations match their page files "
              f"({total} pages)")
    else:
        (ROOT / "manifest.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False))
        print(f"wrote {len(manifest)} concatenations + manifest.json ({total} pages)")


if __name__ == "__main__":
    main()
