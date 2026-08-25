#!/usr/bin/env python3
"""Render PDF pages to PNG for gold-standard transcription.

Deliberately uses poppler's `pdftoppm`, NOT PyMuPDF, so that nothing in the
gold-standard pipeline shares a library with `corpus` (which uses PyMuPDF for
page geometry, docling for layout+text and ocrmypdf/tesseract for OCR). Renders
are scratch: they are regenerable from the PDFs and are not part of the durable
artifact.

Usage:
    render.py <stem> <outdir> [pages]

`pages` is a comma-separated list of 1-based numbers or ranges ("1,3,5-8");
omit it to render every page. Existing PNGs are left alone, so re-running is
cheap.
"""
import json
import pathlib
import subprocess
import sys

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

DPI = 300


def parse_pages(spec, n):
    if not spec:
        return list(range(1, n + 1))
    out = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return [p for p in out if 1 <= p <= n]


def source_pdf(stem):
    """Resolve a stem to its source PDF via sources.json.

    The sources are NOT all in one place -- most sit in `library/<LETTER>/`, but
    some live in `nonlibrary/others/`, `nonlibrary/translations/` or
    `library/orphans/`. Guessing a shelf letter from the stem gets this wrong,
    and wrong in a way that is easy to miss: `Lery1594.pdf` exists BOTH at
    `library/L/` and at `nonlibrary/others/`, they are DIFFERENT scans, and the
    transcription was made from the latter. So sources.json is authoritative and
    records the sha256 of the exact file that was transcribed.
    """
    src = json.loads((DATA / "sources.json").read_text())
    if stem not in src:
        raise SystemExit(f"{stem} is not in sources.json; known stems:\n  " +
                         "\n  ".join(sorted(src)))
    pdf = DATA.parent / src[stem]["pdf"]
    if not pdf.exists():
        raise SystemExit(
            f"source PDF missing: {pdf}\n"
            "If this is a fresh clone, the PDFs are in Git LFS -- run `git lfs pull`.")
    return pdf


def page_count(pdf):
    info = subprocess.run(["pdfinfo", str(pdf)], capture_output=True).stdout.decode(
        "utf8", "replace"
    )
    for line in info.splitlines():
        if line.startswith("Pages:"):
            return int(line.split()[1])
    raise SystemExit(f"could not read page count for {pdf}")


def main():
    stem = sys.argv[1]
    outdir = pathlib.Path(sys.argv[2])
    spec = sys.argv[3] if len(sys.argv) > 3 else None
    pdf = source_pdf(stem)
    outdir.mkdir(parents=True, exist_ok=True)
    for pno in parse_pages(spec, page_count(pdf)):
        dest = outdir / f"{stem}_p{pno:03d}.png"
        if dest.exists() and dest.stat().st_size > 0:
            print(dest)
            continue
        # -singlefile keeps pdftoppm from appending its own page suffix
        subprocess.run(
            ["pdftoppm", "-r", str(DPI), "-f", str(pno), "-l", str(pno),
             "-png", "-singlefile", str(pdf), str(dest.with_suffix(""))],
            check=True,
        )
        print(dest)


if __name__ == "__main__":
    main()
