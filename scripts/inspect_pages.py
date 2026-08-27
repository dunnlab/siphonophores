#!/usr/bin/env python3
"""Per-page structural evidence for every PDF in the library.

Feeds the page-annotation pass (``prompts/annotate_pages.md``), which decides
``keeppages`` / ``doclang`` / ``pagemap`` per document. This script does not
decide anything — it only measures, so that the judgment calls happen in one
place and can be reviewed.

Deliberately carries **no siphonophore-specific knowledge**: no BHL
conventions, no ``_tr`` filename rules, no decisions about translations. Those
live in the prompt. This is a prototype of a generic ``corpus bib
inspect-pages`` subcommand (corpus#217) and should stay portable enough that
upstreaming it is a move rather than a rewrite.

What it measures, per page:

    chars     characters in the text layer
    img_cov   fraction of the page area covered by raster images
    max_img   largest single image as a fraction of page area
    scripts   Unicode-script histogram of the text layer
    markers   vendor-boilerplate strings present
    kind      text | image | blank | sparse   (see classify_page)

The ``kind`` column is the point of the whole exercise. **A text-emptiness test
cannot tell a genuinely blank page from an un-OCR'd image page**, and the two
call for opposite treatment: a blank page should be dropped from ``keeppages``,
an un-OCR'd image page is very likely the actual paper. In this library that
distinction covers 117 documents and 4,139 pages, and they are the oldest and
most valuable material — Linnaeus, delle Chiaje, Bory, the plate atlases.
Asking the raster layer, not the text layer, is what separates them.

Document-level fields join in the bib priors an annotator needs (``bhl_title``,
existing ``keeppages``/``doclang``) and flag which documents cannot be settled
from text alone and so need rendered pages.

Usage::

    python scripts/inspect_pages.py                     # whole library
    python scripts/inspect_pages.py --pdf Ilyin1900.pdf # one file, printed
    python scripts/inspect_pages.py --limit 50          # first 50, for a smoke test
    python scripts/inspect_pages.py --sheets            # also render contact sheets

Needs PyMuPDF. ``--sheets`` additionally needs Pillow.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Iterable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bibio import parse_bib  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BIB = REPO / "siphonophores.bib"
BUILD = REPO / "build"
LOGS = REPO / "logs"
OUT_JSON = BUILD / "page_evidence.json"
SHEET_DIR = BUILD / "contact_sheets"

# A page whose text layer holds fewer characters than this carries no usable
# prose — a page number, a shelfmark, or nothing at all.
NEAR_EMPTY_CHARS = 20
# A page is "a scan" when one image covers at least this much of it. Mirrors
# corpus's _scanned_page_fraction, which separated scans from born-digital
# pages cleanly on its 32-paper smoke corpus with this threshold.
SCAN_IMG_COVERAGE = 0.50
# Below this mean chars/page a document has no text layer worth reading and
# must be inspected visually.
TEXT_LAYER_CHARS_PER_PAGE = 300
# Longest edge of a rendered contact sheet, in pixels.
MAX_SHEET_PX = 1500

# Vendor strings, split into two groups that must never be conflated.
#
# WRAPPERS add pages that are not the paper — a cover sheet, a rights notice, a
# scan banner. Those pages are candidates to drop.
#
# IMPRINTS are publisher branding printed on the paper's *own* pages: a
# ScienceDirect header, a Springer footer, a JSTOR "This content downloaded"
# running line. They mark provenance, not front matter, and dropping the page
# they sit on deletes the article's first page.
#
# The distinction is load-bearing and was not obvious. Measured over pages 1-2
# of all 1775 documents here: wrapper strings hit 34 documents, imprint strings
# hit 128. A single flat marker list would have offered up all 162 as
# front-matter candidates. (corpus's _VENDOR_BOILERPLATE is a flat list, but it
# is used only to re-route to OCR, where the distinction does not bite — see
# corpus#216.)
WRAPPER_MARKERS: dict[str, tuple[str, ...]] = {
    "bhl": ("biodiversitylibrary.org",),
    "jstor_cover": ("links.jstor.org", "Your use of the JSTOR archive"),
    "google_books": ("books.google.com", "digitized by Google", "Über dieses Buch"),
    "proquest": ("ProQuest ebrary",),
    "researchgate": ("researchgate.net",),
    "blank_notice": ("This page intentionally left blank",),
}
IMPRINT_MARKERS: dict[str, tuple[str, ...]] = {
    "jstor_running": ("This content downloaded", "jstor.org/stable"),
    "sciencedirect": ("sciencedirect.com", "Available online at"),
    "springer": ("link.springer.com", "Springer-Verlag"),
    "wiley": ("onlinelibrary.wiley.com",),
    "downloaded_by": ("Downloaded by", "Downloaded from"),
}

# Unicode script buckets, enough to spot a script change mid-document. Deliberately
# coarse: the question is "did the writing system change between page 20 and 21",
# not "which language is this".
_SCRIPT_RANGES: tuple[tuple[str, int, int], ...] = (
    ("Latin", 0x0041, 0x024F),
    ("Greek", 0x0370, 0x03FF),
    ("Cyrillic", 0x0400, 0x04FF),
    ("Hebrew", 0x0590, 0x05FF),
    ("Arabic", 0x0600, 0x06FF),
    ("Devanagari", 0x0900, 0x097F),
    ("Thai", 0x0E00, 0x0E7F),
    ("Hiragana", 0x3040, 0x309F),
    ("Katakana", 0x30A0, 0x30FF),
    ("Han", 0x4E00, 0x9FFF),
    ("Hangul", 0xAC00, 0xD7AF),
)

# Stopword sets for a cheap per-page language guess. This exists for one
# reason: **to locate a translation boundary**. A script histogram cannot see a
# French original followed by an English translation — both are Latin — and
# those are the common case here. Scoring a handful of function words per page
# does, and costs nothing.
#
# Not a language identifier. It is deliberately shallow, it will disagree with
# langdetect, and it must never be written to `doclang` on its own. It is a
# pointer that says "look at page 22".
_STOPWORDS: dict[str, frozenset[str]] = {
    "en": frozenset("the of and to in is was that with for are as by be this it "
                    "which from at on have been not or their they has".split()),
    "fr": frozenset("le la les des une dans est que qui pour par sur aux avec ce "
                    "nous plus sont cette au du il elle ses".split()),
    "de": frozenset("der die das und den von zu ist mit sich auf im dem nicht ein "
                    "eine als auch es an werden bei durch".split()),
    "es": frozenset("el la los las de que en un una por con para es se del al no "
                    "su como más o sus este".split()),
    "it": frozenset("il lo la gli le di che e in un una per con non si sono come "
                    "alla dei delle nel questo".split()),
    "pt": frozenset("de que da do em os as uma para com não por mais dos das ao "
                    "na no se como".split()),
    "nl": frozenset("de het een en van is dat op te in met voor zijn niet aan die "
                    "ook als er".split()),
    "la": frozenset("et in est ad non cum quae qui quod sed ex per ut sunt esse "
                    "atque autem nec".split()),
    "ru": frozenset("и в не на что с по как это для от к но из же они мы все "
                    "или так его".split()),
}
# Below this share of recognised function words the guess is noise — a plate
# caption, a table of numbers, a page of species names.
_LANG_MIN_SCORE = 0.06

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("inspect_pages")


def find_pdfs(root: Path) -> list[Path]:
    """Every PDF under ``root``, case-insensitively.

    Not ``glob("*.pdf")``: three files in this library use an uppercase ``.PDF``
    and are invisible to that glob — Robson1973.PDF, Haddock_Case1999.PDF,
    Justetal2014.PDF. All three also lack a ``file`` field in the bib, because
    the repo's own build tooling missed them the same way.
    """
    out: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.lower().endswith(".pdf") and not name.startswith("."):
                out.append(Path(dirpath) / name)
    return sorted(out)


def script_histogram(text: str) -> dict[str, float]:
    """Fraction of *letters* in each script bucket. Empty if there are none."""
    counts: Counter[str] = Counter()
    total = 0
    for ch in text:
        if not ch.isalpha():
            continue
        cp = ord(ch)
        for name, lo, hi in _SCRIPT_RANGES:
            if lo <= cp <= hi:
                counts[name] += 1
                total += 1
                break
        else:
            # Anything alphabetic we have no bucket for still counts toward the
            # total, so a document in an unlisted script does not read as 100%
            # Latin on the strength of a stray ASCII word.
            counts[unicodedata.name(ch, "?").split()[0].title()] += 1
            total += 1
    if not total:
        return {}
    return {k: round(v / total, 3) for k, v in counts.most_common(6)}


def guess_lang(text: str) -> Optional[tuple[str, float]]:
    """Cheap stopword-frequency language guess, or None if inconclusive.

    See _STOPWORDS: this locates *boundaries*, it does not identify languages.
    """
    toks = [t for t in "".join(
        c.lower() if (c.isalpha() or c.isspace()) else " " for c in text
    ).split() if len(t) > 1]
    if len(toks) < 25:
        return None
    counts = {lang: sum(1 for t in toks if t in words)
              for lang, words in _STOPWORDS.items()}
    lang, hits = max(counts.items(), key=lambda kv: kv[1])
    score = hits / len(toks)
    if score < _LANG_MIN_SCORE:
        return None
    return lang, round(score, 3)


def find_markers(text: str, table: dict[str, tuple[str, ...]]) -> list[str]:
    return [v for v, needles in table.items() if any(n in text for n in needles)]


def image_coverage(page) -> tuple[float, float]:
    """(total, largest-single) image coverage as fractions of page area.

    Clamped to 1.0 — overlapping or repeated placements of the same image can
    otherwise sum past the page, and the number is only ever used as "is this
    page mostly a bitmap".
    """
    area = abs(page.rect.get_area())
    if area <= 0:
        return 0.0, 0.0
    total = 0.0
    largest = 0.0
    try:
        infos = page.get_image_info()
    except Exception:  # pragma: no cover — malformed page objects
        return 0.0, 0.0
    for info in infos:
        bbox = info.get("bbox")
        if not bbox:
            continue
        w = max(0.0, bbox[2] - bbox[0])
        h = max(0.0, bbox[3] - bbox[1])
        frac = (w * h) / area
        total += frac
        largest = max(largest, frac)
    return round(min(total, 1.0), 3), round(min(largest, 1.0), 3)


def classify_page(chars: int, max_img: float, has_vector: bool) -> str:
    """One of text / image / sparse / blank.

    The distinction that matters is image vs blank, and it is not answerable
    from the text layer: both read as zero characters. A page carrying a
    full-bleed bitmap is content that simply has not been OCR'd; a page
    carrying nothing is padding. Only the raster layer separates them.
    """
    if chars >= NEAR_EMPTY_CHARS:
        # Real text, but a full-page image underneath means the text layer is a
        # scan's OCR or a vendor banner rather than digital typesetting. The
        # caller can tell those apart with img_cov.
        return "text"
    if max_img >= SCAN_IMG_COVERAGE:
        return "image"
    if has_vector or max_img > 0.05:
        return "sparse"
    return "blank"


def leading_run(kinds: Iterable[str], want: set[str]) -> int:
    n = 0
    for k in kinds:
        if k in want:
            n += 1
        else:
            break
    return n


def inspect_pdf(path: Path) -> dict:
    """Measure one PDF. Never raises — a failure is recorded, not fatal."""
    import pymupdf as fitz

    rec: dict = {
        "file": path.name,
        "path": str(path.relative_to(REPO)),
        "pages": 0,
        "error": None,
        "page": [],
    }
    try:
        doc = fitz.open(path)
    except Exception as e:
        rec["error"] = f"open failed: {e}"
        return rec

    try:
        rec["pages"] = len(doc)
        chars_total = 0
        last_lang: Optional[str] = None
        for i, page in enumerate(doc, start=1):
            try:
                text = page.get_text() or ""
            except Exception as e:  # pragma: no cover
                text = ""
                rec.setdefault("page_errors", []).append(f"{i}: {e}")
            chars = len(text.strip())
            chars_total += chars
            img_cov, max_img = image_coverage(page)
            try:
                has_vector = bool(page.get_drawings())
            except Exception:  # pragma: no cover
                has_vector = False
            entry = {
                "n": i,
                "chars": chars,
                "img_cov": img_cov,
                "max_img": max_img,
                "kind": classify_page(chars, max_img, has_vector),
            }
            scripts = script_histogram(text)
            if scripts:
                entry["scripts"] = scripts
            markers = find_markers(text, WRAPPER_MARKERS)
            if markers:
                entry["markers"] = markers
            imprints = find_markers(text, IMPRINT_MARKERS)
            if imprints:
                entry["imprints"] = imprints
            guess = guess_lang(text)
            if guess:
                entry["lang"], entry["lang_score"] = guess

            # Keep a short head of text at the places a boundary announces
            # itself, so the annotator can read a heading without rendering a
            # page: the first pages, a script change, and — the case a script
            # histogram is blind to — a *language* change within one script,
            # which is what a French original followed by an English
            # translation looks like.
            prev = rec["page"][-1] if rec["page"] else None
            script_change = bool(
                chars and prev and scripts and scripts != prev.get("scripts"))
            # Compare against the last page that had a confident guess, not
            # against the immediately previous page. A boundary very often
            # *sits on* an inconclusive page: Chun1881_tr p6 carries the end of
            # the German article plus the start of an unrelated Latin one, so
            # it scores below threshold, and comparing only with p6 would hide
            # the German->English change at p7 entirely.
            lang_change = bool(
                guess and last_lang and guess[0] != last_lang)
            if i <= 3 or script_change or lang_change:
                entry["head"] = " ".join(text.split())[:220]
                if lang_change:
                    entry["boundary"] = f"{last_lang} -> {guess[0]}"
                elif script_change:
                    entry["boundary"] = "script change"
            if guess:
                last_lang = guess[0]
            rec["page"].append(entry)

        kinds = [p["kind"] for p in rec["page"]]
        n = max(len(kinds), 1)
        rec["chars_total"] = chars_total
        rec["chars_per_page"] = round(chars_total / n, 1)
        rec["kind_counts"] = dict(Counter(kinds))
        # Blank and image runs are reported separately and on purpose. A
        # trailing run of `blank` is padding and is safe to drop; a trailing
        # run of `image` is un-OCR'd content and dropping it deletes the paper.
        # Bennett1860 is the case that makes this concrete: 21 trailing pages,
        # every one of them the actual article, all invisible to the text layer.
        rec["leading_blank"] = leading_run(kinds, {"blank"})
        rec["trailing_blank"] = leading_run(reversed(kinds), {"blank"})
        rec["leading_nontext"] = leading_run(kinds, {"blank", "image", "sparse"})
        rec["trailing_nontext"] = leading_run(reversed(kinds),
                                              {"blank", "image", "sparse"})
        rec["langs"] = sorted({p["lang"] for p in rec["page"] if p.get("lang")})
        rec["boundaries"] = [
            {"page": p["n"], "at": p["boundary"], "head": p.get("head", "")[:120]}
            for p in rec["page"] if p.get("boundary")
        ]
        rec["wrappers"] = sorted({m for p in rec["page"] for m in p.get("markers", [])})
        # Recorded, but explicitly NOT a reason to drop anything.
        rec["imprints"] = sorted({m for p in rec["page"] for m in p.get("imprints", [])})
        # Page 1 looks like a cover: short prose, with page 2 substantially
        # longer. Low precision on its own — it also fires on a genuine article
        # title page — but it is the only signal that finds the commonest
        # front matter in this library, a bound volume's *journal title page*,
        # which carries no vendor string at all (Agassiz, Aurivillius, Bedot,
        # Balfour). Offered as a pointer for review, never as a verdict.
        ps = rec["page"]
        rec["cover_shaped"] = bool(
            len(ps) >= 3
            and 150 <= ps[0]["chars"] <= 2000
            and ps[1]["chars"] > 2.0 * ps[0]["chars"]
        )
        rec["scripts"] = script_histogram(
            "".join(p.get("head", "") for p in rec["page"])
        )
        # No usable text layer -> the annotator must look at rendered pages.
        rec["text_layer"] = (
            "none" if rec["chars_per_page"] < TEXT_LAYER_CHARS_PER_PAGE else "full"
        )
        # Reasons a text-only read cannot settle this document. A list rather
        # than a boolean: the reasons differ in kind and in cost, and the
        # annotator should triage on *which* one fired, not on whether any did.
        #
        # An earlier version was a single `needs_images` disjunction, and it
        # had a hole worth remembering: the multilingual clause was
        # `len(langs) > 1`, which cannot fire when the scorer returned nothing
        # at all. Documents too garbled to classify — the ones most likely to
        # need a human — were therefore marked quiet. `lang_inconclusive`
        # exists to close that.
        reasons: list[str] = []
        if rec["text_layer"] == "none":
            reasons.append("no_text_layer")
        if rec["leading_nontext"]:
            reasons.append("leading_nontext")
        if rec["trailing_nontext"]:
            reasons.append("trailing_nontext")
        if rec["wrappers"]:
            reasons.append("wrapper")
        if rec["cover_shaped"]:
            reasons.append("cover_shaped")
        if len(rec["langs"]) > 1:
            reasons.append("multilingual")
        if not rec["langs"] and rec["text_layer"] != "none":
            # Text present but unscoreable: garbled OCR, or a language the
            # scorer has no stopwords for. Either way, unexamined.
            reasons.append("lang_inconclusive")
        if any(all(q["kind"] != "text" for q in ps[i:i + 3])
               for i in range(1, max(1, len(ps) - 4))):
            # A run of non-text pages away from the ends — an inserted plate
            # section, a bound-in second paper, a scanning gap.
            reasons.append("mid_doc_nontext_run")
        if rec["pages"] > 100:
            # Long enough to be a bound volume rather than a single paper.
            reasons.append("long_document")
        rec["review_reasons"] = reasons
        rec["needs_images"] = bool(reasons)
    finally:
        doc.close()
    return rec


def render_contact_sheet(path: Path, out_png: Path, dpi: int = 40,
                         cols: int = 5, rows: int = 4, first: int = 1,
                         last: Optional[int] = None) -> Optional[Path]:
    """Render pages into one grid image for visual review. Needs Pillow.

    Grayscale JPEG, not PNG. These are scans of print, so colour carries no
    information, and the sheet exists to answer structural questions — is this
    page a title page, a plate, blank, blackletter — not to be read closely. A
    lossless RGB sheet of 20 pages runs to several MB, which is a poor thing to
    hand an annotator once per document across a 1,775-document library.
    """
    import pymupdf as fitz
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        log.error("--sheets needs Pillow (conda install -c conda-forge pillow)")
        return None

    doc = fitz.open(path)
    try:
        last = min(last or len(doc), len(doc))
        idxs = list(range(first - 1, last))
        if not idxs:
            return None
        tiles = []
        for i in idxs:
            pix = doc[i].get_pixmap(dpi=dpi, colorspace=fitz.csGRAY)
            img = Image.frombytes("L", (pix.width, pix.height), pix.samples)
            tiles.append((i + 1, img))
        tiles = tiles[: cols * rows]
        tw = max(t.width for _, t in tiles)
        th = max(t.height for _, t in tiles)
        label = 14
        # Fit the grid to the tiles actually present. A fixed 5x4 grid spends
        # most of a six-page document's sheet on empty cells, which wastes both
        # the pixel cap below and the image tokens a reader pays for it.
        ncols = min(cols, len(tiles))
        nrows = (len(tiles) + ncols - 1) // ncols
        sheet = Image.new("L", (ncols * tw, nrows * (th + label)), 255)
        draw = ImageDraw.Draw(sheet)
        for k, (pageno, img) in enumerate(tiles):
            cx = (k % ncols) * tw
            cy = (k // ncols) * (th + label)
            sheet.paste(img, (cx, cy))
            draw.text((cx + 2, cy + th + 1), f"p{pageno}", fill=0)
        # Cap the long edge. A 5x4 grid at 40 dpi is ~1700x1800, and a vision
        # model downsamples to roughly this anyway — rendering larger costs
        # bytes and image tokens without adding anything readable.
        longest = max(sheet.size)
        if longest > MAX_SHEET_PX:
            scale = MAX_SHEET_PX / longest
            sheet = sheet.resize(
                (max(1, int(sheet.width * scale)), max(1, int(sheet.height * scale))),
                Image.LANCZOS,
            )
        out_png.parent.mkdir(parents=True, exist_ok=True)
        sheet.save(out_png, format="JPEG", quality=70, optimize=True)
        return out_png
    finally:
        doc.close()


def render_sheets_for(src: Path, n_pages: int, sheet_dir: Path,
                      per_sheet: int = 20) -> list[Path]:
    """Contact sheets covering the whole document, 20 pages each.

    Named ``<stem>_p0001.png``, ``<stem>_p0021.png``, … so the page range a
    sheet covers is legible from the filename alone.
    """
    out: list[Path] = []
    for start in range(1, max(n_pages, 1) + 1, per_sheet):
        dest = sheet_dir / f"{src.stem}_p{start:04d}.jpg"
        got = render_contact_sheet(src, dest, first=start,
                                   last=min(start + per_sheet - 1, n_pages))
        if got:
            out.append(got)
    return out


def bib_priors() -> dict[str, dict]:
    """Lowercased PDF basename -> the bib facts an annotator needs."""
    if not BIB.exists():
        return {}
    doc = parse_bib(BIB.read_text(encoding="utf-8"))
    out: dict[str, dict] = {}
    for name, entry in doc.by_file().items():
        prior = {"bib_key": entry.key}
        for f in ("bhl_title", "keeppages", "doclang", "pagemap", "ocrlang",
                  "title", "year"):
            v = entry.get(f)
            if v:
                prior[f] = v
        url = entry.get("url") or ""
        if "biodiversitylibrary.org" in url:
            prior["bhl_url"] = True
        out[name] = prior
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=LIBRARY,
                    help=f"Directory to walk (default: {LIBRARY.name}/)")
    ap.add_argument("--pdf", action="append", default=None, metavar="NAME",
                    help="Inspect only these basenames; print rather than write. Repeatable.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N documents (smoke test).")
    ap.add_argument("--out", type=Path, default=OUT_JSON)
    ap.add_argument("--sheets", action="store_true",
                    help="Also render contact sheets for documents flagged needs_images.")
    ap.add_argument("--sheet-dir", type=Path, default=SHEET_DIR)
    args = ap.parse_args()

    try:
        import pymupdf  # noqa: F401
    except ImportError:
        raise SystemExit(
            "PyMuPDF is required. It is declared in environment.yaml; if this "
            "env predates that, run:  conda install -c conda-forge pymupdf"
        )

    pdfs = find_pdfs(args.root)
    if args.pdf:
        wanted = {p.lower() for p in args.pdf}
        pdfs = [p for p in pdfs if p.name.lower() in wanted]
        missing = wanted - {p.name.lower() for p in pdfs}
        if missing:
            log.warning("not found under %s: %s", args.root, ", ".join(sorted(missing)))
    if args.limit:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        raise SystemExit(f"no PDFs found under {args.root}")

    priors = bib_priors()
    log.info("inspecting %d PDFs under %s (%d bib priors)",
             len(pdfs), args.root, len(priors))

    records = []
    for i, path in enumerate(pdfs, start=1):
        rec = inspect_pdf(path)
        rec["bib"] = priors.get(path.name.lower())
        if rec["bib"] is None:
            rec["unreferenced"] = True
        records.append(rec)
        if i % 200 == 0:
            log.info("  %d/%d", i, len(pdfs))

    if args.pdf:
        # Render first, so `--pdf X --sheets` works. The bulk path below is
        # unreachable in this mode, and an annotator looking at one document is
        # exactly who needs a rendered page most.
        if args.sheets:
            for r in records:
                for out in render_sheets_for(REPO / r["path"], r["pages"],
                                             args.sheet_dir):
                    log.info("wrote %s", out)
        print(json.dumps(records, indent=2, ensure_ascii=False))
        return 0

    total_pages = sum(r["pages"] for r in records)
    needs_images = [r for r in records if r.get("needs_images")]
    no_text = [r for r in records if r.get("text_layer") == "none"]
    failed = [r for r in records if r.get("error")]
    vendor_counts: Counter[str] = Counter()
    imprint_counts: Counter[str] = Counter()
    for r in records:
        for v in r.get("wrappers", []):
            vendor_counts[v] += 1
        for v in r.get("imprints", []):
            imprint_counts[v] += 1

    summary = {
        "documents": len(records),
        "pages": total_pages,
        "no_text_layer": len(no_text),
        "needs_images": len(needs_images),
        "review_reasons": dict(
            sum((Counter(r.get("review_reasons", [])) for r in records),
                Counter()).most_common()),
        "unreferenced_by_bib": sum(1 for r in records if r.get("unreferenced")),
        "failed_to_open": len(failed),
        "wrapper_documents": dict(vendor_counts.most_common()),
        "imprint_documents": dict(imprint_counts.most_common()),
        "cover_shaped": sum(1 for r in records if r.get("cover_shaped")),
        "page_kinds": dict(sum((Counter(r.get("kind_counts", {})) for r in records),
                               Counter())),
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps({"summary": summary, "documents": records},
                   indent=1, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("wrote %s", args.out)
    print(json.dumps(summary, indent=2))

    if args.sheets:
        made = 0
        for r in needs_images:
            made += len(render_sheets_for(REPO / r["path"], r["pages"],
                                          args.sheet_dir))
        log.info("rendered %d contact sheets into %s", made, args.sheet_dir)

    if failed:
        log.warning("%d PDFs failed to open: %s",
                    len(failed), ", ".join(r["file"] for r in failed[:5]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
