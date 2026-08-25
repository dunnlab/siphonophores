#!/usr/bin/env python3
"""Phase 1: diff each gold page against poppler's independent extraction.

Writes groundtruth/confidence.json with a per-page similarity, so the targeted
second pass has something to aim at.

The similarity is a TRIAGE SIGNAL, NOT A VERDICT. Two things it cannot do, both
learned the hard way and both recorded in RUNBOOK.md:

  * It cannot catch invention. Fluent invented prose does not diff as an error
    against a text layer that simply lacks it -- it reads as the transcriber
    having recovered something the OCR missed. That is why pass 1 was Opus-only
    rather than relying on this diff as a safety net.
  * A low volume ratio is not evidence of loss. Lery1594's 0.52x ratio looked
    like content loss and turned out to be poppler carrying more noise.

So a low score means "look again", never "the gold page is wrong". Where the
poppler layer is garbage -- roman OCR over Fraktur, or absent entirely -- a low
score says nothing at all about the gold page, and this script reports that case
separately rather than letting it masquerade as disagreement.

Usage:
    crosscheck.py [--out confidence.json]
"""
import argparse
import json
import pathlib
import re
import subprocess
import unicodedata
from collections import Counter
from difflib import SequenceMatcher, get_close_matches

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

# Below this many characters the poppler layer carries no usable signal, so the
# page cannot be triaged mechanically at all. Measured: Adanson1757,
# Hjortberg1769 and LoBianco1909 sit at 1 char/page.
NO_SIGNAL_CHARS = 50

# Markers whose bracketed payload is transcriber commentary, not printed text.
# These must be dropped before diffing or they inflate the disagreement.
DROP_WHOLE = ("NOTE:", "PAGE ", "BLANK PAGE", "illegible", "no lettering")

# Markers that label printed text: drop the tag, keep what follows.
KEEP_AFTER_TAG = ("RUNNING HEAD", "FOOTNOTE", "MARGIN", "STAMP", "HANDWRITTEN")

# Structural tags: drop the tag, keep the block contents.
STRUCTURAL = ("FIGURE", "/FIGURE", "PLATE", "/PLATE", "TABLE", "/TABLE")


def spans(text):
    """Yield (start, end) of top-level bracketed spans, respecting nesting.

    Notes routinely quote other markers inside themselves (48 instances of
    'inside [FIGURE]' corpus-wide), so a non-nesting regex would end a [NOTE:]
    at the wrong bracket and leak commentary into the compared text.
    """
    depth, start = 0, None
    for i, ch in enumerate(text):
        if ch == "[":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "]" and depth:
            depth -= 1
            if depth == 0:
                yield start, i + 1


def strip_markup(text):
    """Reduce a gold page to just the text printed on the page."""
    out, pos = [], 0
    for a, b in spans(text):
        out.append(text[pos:a])
        pos = b
        inner = text[a + 1:b - 1]
        if inner.startswith("?"):
            # [?reading] -- an uncertain reading. Keep the transcriber's best
            # guess: it is what they actually saw on the page.
            out.append(inner[1:].split(":", 1)[-1] if inner.startswith("?reading:") else inner[1:])
        elif any(inner.startswith(d) for d in DROP_WHOLE):
            pass
        elif inner in STRUCTURAL:
            pass
        elif any(inner.startswith(k) for k in KEEP_AFTER_TAG):
            pass  # the tag is its own span; following text is outside it
    out.append(text[pos:])
    return "".join(out)


# CJK ranges plus kana. These scripts are not written with spaces, so
# whitespace tokenisation would yield a handful of enormous tokens and the
# similarity would be meaningless; they are compared character by character.
CJK = re.compile(r"[぀-ヿ㐀-䶿一-鿿豈-﫿ｦ-ﾟ]")


NONLATIN = re.compile(r"[぀-ヿ㐀-䶿一-鿿豈-﫿ｦ-ﾟ\u0400-\u04ff\u0370-\u03ff]")


def script_lost(gold, pop):
    """True if the gold page is substantially non-Latin but the text layer
    contains almost none of that script.

    A text layer that cannot represent the page's writing system yields a score
    near zero that looks like total disagreement. It is not disagreement -- it
    is absence of signal, and it must not be reported as a gold-page problem.
    """
    g = NONLATIN.findall(gold)
    if len(g) < 0.15 * max(len(gold.split()), 1):
        return False           # gold is not meaningfully non-Latin
    return len(NONLATIN.findall(pop)) < 0.02 * len(g)


def tokens(text):
    """Normalise aggressively -- we are measuring agreement on words, not on
    typography. OCR mangles case, spacing and punctuation constantly and those
    differences are not what we want to triage on.

    Must be Unicode-aware: this corpus is 13 languages, and an ASCII-only filter
    silently deletes the Cyrillic, Greek and CJK documents' actual content,
    leaving only stray Latin species names to compare. That produces a
    confident-looking score computed from a few percent of the page.
    """
    text = unicodedata.normalize("NFKD", text.lower())
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = text.replace("_", " ")
    out = []
    for tok in text.split():
        if CJK.search(tok):
            out.extend(tok)
        else:
            out.append(tok)
    return out


def regen_provenance():
    """Re-extract the poppler comparison layer from the source PDFs.

    This layer is NOT committed: it is derived from PDFs that are already in the
    repo, it doubles the file count, and it carries no information the PDFs do
    not. Regenerate it here when you want to reproduce confidence.json.

    Caveat worth stating: poppler's output changes between versions, so a
    regeneration on anything other than the recorded version (24.02.0) will not
    reproduce the committed numbers exactly. That is expected -- record the
    version you used rather than assuming a mismatch is a defect.
    """
    src = json.loads((DATA / "sources.json").read_text())
    made = 0
    for d in document_dirs():
        entry = src.get(d.name)
        if entry is None:
            print(f"  skip {d.name}: not in sources.json")
            continue
        pdf = DATA.parent / entry["pdf"]
        if not pdf.exists():
            print(f"  skip {d.name}: source PDF missing (git lfs pull?)")
            continue
        out = d / "_provenance" / "poppler"
        out.mkdir(parents=True, exist_ok=True)
        for gf in sorted(d.glob("page_*.txt")):
            n = int(gf.stem.split("_")[1])
            # -layout is REQUIRED to reproduce the committed confidence.json:
            # the original layer was extracted with it and is byte-identical
            # only with it. Note the side effect, because it explains one of the
            # failure modes in CROSSCHECK_REPORT.md: -layout preserves the
            # spatial arrangement, so a two-column page comes out with its
            # columns interleaved line by line. That depresses the
            # order-sensitive `similarity` score while saying nothing about the
            # gold page -- which is exactly why the order-insensitive `coverage`
            # and `recall` measures exist beside it.
            subprocess.run(["pdftotext", "-layout", "-f", str(n), "-l", str(n),
                            str(pdf), str(out / gf.name)], check=True)
            made += 1
    ver = subprocess.run(["pdftotext", "-v"], capture_output=True).stderr.decode(
        "utf8", "replace").splitlines()
    print(f"extracted {made} pages with {ver[0] if ver else 'pdftotext'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "confidence.json"))
    ap.add_argument("--regen-provenance", action="store_true",
                    help="re-extract the (uncommitted) poppler layer, then exit")
    a = ap.parse_args()

    if a.regen_provenance:
        regen_provenance()
        return

    docs, pages_out = {}, []
    for d in document_dirs():
        pop_dir = d / "_provenance" / "poppler"
        recs = []
        for gf in sorted(d.glob("page_*.txt")):
            n = int(gf.stem.split("_")[1])
            gold_raw = gf.read_text(encoding="utf-8")
            gold = strip_markup(gold_raw)
            pf = pop_dir / gf.name
            pop = pf.read_text(encoding="utf-8", errors="replace") if pf.exists() else ""

            gt, pt = tokens(gold), tokens(pop)
            if not pt or len("".join(pt)) < NO_SIGNAL_CHARS:
                sim, cov, recall, excess_wordlike, excess_novel, status = None, None, None, None, None, "no_signal"
            elif not gt:
                sim, cov, recall, excess_wordlike, excess_novel, status = None, None, None, None, None, "gold_empty"
            elif script_lost(gold, pop):
                # The page is written in a script the text layer does not
                # contain at all -- e.g. Kawamura1911a pp.14-25, the original
                # vertical Japanese, where poppler returns roman OCR noise
                # ("H5-h>^ -^pg-f") because it is reading kanji as latin.
                # Scoring these would report ~0.00 agreement and read as a
                # catastrophic gold failure, when in fact there is simply
                # nothing to compare against. This is a PER-PAGE property:
                # Kawamura's English first half cross-checks perfectly.
                sim, cov, recall, excess_wordlike, excess_novel, status = None, None, None, None, None, "script_lost"
            else:
                sim = round(SequenceMatcher(None, gt, pt, autojunk=False).ratio(), 4)
                # Order-INSENSITIVE companion: what fraction of the gold page's
                # words appear in poppler at all, as a multiset. SequenceMatcher
                # punishes reordering as hard as it punishes wrong content, and
                # poppler routinely scrambles two-column reading order -- so a
                # page can score low on `similarity` while the gold text is
                # perfect. Reading the two together separates the cases:
                #   low sim + high coverage -> reading-order difference only
                #   low sim + low  coverage -> genuine content disagreement
                gc, pc = Counter(gt), Counter(pt)
                inter = sum((gc & pc).values())
                cov = round(inter / len(gt), 4)
                # THE ONE THAT MATTERS FOR TARGETING. `coverage` above asks how
                # much of the gold appears in poppler, and is LOW for exactly the
                # pages where the gold is best: whole-page figures and engraved
                # plates, where the transcriber recovered lettering that exists
                # only as image and has no text layer (Ahuja p8: 1007 gold words
                # vs 54 in poppler, all of it the caption). Flagging those sends
                # the second pass to re-transcribe pages that are already right.
                #
                # `recall` asks the opposite and far more dangerous question:
                # how much of what poppler SAW is missing from the gold page?
                # That is the shape of a genuine omission -- it is what Mańko p9
                # looked like when a session limit killed the agent mid-page and
                # both prose columns were lost.
                recall = round(inter / len(pt), 4)
                # Is poppler's EXCESS real text, or is it noise? Low recall has
                # two utterly different causes and they must not be conflated:
                #   (a) poppler read text the gold page is missing -- a real
                #       omission, the thing worth finding;
                #   (b) poppler hallucinated character salad out of image
                #       texture -- Linnaeus1735 p1 is the binding's marbled
                #       front board, where 6 words of gold-stamped spine label
                #       are correct and poppler's "70 words" are noise
                #       ("¿rit 1H äL «•f ^H").
                # Word-likeness separates them cheaply: real words are
                # alphabetic, of reasonable length, and contain a vowel.
                excess = list((pc - gc).elements())
                real = [w for w in excess if len(w) >= 3 and w.isalpha()
                        and re.search(r"[aeiouyаеиоуяыэюі]", w)]
                excess_wordlike = round(len(real) / len(excess), 4) if excess else 0.0
                # Final discriminator, and the one that makes this list usable.
                # A word-like excess token is still not evidence of a gold
                # omission if it is merely a MANGLED FORM of a word the gold
                # already has. DeHaan1827 is the case in point: poppler renders
                #   "Vdica integra ; bnchia bafi ramofa t aequllia"
                # where the gold correctly reads
                #   "Veſica integra ; brachia baſi ramoſa , aequalia".
                # Same content, corrupted OCR -- and 1827 long-s typography
                # guarantees a mismatch on nearly every word, because poppler
                # reads "ſ" as "f". Those pages are not omissions and
                # re-transcribing them would reproduce the identical gold text.
                #
                # `excess_novel` counts only excess words with NO near-match in
                # the gold page. Genuinely missing content looks like whole
                # unfamiliar words; OCR damage looks like near-misses.
                goldset = set(gt)
                novel = [w for w in real if not get_close_matches(w, goldset, n=1, cutoff=0.75)]
                excess_novel = round(len(novel) / len(real), 4) if real else 0.0
                status = "ok"

            recs.append({
                "page": n,
                "similarity": sim,
                "coverage": cov,
                "recall": recall,
                "excess_wordlike": excess_wordlike,
                "excess_novel": excess_novel,
                "status": status,
                "gold_words": len(gt),
                "poppler_words": len(pt),
                "volume_ratio": round(len(gt) / len(pt), 3) if pt else None,
                "uncertain": len(re.findall(r"\[\?", gold_raw)),
                "illegible": len(re.findall(r"\[illegible", gold_raw)),
            })

        scored = [r["similarity"] for r in recs if r["similarity"] is not None]
        covs = [r["coverage"] for r in recs if r["coverage"] is not None]
        recs_r = [r["recall"] for r in recs if r["recall"] is not None]
        docs[d.name] = {
            "pages": len(recs),
            "scored": len(scored),
            "no_signal": sum(1 for r in recs if r["status"] == "no_signal"),
            "median_similarity": round(sorted(scored)[len(scored) // 2], 4) if scored else None,
            "median_coverage": round(sorted(covs)[len(covs) // 2], 4) if covs else None,
            "median_recall": round(sorted(recs_r)[len(recs_r) // 2], 4) if recs_r else None,
            "uncertain": sum(r["uncertain"] for r in recs),
            "illegible": sum(r["illegible"] for r in recs),
        }
        pages_out.append({"document": d.name, "pages": recs})

    out = {
        "generated": "2026-08-24",
        "method": "token-level SequenceMatcher, gold markup stripped, "
                  "case/diacritic/punctuation normalised",
        "caveat": "TRIAGE SIGNAL ONLY. Cannot detect invention; a low volume "
                  "ratio is not evidence of loss. Where poppler is garbage "
                  "(Fraktur, no text layer) a low score says nothing about the "
                  "gold page.",
        "no_signal_threshold_chars": NO_SIGNAL_CHARS,
        "documents": docs,
        "detail": pages_out,
    }
    pathlib.Path(a.out).write_text(json.dumps(out, indent=1, ensure_ascii=False))
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
