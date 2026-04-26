#!/usr/bin/env python3
"""Generate ``siphonophores.bib`` from parsed entries + DOI / Crossref lookups.

Inputs (all under ``build/``):
    * entries.json     — raw parsed docx entries (output of parse_docx.py)
    * match.json       — entries enriched with PDF matches (match_library.py)
    * dois.json        — verified DOIs extracted from PDF text (extract_dois.py)
    * crossref_dois.json — DOIs found via Crossref title/author search (crossref_lookup.py)

Output:
    * siphonophores.bib in repo root.

Per-entry rules:
    * Citation key: derived from the matched PDF basename when available,
      otherwise from author+year (the same way Pugh's filenames are formed).
    * DOI: prefer the PDF-extracted DOI if both PDF and Crossref agree; else
      prefer Crossref-search (because PDF text-extraction sometimes captures a
      reference's DOI rather than the paper's). Mismatches are logged.
    * BHL part GUID: extracted from any DOI of the form ``10.5962/bhl.part.<n>``.
      Volume-level BHL DOIs (``10.5962/bhl.title.<n>``) are also stored on the
      ``bhl_title`` field but are NOT used as ``bhl_part`` since they aren't
      article-level.
    * URL fallback order: doi -> bhl part -> journal/Crossref ``url``.
      Each candidate URL is sanity-checked for syntactic plausibility; we don't
      hit the network at this stage.

Run::

    python scripts/build_bib.py
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "build"
LOGS = REPO / "logs"
LOGS.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "build_bib.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("build_bib")

OUT_BIB = REPO / "siphonophores.bib"

BHL_PART_RE = re.compile(r"^10\.5962/bhl\.part\.(\d+)$", re.IGNORECASE)
BHL_TITLE_RE = re.compile(r"^10\.5962/bhl\.title\.(\d+)$", re.IGNORECASE)
DOI_RE = re.compile(r"^10\.\d{4,9}/.+$")


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z0-9]+", "", s)


def make_key(entry: dict) -> str:
    """Citation key for the bib record.

    Prefer the PDF basename (which Pugh hand-curated); fall back to a
    constructed surname-year-suffix key otherwise.
    """
    if entry.get("pdf_basename"):
        stem = Path(entry["pdf_basename"]).stem
        # If the stem ends with descriptive _Tag, drop everything after the year.
        m = re.match(r"^(.+?\d{4}[a-z]*)", stem)
        return fold(m.group(1) if m else stem)
    # constructed
    if entry.get("authors") and entry.get("year"):
        a = entry["authors"]
        first_sn = fold(a[0].split(",", 1)[0])
        if len(a) == 1:
            base = first_sn
        elif len(a) == 2:
            base = first_sn + fold(a[1].split(",", 1)[0])
        else:
            base = first_sn + "Etal"
        return base + entry["year"] + (entry["year_suffix"] or "")
    return f"unknown{abs(hash(entry['raw'])) % 100000:05d}"


def escape_bibtex(value: str, *, is_url_or_id: bool = False) -> str:
    """Escape a value for BibTeX.

    `is_url_or_id` keeps URL-friendly characters (``&``, ``_``, ``#``) intact,
    since they appear unescaped inside braces in DOIs and URLs.
    """
    if value is None:
        return ""
    s = str(value)
    if is_url_or_id:
        # In braced URLs/DOIs, only stray braces or backslashes need handling.
        return s
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("\\", "\\textbackslash{}")
    # Stray braces (OCR errors in the docx, etc.) break BibTeX parsing —
    # balance them by replacing unmatched ``{`` / ``}`` with their square
    # bracket equivalents.
    open_count = s.count("{")
    close_count = s.count("}")
    if open_count != close_count:
        # Walk the string and substitute the unmatched ones.
        out_chars = []
        depth = 0
        # First pass: turn unmatched closing braces into ']'.
        for ch in s:
            if ch == "{":
                depth += 1
                out_chars.append(ch)
            elif ch == "}":
                if depth > 0:
                    depth -= 1
                    out_chars.append(ch)
                else:
                    out_chars.append("]")
            else:
                out_chars.append(ch)
        # Second pass (right-to-left): turn unmatched openers into '['.
        s = "".join(out_chars)
        out_chars = list(s)
        depth = 0
        for i in range(len(out_chars) - 1, -1, -1):
            ch = out_chars[i]
            if ch == "}":
                depth += 1
            elif ch == "{":
                if depth > 0:
                    depth -= 1
                else:
                    out_chars[i] = "["
        s = "".join(out_chars)
    s = s.replace("&", "\\&")
    s = s.replace("%", "\\%")
    s = s.replace("$", "\\$")
    s = s.replace("#", "\\#")
    s = s.replace("~", "\\textasciitilde{}")
    s = s.replace("^", "\\textasciicircum{}")
    return s


def authors_to_bib(authors: list[str]) -> str:
    """Convert ['Surname, F.M.', ...] into BibTeX ``Surname, First and ...`` form.

    BibTeX expects ``and`` as separator. Surnames are kept as-is.
    """
    parts = []
    for a in authors:
        a = a.strip()
        if not a:
            continue
        # Make sure there's at most one comma (BibTeX won't like extras)
        if a.count(",") > 1:
            head, *rest = a.split(",")
            a = head + ", " + " ".join(p.strip() for p in rest)
        parts.append(a)
    return " and ".join(parts)


def make_pages(p: str | None) -> str | None:
    if not p:
        return None
    p = p.replace("–", "-").replace("—", "-")
    # Collapse already-doubled dashes back, then double the single dash.
    p = re.sub(r"-+", "-", p)
    return p.replace("-", "--")


def derive_bhl_part(doi: str | None) -> str | None:
    if not doi:
        return None
    m = BHL_PART_RE.match(doi)
    return m.group(1) if m else None


def derive_bhl_title(doi: str | None) -> str | None:
    if not doi:
        return None
    m = BHL_TITLE_RE.match(doi)
    return m.group(1) if m else None


def title_similarity(a: str, b: str) -> int:
    if not a or not b:
        return 0
    from rapidfuzz import fuzz as _fuzz

    return _fuzz.token_set_ratio(a.lower(), b.lower())


def pick_doi(
    *,
    pdf_doi: str | None,
    pdf_title: str | None,
    cr_doi: str | None,
    cr_score: float | None,
    entry_title: str | None,
) -> tuple[str | None, str]:
    """Pick best DOI between PDF-extracted and Crossref-search results.

    Source is one of: ``pdf``, ``crossref``, ``agree``, ``crossref-mismatch``,
    ``pdf-mismatch``, ``none``.
    """
    if pdf_doi and cr_doi:
        if pdf_doi.lower() == cr_doi.lower():
            return pdf_doi.lower(), "agree"
        # Disagree: compare each candidate's reported title against our entry
        # title and pick whichever is more similar.
        pdf_sim = title_similarity(entry_title or "", pdf_title or "")
        # crossref score is in cr_score (0-100, token_set_ratio of titles)
        cr_sim = cr_score if cr_score is not None else 0
        if pdf_sim >= cr_sim and pdf_sim >= 80:
            return pdf_doi.lower(), "pdf-mismatch"
        return cr_doi.lower(), "crossref-mismatch"
    if pdf_doi:
        return pdf_doi.lower(), "pdf"
    if cr_doi:
        return cr_doi.lower(), "crossref"
    return None, "none"


def pick_url(*, doi: str | None, bhl_part: str | None, journal_url: str | None) -> str | None:
    if doi and DOI_RE.match(doi):
        return f"https://doi.org/{doi}"
    if bhl_part:
        return f"https://www.biodiversitylibrary.org/part/{bhl_part}"
    if journal_url and journal_url.startswith(("http://", "https://")):
        return journal_url
    return None


URLY = {"doi", "url", "bhl_part", "bhl_title", "file"}


def emit_record(out, *, key: str, fields: dict[str, str]) -> None:
    """Write a single @article record."""
    out.write(f"@article{{{key},\n")
    keys = [
        "author",
        "year",
        "title",
        "journal",
        "volume",
        "number",
        "pages",
        "doi",
        "bhl_part",
        "bhl_title",
        "url",
        "file",
        "note",
    ]
    seen = set()
    for k in keys:
        if k in fields and fields[k]:
            v = escape_bibtex(fields[k], is_url_or_id=k in URLY)
            out.write(f"  {k} = {{{v}}},\n")
            seen.add(k)
    for k, v in fields.items():
        if v and k not in seen:
            v = escape_bibtex(v, is_url_or_id=k in URLY)
            out.write(f"  {k} = {{{v}}},\n")
    out.write("}\n\n")


def make_bib_fields(
    entry: dict,
    *,
    pdf_doi_record: dict | None,
    cr_record: dict | None,
    url_ok=None,
) -> dict[str, str]:
    fields: dict[str, str] = {}

    # author
    if entry.get("authors"):
        fields["author"] = authors_to_bib(entry["authors"])

    # year
    if entry.get("year"):
        fields["year"] = entry["year"]

    # title
    if entry.get("title"):
        fields["title"] = entry["title"]

    # journal/source
    journal = entry.get("journal")
    if cr_record and cr_record.get("container_title"):
        # Trust Crossref's journal name when we have a Crossref hit
        journal = cr_record["container_title"]
    if journal:
        fields["journal"] = journal

    # volume / number / pages — prefer Crossref values if present
    volume = entry.get("volume")
    issue = entry.get("number")
    pages = make_pages(entry.get("pages"))
    if cr_record:
        volume = cr_record.get("volume") or volume
        issue = cr_record.get("issue") or issue
        cr_pages = cr_record.get("page")
        if cr_pages:
            pages = cr_pages.replace("-", "--")
    if volume:
        fields["volume"] = str(volume)
    if issue:
        fields["number"] = str(issue)
    if pages:
        fields["pages"] = pages

    # DOI selection
    pdf_doi = pdf_doi_record.get("doi") if pdf_doi_record else None
    pdf_title = pdf_doi_record.get("title") if pdf_doi_record else None
    cr_doi = cr_record.get("doi") if cr_record else None
    cr_score = cr_record.get("score") if cr_record else None
    doi, source = pick_doi(
        pdf_doi=pdf_doi,
        pdf_title=pdf_title,
        cr_doi=cr_doi,
        cr_score=cr_score,
        entry_title=entry.get("title"),
    )
    if doi:
        fields["doi"] = doi
    if source.endswith("mismatch"):
        log.warning(
            "DOI mismatch %s: pdf=%s crossref=%s — using %s",
            entry["raw"][:60],
            pdf_doi,
            cr_doi,
            source.split("-")[0],
        )

    # BHL identifiers — derive from whichever DOI is BHL-shaped
    bhl_part = derive_bhl_part(pdf_doi) or derive_bhl_part(cr_doi) or derive_bhl_part(doi)
    bhl_title = derive_bhl_title(pdf_doi) or derive_bhl_title(cr_doi) or derive_bhl_title(doi)
    if bhl_part:
        fields["bhl_part"] = bhl_part
    if bhl_title:
        fields["bhl_title"] = bhl_title

    # URL — apply fallback chain, dropping any URL the verifier flagged as bad.
    candidates: list[str] = []
    if doi:
        candidates.append(f"https://doi.org/{doi}")
    if bhl_part:
        candidates.append(f"https://www.biodiversitylibrary.org/part/{bhl_part}")
    if bhl_title:
        candidates.append(f"https://www.biodiversitylibrary.org/bibliography/{bhl_title}")
    if cr_record and cr_record.get("url"):
        candidates.append(cr_record["url"])
    chosen_url = None
    for u in candidates:
        if url_ok is None or url_ok(u):
            chosen_url = u
            break
    if chosen_url:
        fields["url"] = chosen_url

    # File pointer (basename only — directory layout may move)
    if entry.get("pdf_basename"):
        fields["file"] = entry["pdf_basename"]

    # Note: anything that didn't fit (Pugh's [In Library] etc.). Skip for now.
    return fields


def main() -> None:
    entries = json.loads((BUILD / "match.json").read_text())
    pdf_dois = json.loads((BUILD / "dois.json").read_text()) if (BUILD / "dois.json").exists() else {}
    cr = (
        json.loads((BUILD / "crossref_dois.json").read_text())
        if (BUILD / "crossref_dois.json").exists()
        else {}
    )
    url_status: dict[str, dict] = (
        json.loads((BUILD / "url_status.json").read_text())
        if (BUILD / "url_status.json").exists()
        else {}
    )

    def url_ok(u: str | None) -> bool:
        """Treat unverified URLs as OK (we'll fall back gracefully); only drop
        URLs we've explicitly checked and found bad."""
        if not u:
            return False
        rec = url_status.get(u)
        if rec is None:
            return True
        return bool(rec.get("ok"))

    used_keys: Counter = Counter()
    pdf_no_doi: list[str] = []
    no_doi_modern: list[str] = []  # post-1997 entries with no DOI
    mismatches = 0

    with OUT_BIB.open("w") as out:
        out.write(
            "% siphonophores.bib — generated by scripts/build_bib.py\n"
            "% Source: AASCANNED LITERATURE.docx (curated by P.R. Pugh) plus library/ PDFs.\n"
            "% Do not edit by hand — re-run `python scripts/build_bib.py` instead.\n\n"
        )
        for idx, entry in enumerate(entries):
            pdf_basename = entry.get("pdf_basename")
            pdf_doi_record = pdf_dois.get(pdf_basename) if pdf_basename else None
            cr_record = cr.get(str(idx)) if cr else None

            # Re-validate Crossref hit using a stricter token-sort title match.
            # The original lookup used token_set, which can over-match when one
            # title is a substring of another (e.g. "Acalephs of the Fiji Islands"
            # vs "The islands and coral reefs of the Fiji Group").
            if cr_record and cr_record.get("title") and entry.get("title"):
                from rapidfuzz import fuzz as _fuzz

                strict = _fuzz.token_sort_ratio(
                    entry["title"].lower(), cr_record["title"].lower()
                )
                if strict < 80:
                    log.info(
                        "rejecting crossref hit (strict title=%d) %r vs %r",
                        strict,
                        entry["title"][:60],
                        cr_record["title"][:60],
                    )
                    cr_record = None

            fields = make_bib_fields(
                entry,
                pdf_doi_record=pdf_doi_record,
                cr_record=cr_record,
                url_ok=url_ok,
            )

            # Disambiguate duplicate keys
            base_key = make_key(entry)
            n = used_keys[base_key]
            key = base_key if n == 0 else f"{base_key}_{n}"
            used_keys[base_key] += 1

            emit_record(out, key=key, fields=fields)

            # Bookkeeping for logs
            try:
                yr = int(entry["year"]) if entry.get("year") else 0
            except ValueError:
                yr = 0
            if pdf_basename and pdf_doi_record and not pdf_doi_record.get("doi"):
                pdf_no_doi.append(f"{pdf_basename}\t(year={yr})")
            if not fields.get("doi") and yr >= 1997:
                no_doi_modern.append(f"{entry['raw'][:160]}")

    log.info("wrote %s with %d records", OUT_BIB, len(entries))

    # Final orphan/issue logs (overwrite each run)
    with (LOGS / "no_doi_post_1997.log").open("w") as fh:
        fh.write(f"# Post-1997 entries that ended up with no DOI ({len(no_doi_modern)})\n")
        for line in no_doi_modern:
            fh.write(line + "\n")

    with (LOGS / "pdf_text_no_doi.log").open("w") as fh:
        fh.write(f"# Library PDFs (post-1997) where extract_dois.py could not find a DOI in text ({len(pdf_no_doi)})\n")
        for line in pdf_no_doi:
            fh.write(line + "\n")

    log.info("logs/no_doi_post_1997.log: %d entries", len(no_doi_modern))
    log.info("logs/pdf_text_no_doi.log: %d pdfs", len(pdf_no_doi))


if __name__ == "__main__":
    main()
