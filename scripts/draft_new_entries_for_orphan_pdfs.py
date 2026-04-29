#!/usr/bin/env python3
"""Draft new ``@article`` entries for orphan PDFs whose verified DOI is
not already in the bib. Reads ``build/dois.json`` for the verified DOI &
title, fetches authors/volume/page from Crossref, and emits well-formed
records into ``siphonophores.bib``.

Citation key strategy:
    First-author surname (folded) + "Etal" if 3+ authors + 4-digit year.
    Disambiguate against existing bib keys by appending ``b``, ``c``, ….

Output: appended bib entries, log of additions in
``logs/drafted_new_entries.log``. Pass ``--dry-run`` to inspect first.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
import unicodedata
from pathlib import Path

import bibtexparser
import requests

REPO = Path(__file__).resolve().parents[1]
LIBRARY = REPO / "library"
BUILD = REPO / "build"
LOGS = REPO / "logs"
BIB = REPO / "siphonophores.bib"
DOIS_CACHE = BUILD / "dois.json"
OUT = LOGS / "drafted_new_entries.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("draft_new_entries")

USER_AGENT = "siphonophores-bib (mailto:caseywdunn@gmail.com)"
CROSSREF_WORK = "https://api.crossref.org/works/{doi}"

URLY = {"doi", "url", "file"}


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z0-9]+", "", s)


def surname_only(author: str) -> str:
    """Pull just the family-name portion of a "Surname, Given" string."""
    if "," in author:
        return author.split(",", 1)[0].strip()
    return author.strip()


def make_key(authors: list[str], year: int, *, taken: set[str]) -> str:
    if not authors:
        base = "Anon" + str(year)
    else:
        first = fold(surname_only(authors[0])) or "Anon"
        if len(authors) >= 3:
            base = f"{first}Etal{year}"
        elif len(authors) == 2:
            second = fold(surname_only(authors[1]))
            base = f"{first}{second}{year}"
        else:
            base = f"{first}{year}"
    if base not in taken:
        return base
    for letter in "bcdefghij":
        candidate = base + letter
        if candidate not in taken:
            return candidate
    raise RuntimeError(f"could not find free key for base {base}")


def escape_bibtex(value: str, *, is_url_or_id: bool = False) -> str:
    if value is None:
        return ""
    s = str(value)
    if is_url_or_id:
        return s
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("\\", "\\textbackslash{}")
    open_count = s.count("{")
    close_count = s.count("}")
    if open_count != close_count:
        out_chars = []
        depth = 0
        for ch in s:
            if ch == "{":
                depth += 1; out_chars.append(ch)
            elif ch == "}":
                if depth > 0:
                    depth -= 1; out_chars.append(ch)
                else:
                    out_chars.append("]")
            else:
                out_chars.append(ch)
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
    s = s.replace("&", "\\&").replace("%", "\\%").replace("$", "\\$")
    s = s.replace("#", "\\#").replace("~", "\\textasciitilde{}").replace("^", "\\textasciicircum{}")
    return s


def emit_record(key: str, fields: dict[str, str]) -> str:
    lines = [f"@article{{{key},"]
    keys = ["author", "year", "title", "journal", "volume", "number", "pages",
            "doi", "url", "file", "note"]
    seen = set()
    for k in keys:
        if k in fields and fields[k]:
            v = escape_bibtex(fields[k], is_url_or_id=k in URLY)
            lines.append(f"  {k} = {{{v}}},")
            seen.add(k)
    for k, v in fields.items():
        if v and k not in seen:
            v = escape_bibtex(v, is_url_or_id=k in URLY)
            lines.append(f"  {k} = {{{v}}},")
    lines.append("}")
    return "\n".join(lines) + "\n"


def author_to_bib(a: dict) -> str:
    family = (a.get("family") or "").strip()
    given = (a.get("given") or "").strip()
    if not family and not given:
        name = (a.get("name") or "").strip()
        return name
    if family and given:
        return f"{family}, {given}"
    return family or given


def fetch_crossref(doi: str, session: requests.Session) -> dict | None:
    try:
        r = session.get(
            CROSSREF_WORK.format(doi=requests.utils.quote(doi, safe="")),
            timeout=15,
            headers={"User-Agent": USER_AGENT},
        )
    except requests.RequestException as exc:
        log.warning("network error for %s: %s", doi, exc)
        return None
    if r.status_code != 200:
        return None
    try:
        return r.json().get("message")
    except ValueError:
        return None


def collect_orphan_pdfs(entries: list[dict]) -> list[Path]:
    referenced = {(e.get("file") or "").strip() for e in entries if (e.get("file") or "").strip()}
    out = []
    for p in LIBRARY.rglob("*.pdf"):
        rel = p.relative_to(LIBRARY)
        if rel.parts and rel.parts[0] == "orphans":
            continue
        if p.name in referenced:
            continue
        out.append(p)
    return sorted(out, key=lambda p: p.name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.1)
    args = ap.parse_args()

    with BIB.open() as f:
        db = bibtexparser.load(f)
    bib_dois = {(e.get("doi") or "").strip().lower() for e in db.entries if (e.get("doi") or "").strip()}
    taken_keys = {e["ID"] for e in db.entries}

    cache = json.loads(DOIS_CACHE.read_text()) if DOIS_CACHE.exists() else {}

    orphans = collect_orphan_pdfs(db.entries)
    log.info("orphan PDFs: %d", len(orphans))

    session = requests.Session()
    drafted: list[tuple[str, str, dict]] = []  # (basename, key, fields)
    skipped: list[tuple[str, str]] = []

    items = orphans[: args.limit] if args.limit else orphans
    for n, pdf in enumerate(items, 1):
        rec = cache.get(pdf.name) or {}
        doi = (rec.get("doi") or "").lower()
        if not doi or not rec.get("verified"):
            skipped.append((pdf.name, "no verified DOI"))
            continue
        if doi in bib_dois:
            skipped.append((pdf.name, f"already in bib ({doi})"))
            continue

        msg = fetch_crossref(doi, session=session)
        if msg is None:
            skipped.append((pdf.name, "crossref lookup failed"))
            continue

        # Build the record.
        authors = [author_to_bib(a) for a in (msg.get("author") or []) if author_to_bib(a)]
        year = (msg.get("issued", {}) or {}).get("date-parts", [[None]])[0][0]
        try:
            year_int = int(year) if year else None
        except (TypeError, ValueError):
            year_int = None
        if not year_int:
            skipped.append((pdf.name, "no year from Crossref"))
            continue

        key = make_key(authors, year_int, taken=taken_keys)
        taken_keys.add(key)
        bib_dois.add(doi)

        fields: dict[str, str] = {}
        if authors:
            fields["author"] = " and ".join(authors)
        fields["year"] = str(year_int)
        title = (msg.get("title") or [None])[0]
        if title:
            fields["title"] = title
        container = (msg.get("container-title") or [None])[0]
        if container:
            fields["journal"] = container
        if msg.get("volume"):
            fields["volume"] = str(msg["volume"])
        if msg.get("issue"):
            fields["number"] = str(msg["issue"])
        page = msg.get("page")
        if page:
            fields["pages"] = page.replace("-", "--")
        fields["doi"] = doi
        fields["url"] = msg.get("URL") or f"https://doi.org/{doi}"
        fields["file"] = pdf.name

        drafted.append((pdf.name, key, fields))
        time.sleep(args.sleep)

    # ---------- Write ----------
    OUT.parent.mkdir(exist_ok=True)
    out_lines = [f"# drafted_new_entries.log {'(dry-run)' if args.dry_run else ''}",
                 f"# orphan PDFs scanned: {len(items)}",
                 f"# drafted: {len(drafted)}, skipped: {len(skipped)}",
                 ""]
    out_lines.append(f"\n=== DRAFTED ({len(drafted)}) ===\n")
    blocks: list[str] = []
    for basename, key, fields in drafted:
        block = emit_record(key, fields)
        blocks.append(block)
        out_lines.append(f"# {basename}")
        out_lines.append(block)
    out_lines.append(f"\n=== SKIPPED ({len(skipped)}) ===\n")
    for basename, reason in skipped:
        out_lines.append(f"  {basename:50s} {reason}")
    OUT.write_text("\n".join(out_lines) + "\n")
    log.info("wrote %s", OUT)

    if not args.dry_run and drafted:
        new_text = "\n" + "\n".join(blocks).rstrip("\n") + "\n"
        with BIB.open("a") as f:
            f.write(new_text)
        log.info("appended %d new bib entries", len(drafted))


if __name__ == "__main__":
    main()
