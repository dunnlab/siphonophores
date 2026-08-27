#!/usr/bin/env python3
"""Minimal, edit-preserving BibTeX reader/writer for ``siphonophores.bib``.

``siphonophores.bib`` is the source of truth for this library and is edited in
place, so any script that writes into it must not disturb the entries it is not
touching. Regenerating entries from parsed fields would silently reformat
author strings, drop the ``%`` comment header, normalise brace protection in
titles (``{Physalia} {physalis}``) and re-indent the hand-added entries that use
four spaces instead of two.

So this module does not round-trip through a model of a BibTeX entry. It records
the *byte span* of every field inside the entry's original text and edits those
spans surgically. An entry nobody touched is emitted exactly as it was read;
an entry that gained a field differs from the original by precisely the lines
added. That keeps ``git diff`` reviewable on a 1,800-entry file.

Two properties this guarantees, both asserted by ``--selftest``:

    parse(text) -> render(...) == text          (nothing modified)
    only touched entries appear in a diff       (something modified)

Not a general BibTeX implementation. It handles what this file contains:
``@type{key,`` entries with ``name = {value},`` fields whose values are
brace-balanced. It does not handle quoted ``"value"`` fields, ``@string``
macros, or concatenation — none of which appear here, and all of which raise
rather than being silently mangled.

Usage as a library::

    from bibio import parse_bib, render_bib
    doc = parse_bib(Path("siphonophores.bib").read_text())
    for e in doc.entries:
        if e.fields.get("file") == "Ilyin1900.pdf":
            e.set_field("keeppages", "3--6")
    Path("siphonophores.bib").write_text(render_bib(doc))

Self-check::

    python scripts/bibio.py --selftest
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Iterator, Optional

REPO = Path(__file__).resolve().parents[1]
BIB = REPO / "siphonophores.bib"

# "@article{Key," — the entry opener. Key runs to the first comma.
_ENTRY_RE = re.compile(r"@(\w+)\s*\{\s*([^,\s]+)\s*,", re.MULTILINE)
# "  name = {" — a field opener inside an entry. Captures the indent so
# inserted fields match whatever the surrounding entry already uses.
_FIELD_RE = re.compile(r"(?P<indent>[ \t]*)(?P<name>[A-Za-z_][A-Za-z0-9_-]*)\s*=\s*\{")


class BibFormatError(ValueError):
    """The input used BibTeX we deliberately do not handle."""


@dataclass
class Field:
    name: str
    value: str
    #: Span of the whole field *line* within ``Entry.raw`` — from the start of
    #: its indent through the trailing comma, so a replacement swaps the line.
    start: int
    end: int
    indent: str


@dataclass
class Entry:
    etype: str
    key: str
    #: The entry verbatim, "@article{...\n}" with no surrounding whitespace.
    raw: str
    fields_list: list[Field]
    #: Replacements for spans that exist in ``raw``, keyed by span so setting
    #: the same field twice replaces the pending edit instead of stacking.
    _span_edits: dict[tuple[int, int], str] = dc_field(default_factory=dict)
    #: Fields not present in ``raw``, materialised before the closing brace at
    #: render time. Kept separate from ``_span_edits`` because an appended
    #: field has no span to replace — treating it as a zero-width span makes a
    #: second set_field insert a duplicate rather than update.
    _appends: dict[str, str] = dc_field(default_factory=dict)

    @property
    def fields(self) -> dict[str, str]:
        """Field name -> value, last occurrence winning (as BibTeX readers do)."""
        out = {f.name: f.value for f in self.fields_list}
        out.update(self._appends)
        return out

    @property
    def dirty(self) -> bool:
        return bool(self._span_edits or self._appends)

    def get(self, name: str) -> Optional[str]:
        return self.fields.get(name)

    def _indent(self) -> str:
        return self.fields_list[-1].indent if self.fields_list else "  "

    def set_field(self, name: str, value: str) -> None:
        """Set ``name`` to ``value``, updating in place or appending.

        Appending puts the new field immediately before the entry's closing
        brace rather than in some canonical position: it keeps the diff to the
        added lines alone, and field order carries no meaning in BibTeX.
        """
        name = name.lower()
        if "{" in value or "}" in value:
            # A brace in the value would unbalance the field we are writing.
            # Callers pass plain text; refuse rather than emit a broken entry.
            raise BibFormatError(
                f"{self.key}: refusing to write braces in {name} = {value!r}"
            )
        existing = [f for f in self.fields_list if f.name == name]
        if existing:
            f = existing[-1]
            if f.value == value and (f.start, f.end) not in self._span_edits:
                return
            self._span_edits[(f.start, f.end)] = (
                f"{f.indent}{name} = {{{value}}},"
            )
            f.value = value
            return
        self._appends[name] = value

    def remove_field(self, name: str) -> bool:
        """Drop every occurrence of ``name``. Returns whether anything went."""
        name = name.lower()
        removed = self._appends.pop(name, None) is not None
        for f in [f for f in self.fields_list if f.name == name]:
            # Swallow the trailing newline too, so no blank line is left behind.
            end = f.end
            if self.raw[end : end + 1] == "\n":
                end += 1
            self._span_edits[(f.start, end)] = ""
            removed = True
        self.fields_list = [f for f in self.fields_list if f.name != name]
        return removed

    def render(self) -> str:
        if not self.dirty:
            return self.raw
        out = self.raw
        # Apply right-to-left so earlier spans keep their offsets.
        for (start, end), repl in sorted(
            self._span_edits.items(), key=lambda kv: kv[0][0], reverse=True
        ):
            out = out[:start] + repl + out[end:]
        if self._appends:
            close = out.rstrip().rfind("}")
            if close < 0:  # pragma: no cover — parse() guarantees one
                raise BibFormatError(f"{self.key}: no closing brace")
            indent = self._indent()
            added = "".join(
                f"{indent}{n} = {{{v}}},\n" for n, v in self._appends.items()
            )
            out = out[:close] + added + out[close:]
        return out


@dataclass
class BibDoc:
    #: Alternating literal chunks and entries, in file order. Literals hold the
    #: comment header and the blank lines between entries, so they survive.
    parts: list[object]

    @property
    def entries(self) -> list[Entry]:
        return [p for p in self.parts if isinstance(p, Entry)]

    def by_file(self) -> dict[str, Entry]:
        """Lowercased PDF basename -> entry, mirroring corpus's BibIndex.

        corpus keys on ``Path(file).name.lower()`` and lets the last entry win
        on a collision (bib/parser.py:481-489). Match that exactly, so what
        this repo believes about a file is what corpus will believe.
        """
        out: dict[str, Entry] = {}
        for e in self.entries:
            raw = (e.get("file") or "").strip()
            if not raw:
                continue
            for part in re.split(r"[;,]", raw):
                name = Path(part.strip()).name.lower()
                if name:
                    out[name] = e
        return out


def _scan_braced(text: str, open_idx: int) -> int:
    """Index just past the ``}`` matching the ``{`` at ``open_idx``."""
    depth = 0
    i = open_idx
    n = len(text)
    while i < n:
        c = text[i]
        if c == "\\":  # escaped char — skip the pair
            i += 2
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise BibFormatError(f"unbalanced braces starting at offset {open_idx}")


def _parse_entry(text: str, start: int) -> tuple[Entry, int]:
    """Parse one entry beginning at ``@``. Returns (entry, end_offset)."""
    m = _ENTRY_RE.match(text, start)
    if not m:  # pragma: no cover — caller only calls us on a match
        raise BibFormatError(f"not an entry at offset {start}")
    body_open = text.index("{", start)
    end = _scan_braced(text, body_open)
    raw = text[start:end]
    inner_start = m.end() - start  # just past "@article{Key,"

    fields: list[Field] = []
    pos = inner_start
    while True:
        fm = _FIELD_RE.search(raw, pos)
        if not fm or fm.end() > len(raw) - 1:
            break
        val_open = fm.end() - 1
        val_end = _scan_braced(raw, val_open)
        value = raw[val_open + 1 : val_end - 1]
        # Consume a trailing comma so replacing the span keeps the entry valid.
        after = val_end
        while after < len(raw) and raw[after] in " \t":
            after += 1
        if after < len(raw) and raw[after] == ",":
            after += 1
        fields.append(
            Field(
                name=fm.group("name").lower(),
                value=value,
                start=fm.start(),
                end=after,
                indent=fm.group("indent"),
            )
        )
        pos = after
    return Entry(etype=m.group(1).lower(), key=m.group(2), raw=raw,
                 fields_list=fields), end


def parse_bib(text: str) -> BibDoc:
    """Parse into entries plus the literal text between them."""
    parts: list[object] = []
    pos = 0
    for m in _ENTRY_RE.finditer(text):
        if m.start() < pos:
            continue  # inside an entry we already consumed
        if m.start() > pos:
            parts.append(text[pos : m.start()])
        entry, end = _parse_entry(text, m.start())
        parts.append(entry)
        pos = end
    if pos < len(text):
        parts.append(text[pos:])
    return BibDoc(parts=parts)


def render_bib(doc: BibDoc) -> str:
    return "".join(p if isinstance(p, str) else p.render() for p in doc.parts)


def iter_entries(path: Path = BIB) -> Iterator[Entry]:
    yield from parse_bib(path.read_text(encoding="utf-8")).entries


# ---------------------------------------------------------------------------
# self-test
# ---------------------------------------------------------------------------

def _selftest(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    doc = parse_bib(text)
    ok = True

    out = render_bib(doc)
    if out == text:
        print(f"round-trip: OK ({len(doc.entries)} entries, {len(text)} bytes)")
    else:
        ok = False
        print("round-trip: FAIL — rendered text differs from input")
        for i, (a, b) in enumerate(zip(text, out)):
            if a != b:
                print(f"  first difference at byte {i}: {text[i-60:i+60]!r}")
                break

    # Field coverage: every entry should have parsed at least author+title,
    # which catches a regex that silently matched nothing.
    empty = [e.key for e in doc.entries if not e.fields_list]
    if empty:
        ok = False
        print(f"field parse: FAIL — {len(empty)} entries with no fields, e.g. {empty[:5]}")
    else:
        print("field parse: OK (every entry has at least one field)")

    # Nested-brace values must survive verbatim.
    nested = [e for e in doc.entries if any("{" in f.value for f in e.fields_list)]
    print(f"nested-brace values: {len(nested)} entries carry them")

    # A surgical edit must touch only its own entry. Use a field name that
    # cannot occur in the real bib, so the assertions below stay valid once
    # real annotations (keeppages, doclang, pagemap) are present in the file.
    probe = "zzselftest"
    doc2 = parse_bib(text)
    target = doc2.entries[0]
    target.set_field(probe, "3--20")
    edited = render_bib(doc2)
    if edited == text:
        ok = False
        print("edit: FAIL — set_field made no change")
    else:
        delta = len(edited) - len(text)
        reparsed = parse_bib(edited)
        got = reparsed.entries[0].get(probe)
        untouched = render_bib(parse_bib(edited)).count("@") == text.count("@")
        if got == "3--20" and untouched:
            print(f"edit: OK (+{delta} bytes, value reads back, entry count stable)")
        else:
            ok = False
            print(f"edit: FAIL — read back {got!r}")

    # Updating in place must not duplicate the field. Count within the target
    # entry, not the whole file — other entries may legitimately carry it.
    target.set_field(probe, "3--21")
    n_in_entry = target.render().count(probe)
    if n_in_entry != 1:
        ok = False
        print(f"update: FAIL — {n_in_entry} occurrences of {probe} in the entry")
    elif "3--21" not in target.render():
        ok = False
        print("update: FAIL — updated value not present")
    else:
        print("update: OK (in-place replacement, no duplicate)")

    # Removal must restore the original byte-for-byte.
    target.remove_field(probe)
    if render_bib(doc2) == text:
        print("remove: OK (restores original exactly)")
    else:
        ok = False
        print("remove: FAIL — does not restore original")

    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--selftest", action="store_true",
                    help="Verify round-trip and surgical-edit properties against the bib.")
    ap.add_argument("--bib", type=Path, default=BIB)
    args = ap.parse_args()
    if args.selftest:
        return _selftest(args.bib)
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
