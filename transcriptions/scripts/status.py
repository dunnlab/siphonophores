#!/usr/bin/env python3
"""Report progress of the gold-standard transcription, and emit work queues.

The transcription runs across many sittings and many agents, so the queue has
to be derivable from what is on disk rather than held in anyone's head. A page
is DONE when `<stem>/page_NNN.txt` exists and is non-empty.

Usage:
    status.py                 # summary table
    status.py --todo          # every outstanding page, one per line
    status.py --todo <stem>   # outstanding pages for one document
    status.py --claim N       # next N outstanding pages (for one agent)
"""
import argparse
import json
import pathlib

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
CLASS = DATA / "page_class.json"


def load_class():
    return json.loads(CLASS.read_text())


def state():
    cls = load_class()
    rows = []
    for name in sorted(cls, key=str.lower):
        stem = name[:-4] if name.endswith(".pdf") else name
        n = cls[name]["pages"]
        d = ROOT / stem
        done, todo = [], []
        for i in range(1, n + 1):
            f = d / f"page_{i:03d}.txt"
            (done if (f.exists() and f.stat().st_size > 0) else todo).append(i)
        rows.append((stem, n, done, todo))
    return rows


def fmt_ranges(nums):
    if not nums:
        return ""
    out, start, prev = [], nums[0], nums[0]
    for x in nums[1:]:
        if x == prev + 1:
            prev = x
            continue
        out.append(f"{start}" if start == prev else f"{start}-{prev}")
        start = prev = x
    out.append(f"{start}" if start == prev else f"{start}-{prev}")
    return ",".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--todo", nargs="?", const="__all__", default=None)
    ap.add_argument("--claim", type=int, default=None)
    a = ap.parse_args()
    rows = state()

    if a.claim:
        n = 0
        for stem, total, done, todo in rows:
            for p in todo:
                print(f"{stem} {p}")
                n += 1
                if n >= a.claim:
                    return
        return

    if a.todo:
        for stem, total, done, todo in rows:
            if a.todo != "__all__" and stem != a.todo:
                continue
            if todo:
                print(f"{stem}\t{fmt_ranges(todo)}")
        return

    tp = td = 0
    print(f"{'document':30}{'pages':>7}{'done':>7}{'todo':>7}  outstanding")
    print("-" * 88)
    for stem, total, done, todo in rows:
        tp += total
        td += len(done)
        mark = "" if todo else "  COMPLETE"
        print(f"{stem[:29]:30}{total:>7}{len(done):>7}{len(todo):>7}  "
              f"{fmt_ranges(todo)[:28]}{mark}")
    print("-" * 88)
    pct = 100 * td / tp if tp else 0
    print(f"{'TOTAL':30}{tp:>7}{td:>7}{tp-td:>7}  {pct:.1f}% complete")


if __name__ == "__main__":
    main()
