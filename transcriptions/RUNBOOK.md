# Runbook: continuing the gold-standard transcription

> **STATUS 2026-08-24: PASS 1 IS COMPLETE — 761/761 pages.** The per-job loop
> below is finished and no transcription agent should be launched. What remains
> is "Remaining phases after pass 1" further down; those are a different kind of
> work and were not started. The loop is kept because phase 2 (the targeted
> second pass) reuses it, and because the failure modes recorded here are the
> reason the pass survived a reboot, a session-limit kill and five content-filter
> blocks without losing a page.
>
> Two lessons worth reading before phase 2:
> - **A page can be truncated yet pass every mechanical check.** Mańko p9 was
>   4,986 bytes with a valid header and was missing both prose columns. See
>   "After a machine reboot" below.
> - **Mańko_et al2020 must be transcribed one page per agent.** Multi-page jobs
>   blocked 4/4 on the output content filter; single-page jobs succeeded 4/4.

The transcription runs as a **serial chain of one agent at a time**, across many
sessions, because session token limits interrupt it repeatedly. Nothing is held
in memory between cycles — the queue is derived entirely from which page files
exist on disk, so a fresh session can pick this up cold.

## The loop

1. **Arm the wake mechanism FIRST, before launching the agent** — and use
   `CronCreate`, not a background `sleep`.

   ```
   CronCreate  cron: "23,53 * * * *"  recurring: true
   ```

   Two separate reasons for this, both learned the hard way:

   - **Order.** If the wake is armed *after* the agent, then when the agent
     exhausts the session budget there may be no budget left to arm it, and the
     chain stops silently until a human notices.
   - **Mechanism.** A background `sleep` task's completion notification does
     **not** wake an idle session. Observed 2026-08-07: a heartbeat fired at
     07:32 and its notification was not delivered until 11:32, when the user
     next spoke. Cron jobs, by contrast, fire while the REPL is idle, which is
     exactly the state the chain sits in between jobs.

   Also size the interval against the *reset*, not by feel. A 70-minute
   heartbeat against a five-hour reset window wakes uselessly and then, being
   one-shot, is gone before the reset arrives. A ~30-minute recurring cron
   costs almost nothing per fire and cannot miss the window.

   Caveat: cron jobs here are session-only (in memory, gone when Claude exits)
   and auto-expire after 7 days. If the session ends, re-create the job.

2. **Compute the next job.**

   ```bash
   cd transcriptions
   python3 scripts/next_job.py --budget 25
   ```

   Emits the next ~25 pages, hardest documents first, with a recommended model
   per document.

3. **Launch exactly one agent** with the model the planner names, giving it the
   page list and telling it to read `TRANSCRIPTION_PROTOCOL.md`. Tell it
   explicitly **not to spawn subagents** — that breaks the serial guarantee.

4. **On any wake** (agent completed, agent died, or heartbeat fired): check
   `python3 scripts/status.py`. If no agent is in flight and pages remain,
   go to step 1.

## Model policy

**Opus only.** Set in `OPUS_ONLY` in `scripts/next_job.py`; the full
reasoning is in the `MODEL POLICY` comment at the top of that file. Launch
whatever model the planner names and do not substitute a cheaper tier.

Sonnet was trialled on job 22 and failed on the one rule a gold standard cannot
bend: it **invented content** — 26 `[FIGURE]` blocks across 8 pages carried
English descriptions of what the figure depicts, none of it printed on the page.
The 13 Opus pages of the *same document* under the *same protocol* invented
nothing. Those 8 pages were deleted and re-queued.

The earlier policy routed by whether the poppler cross-check could catch an
error, and sent clean roman-type documents — above all **Totton1965a** — to
Sonnet. That rested on a false premise: fluent invented prose does not diff as
an error against a text layer that simply lacks it. It reads as the transcriber
having *recovered* something the OCR missed, which is exactly what we want on
hard pages. The failure mode is invisible to the verification design, so the
tier cannot be used. Do not re-enable it without a check that specifically
detects invention.

Cross-check availability still matters for the *second* pass: only three
documents — Adanson1757, Hjortberg1769, LoBianco1909 — have no cross-check at
all (1 char/page), so nothing can triage them.

## Mańko_et al2020 — one page per agent (was "Deferred document")

**Complete, 17/17.** `DEFERRED` in the planner is now empty; this document is no
longer excluded and the planner's output can be trusted as-is.

It is recorded here because the constraint still applies to phase 2. This document
hit `API Error: 400 Output blocked by content filtering policy` **five times**, and
the pattern is not about page content:

| job shape | record |
|---|---|
| multi-page | **0 for 4** — blocked every time |
| single-page | **4 for 4** — never blocked |

Pages 1 and 14 each killed a batch job and then transcribed cleanly on their own.
The correlation is with how much output an agent has already emitted, not with
which page it is reading. **So: transcribe this document one page per agent.**

Two traps this left behind, both worth knowing before re-reading any of its pages:

- `DEFERRED` **reordered rather than excluded**, so the planner kept offering the
  problem page as budget filler at the tail of large jobs — risking 25 pages to
  test one. That is why the prescribed isolated retry went unrun for twelve days.
- Never conclude a page is poisoned from a batch failure. **Retry it alone first.**

See `SECOND_OPINION_QUEUE.md` for the full history.

## Why per-page writes matter

The protocol requires writing each page the moment it is finished. Four waves
have now been interrupted mid-job and **none lost completed work**. Do not
relax this rule for any reason.

## Remaining phases after pass 1

1. **Cross-check**: diff each `page_NNN.txt` against
   `_provenance/poppler/page_NNN.txt`, record a per-page similarity in
   `confidence.json`.
2. **Targeted second pass**: re-transcribe (Opus) any page where the diff
   disagrees materially *and* the poppler layer is known-decent, plus every page
   with no cross-check signal.
3. **Concatenate** per document to `<stem>.txt` with `[PAGE n]`
   markers, and write `manifest.json` with page counts and checksums.
4. **README.md** at `` recording provenance, method, tool versions
   (poppler 24.02.0), the independence guarantee, and known gaps.

## After a machine reboot

Three things do not survive, in order of importance:

1. **The cron job is session-only** — it lives in Claude's memory, not on disk, so
   it dies with the process and must be re-created (see The loop above). This is
   the usual reason progress silently stops.
2. **`/tmp` is emptied at boot.** `/usr/lib/tmpfiles.d/tmp.conf` carries
   `D /tmp 1777 root root 30d`, and the `D` directive removes directory contents
   at boot — disk-backed does not mean persistent. The v1.0.0 fidelity audit was
   copied out to a durable location for this reason (it lives outside this repo,
   in the working area the transcription was built in); anything else left in
   `/tmp` is gone. Rendered page PNGs are regenerable and were not preserved.
3. **An in-flight transcription may leave one truncated page.** Pages are written
   as they finish, so at most the page being typed at shutdown is affected — but a
   partially-written file is non-empty, and `status.py` counts non-empty as
   done. **Run this integrity check after any unclean stop:**

   ```bash
   cd transcriptions
   # page files suspiciously short, or lacking the mandatory [PAGE n] first line
   find . -name 'page_*.txt' -not -path '*_provenance*' -size -200c \
        -printf '%p %s\n' | sort
   find . -name 'page_*.txt' -not -path '*_provenance*' -print0 |
   while IFS= read -r -d '' f; do
       head -1 "$f" | grep -q '^\[PAGE' || echo "NO [PAGE] HEADER: $f"
       head -1 "$f" | grep -qE '^\[PAGE (unnumbered|n:)' && echo "MALFORMED HEADER: $f"
   done
   ```

   **The `-print0`/`read -d ''` form is required, not stylistic.** The earlier
   `for f in $(find ...)` version word-split on the space in `Mańko_et al2020`
   and so **never checked that document at all** — every one of its pages was
   reported as a path-not-found error that scrolled past as noise. That is the
   one document in the corpus with a space in its name, and it was also the one
   carrying a truncated page.

   These two checks are necessary but **not sufficient**, and the gap is the
   dangerous part. A page killed mid-transcription can still be well over 200
   bytes and still carry a valid header — page 9 of Mańko was 4,986 bytes with a
   correct `[PAGE 9]` line, and was missing both of its prose columns. Nothing
   mechanical caught it; it was found by rendering the page and comparing.
   **After any agent dies mid-job, render the page it died on and look at it.**
   The agent's last message names that page.

   A genuinely blank leaf is legitimately short (`[BLANK PAGE]`), and plate pages
   with no lettering are short too, so review rather than delete blindly. Delete
   any file that is truncated mid-word or missing its header; the planner will
   re-queue it automatically.
