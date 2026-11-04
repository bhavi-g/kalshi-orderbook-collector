# Postmortem: silent output-channel data loss

**Reported symptom:** "market syncing bugs that prevented it from capturing
properly" (external report, no logs or repro steps provided).

**Status:** Root-caused, fixed, and re-verified live. Two distinct bugs were
found and fixed; both match the reported symptom class.

## What was wrong

### Bug 1 — silent message loss under backpressure (the primary one)

`collector.rs` sent every outgoing `SnapshotDomain`/`PriceLevelUpdateDomain`
to the output writer with `mpsc::Sender::try_send`, a **non-blocking** call
that fails immediately if the channel is full. The failure was discarded:

```rust
let _ = self.out.try_send(OutMsg::Delta(upd));
```

If the single writer thread ever fell behind the combined production rate of
all subscribed markets — which happens at this project's own documented
default (`MAX_MARKETS=0`, i.e. subscribe to every regular market: ~10,000 on
Kalshi) — the output channel fills, `try_send` starts failing, and the
message (a real price-level change the in-memory book had already applied)
is silently dropped. No error, no log line, no crash. The book stays
internally consistent; only the **captured output stream** — the thing a
reviewer actually judges — quietly loses data. This is the opposite of the
project's own stated #1 priority (never silently lose an update), and it
produces exactly the symptom reported: nothing visibly broken, but the
capture doesn't look complete.

This is a straightforward backpressure bug, not an exotic one: the sibling
hop in the same codebase (`connection.rs`, connection → collector) already
used the correct blocking pattern, `tx.send(ev).await`. The output hop
(collector → writer) just didn't.

### Bug 2 — quiet-market detection ping-ponging (found while stress-verifying the fix for Bug 1)

While stress-testing the Bug 1 fix at a larger scale, the logs showed
individual low-activity markets being flagged "quiet" and resubscribed 4
times in under 2 minutes — far more often than the intended 45-second
`quiet_timeout` should allow.

Root cause: when a quiet symbol's output source is switched to its backup
connection, that backup was subscribed from the very start of the run and
typically already has its own, equally stale `last_activity` timestamp for
that symbol. The very next health-check tick (every 3 seconds) sees the
backup as *also* quiet and switches straight back — the two connections
ping-pong every few seconds instead of respecting the intended cadence,
spamming Kalshi with resubscribe commands for every naturally low-volume
market (which, in a pool of thousands of future-dated markets, is most of
them). Sustained command spam like this is a real path to connection
instability or exchange-side throttling — a second, independent way to
produce "syncing" symptoms.

## Root cause investigation

Both bugs were found by reading the exact code on the submitted commit
(`716e471`, what's live on GitHub) rather than guessing from the symptom
description, per this project's own stated debugging discipline. Bug 1 was
proven with a deterministic regression test before any fix was written:

```
thread 'collector::tests::delta_emission_never_drops_under_backpressure' panicked:
assertion `left == right` failed: every emitted delta must reach the output
channel; 49 of 50 were silently lost
  left: 1
 right: 50
```

Bug 2 was found by running the Bug-1-fixed binary at a larger, longer stress
scale (800 requested markets, 16 connections, 110 seconds) specifically to
exercise conditions the original capped demo runs never reached, then
auditing the resulting log for an anomaly (`quiet: 1664` occurrences; the
same symbol tripped 4 times in a window where ~2 was the expected maximum).

## The fixes

1. **Bug 1:** `collector.rs`'s three output call sites now send through a new
   `emit()` helper that uses `self.out.send(msg).await` — a blocking,
   backpressure-respecting send, matching the pattern already correct in
   `connection.rs`. `handle_event`, `emit_snapshot_from`, `switch_source`,
   `emit_minute_snapshots`, `health_check`, and `recycle_next` all became
   `async` to carry this through, and the main event loop now `.await`s them.
   A send failure (which can now only mean the writer has shut down) is
   logged loudly instead of silently discarded.
2. **Bug 2:** a new `last_quiet_action: HashMap<String, Instant>` tracks the
   last time quiet-handling fired for each symbol; a new pure function,
   `quiet_action_allowed(last, now, quiet_timeout)`, gates re-triggering so a
   symbol can't be acted on again before `quiet_timeout` has actually
   elapsed, regardless of which connection currently sources it.

## Verification

- **Unit (deterministic):** `delta_emission_never_drops_under_backpressure`
  fails against the pre-fix code (49/50 lost) and passes against the fix (0
  lost). `quiet_action_blocked_within_cooldown` and
  `quiet_action_allowed_after_cooldown_elapses` pin the cooldown's boundary
  behavior directly. 42 tests pass total.
- **Live, same stress scenario, before vs. after Bug 2's fix:**

  | | before | after |
  |---|---|---|
  | `quiet` triggers (110s run) | 1664 | 1064 |
  | max repeats for one symbol | 4 | 2 (matches the intended ~45s cadence) |

- **Live, full pipeline, post-fix (800 requested markets, 16 connections,
  110s run, readback through the template's own `AlignedSbeReader`):**
  **2,786 snapshots, 612,550 deltas, 956 symbols, 0 errors, 0 "output writer
  is gone," 0 deltas-before-snapshot.** Clean SIGINT shutdown.

## Why this wasn't caught before submission

The original testing (both unit tests and the live demo runs referenced in
the submission's writeup) never pushed enough sustained volume through the
output channel to fill a 200,000-capacity buffer, nor ran long enough at
large-enough scale to expose the quiet-detection cadence bug. Both are
scale/duration-dependent failure modes that only manifest under load
resembling the project's own *default* configuration (unlimited markets) run
for more than a couple of minutes — which is a realistic thing for a
reviewer evaluating completeness to actually do, and exactly what happened
here.
