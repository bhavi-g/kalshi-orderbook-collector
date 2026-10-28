# Kalshi/Polymarket Order-Book Collector — Project Writeup

This document explains what was built, why it was built that way, what the
real trade-offs were, and what I learned by actually running it against
production exchanges rather than guessing from docs. It's written for someone
reviewing the submission who wants the reasoning, not just the code.

For the formal spec and task breakdown, see `docs/specs/` and `docs/plans/`.
This document is the narrative version: decisions, alternatives considered,
and why each one won.

---

## 1. What this is

A Rust service that connects to a prediction-market exchange (Kalshi or
Polymarket — same binary, one env var), subscribes to every live regular
market, maintains an accurate order book per market in memory, and streams
that book's history — a snapshot every minute, a delta on every change — to
stdout as binary SBE messages, using the challenge's own encode/decode
helpers so the output format is guaranteed standard.

The two properties the spec cares about most are **completeness** (never
silently lose a price-level update) and **time accuracy** (record both when
the exchange acted and when we received it). Almost every design decision
below traces back to one of those two goals.

## 2. Why I spiked before designing

The challenge gives you Kalshi's WebSocket docs, but docs and wire reality
diverge in ways that matter: timestamp units, field names, whether a `clock`
field actually appears, whether deltas are absolute or relative. I didn't
want to design against documentation and then discover during implementation
that the real messages don't match.

So before writing the design doc, I built a disposable Rust program, got real
Kalshi API credentials, and connected to **production** Kalshi. That single
decision paid for itself repeatedly:

- **Kalshi's delta is a signed *change* in size (`delta_fp`), not an absolute
  quantity.** The SBE schema's `PriceLevelUpdateDomain.quantity` is explicitly
  documented as absolute. If I'd designed from the docs alone and assumed
  "delta = diff to apply downstream," I'd have shipped a book that silently
  drifts from reality — the exact anti-goal the spec warns about.
- **Kalshi quotes YES and NO, and both are *bids*.** There's no asks field.
  A naive "YES→buy, NO→sell" mapping would produce a *fake* order book — not
  wrong-looking, just wrong. This is the kind of bug that would pass casual
  review and fail every backtest downstream.
- **The open-market listing is ~95% `KXMVE...` auto-generated multivariate
  markets** (confirmed later: ~199,500 MVE vs ~9,800 regular, out of a scan
  that hit my own 200-page safety cap). The challenge says to exclude MVEs,
  but I wouldn't have known *how dominant* they are, or that regular markets'
  tickers are embedded as nested `market_ticker` fields inside MVE market
  objects (useful for harvesting), without actually paging through the API.
- **The docs mention a `clock` field for matching updates across redundant
  connections. It is not present on the live wire.** The real messages have
  `seq` (per-connection) and `ts_ms` (per-event, comparable across
  connections). This directly determined how Extension 1 (redundancy) and
  Extension 4 (latency) are implemented — I built them around what's actually
  there, not what the docs describe.
- **Timestamp units**: the docs contradict themselves on seconds vs.
  milliseconds for `KALSHI-ACCESS-TIMESTAMP`. Live testing confirmed
  milliseconds immediately, instead of costing a debugging session later.

**Trade-off:** spiking first costs time up front before any "real" design
work. I judged that cost small next to the cost of discovering a wrong
assumption mid-implementation, or worse, shipping a book that's subtly wrong
in a way that only shows up in a backtest months later. For a challenge whose
entire premise is "small data errors matter enormously," I didn't think
guessing from docs was acceptable.

## 3. The core representation decision: YES-referenced book

This was the single highest-leverage design decision, so it's worth
explaining in full.

Kalshi has no native bid/ask book — every market has a YES side and a NO
side, and both sides only ever show *bids* (resting buy orders). A NO bid at
price `q` is economically equivalent to offering to sell YES at `1 − q` (if
you're willing to buy NO at 30¢, you're equally willing to sell YES at 70¢,
since YES + NO = $1 at settlement).

I considered two representations:

1. **Raw passthrough**: emit YES levels as `Side::Buy` and NO levels as
   `Side::Sell`, using Kalshi's own prices unchanged. Simplest to code —
   almost no transform logic.
2. **YES-referenced synthetic book** (what I built): YES bids → bids as-is;
   NO bids → asks at `1 − price`, same size.

I chose option 2, and asked the requester to confirm rather than assuming,
because it changes what the output *means*. Option 1 is not actually a bid/ask
book — it's two bid books stapled together with mislabeled sides, where
"asks" are really someone else's bids on the inverse contract. A strategy
reading this data and computing a spread, or crossing what it thinks is an
ask, would be trading against a meaningless number. Option 2 produces the
same two-sided depth chart you'd see on any exchange, with a correct bid-ask
spread and correct crossing logic. Given the spec's emphasis on data being
*usable* for backtesting and HFT strategies, only option 2 is actually
correct, not just plausible-looking.

**Trade-off:** option 2 requires a price transform (`1_000_000 - price_fp`)
and loses the "which contract is this really" distinction at the top level —
if you want raw YES/NO data back out, you'd need to reconstruct it from
`symbol` conventions. I accepted that because the challenge's SBE schema has
exactly one `Side` enum meant to mean buy/sell, and a downstream backtester
has no way to know it should special-case Kalshi's convention.

## 4. Architecture: one seam, not many

### The shape

```
Exchange adapter (ExchangeCodec trait)
  auth signing · REST discovery · WS frame decode
            │  NormalizedEvent (exchange-agnostic)
Generic collector core
  connections · books · source selection · monitoring · lifecycle
            │  SnapshotDomain / PriceLevelUpdateDomain
Output writer → stdout (AlignedSbeWriter)
```

Everything except the `ExchangeCodec` implementation is exchange-agnostic.
The core never sees "YES," "NO," "dollars," or `delta_fp" — it sees a
`NormalizedEvent::Snapshot` or `NormalizedEvent::Delta` with fixed-point
prices and an `Amount` that's either a relative change or an absolute value.

### Why one seam, and why there

I considered three shapes for the extensibility the challenge's Extension 3
asks for:

- **Config-driven exchange differences** (one codebase, if/else branches on
  an "exchange" flag scattered through the collector). Rejected: this is how
  codebases accumulate exchange-specific bugs in shared logic. The collector
  core should never need to know which exchange it's talking to.
- **Fully separate binaries per exchange**, sharing only the SBE output
  layer. Rejected: this duplicates connection management, redundancy,
  monitoring, and lifecycle logic — exactly the generic, hard-to-get-right
  code the challenge wants built once and reused.
- **One trait boundary (`ExchangeCodec`), one generic core** (what I built).
  The trait has five responsibilities: URL/auth, symbol discovery, frame
  construction (subscribe/add/remove), and decode. Everything on the other
  side of that boundary — connection lifecycle, redundancy, monitoring,
  symbol add/remove, snapshotting, output — is exchange-agnostic.

I didn't just assert this trait was sufficient — I **proved it** by writing
a second, real adapter (`codec/polymarket.rs`) against Polymarket's public
CLOB market channel and running the *same collector binary* against it live,
with zero changes outside that one file. That run produced a verified
capture (215 snapshots, 5,207 deltas, 60 symbols, 0
deltas-before-snapshot) — the same completeness guarantee the Kalshi path
gives, through the same core.

Writing the second adapter also surfaced a real design gap I wouldn't have
found by inspection alone: **Polymarket sends absolute level sizes in its
`price_changes` messages; Kalshi sends signed changes.** My first cut of
`NormalizedEvent::Delta` only had a `change: Fp` field, implicitly assuming
"relative" was universal. I had to introduce an `Amount` enum
(`Relative(Fp)` / `Absolute(Fp)`) and extend `OrderBook` with both
`apply_delta` (accumulate) and `set_level` (overwrite) before Polymarket's
semantics fit. That's exactly the kind of assumption a single-exchange
implementation bakes in silently and a second real adapter exposes — which
is why I think building the second adapter, not just designing for one, was
the right bar for "prove the core is generic."

**Trade-off:** the `Amount` abstraction adds a small amount of
indirection (`book.apply()` dispatches on the enum) for logic that, for
Kalshi alone, could have been a single `apply_delta` call. I accepted that
because the alternative — a book API that only understands relative changes
— would have made Polymarket support impossible without a second, parallel
book implementation, defeating the entire point of Extension 3.

## 5. The order book itself

`OrderBook` (per market, per *connection* — see §6) is two `BTreeMap<price,
qty>`, one per side. Using a sorted map rather than, say, a `Vec` of levels
was a direct choice for correctness: deltas arrive keyed by price and must
update-or-insert-or-delete a specific level in O(log n), and the output
snapshot needs levels in price order. A `HashMap` would be O(1) for the
update but require a sort on every snapshot; a sorted `Vec` would need O(n)
insertion. `BTreeMap` gives both at the complexity that matters for a
structure with typically tens to low hundreds of live price levels.

A level reaching quantity 0 is **removed from the map**, not stored as a
zero entry — and the SBE output explicitly emits `quantity: 0` as the
*signal* that a level should be deleted downstream. This was a conscious
choice to match the review focus called out in the plan ("level emptied then
re-added" — covered by a dedicated test, `readd_after_delete`): a downstream
consumer reconstructing a book from the delta stream needs an unambiguous
delete signal, and "the level just isn't in the next snapshot" is not
unambiguous if snapshots and deltas can interleave.

## 6. Extension 1 — Redundant sockets, and why books live per-connection

The hardest real design question here: **what does "the book" mean when you
have two sockets that may have seen different messages?**

I chose to maintain **one `OrderBook` per (connection, symbol) pair**, not
one shared book per symbol. Every connection carrying a given market applies
its own stream of snapshots and deltas to its own book, independently. The
collector tracks, separately, which connection is the current **output
source** for each symbol — i.e., whose book gets translated into emitted
SBE messages.

The alternative — one shared book per symbol, fed by whichever connection's
message arrives first — is simpler to reason about on paper, but it has a
real correctness problem: if connection A drops a delta silently (the
scenario the entire redundancy extension exists to protect against) and
connection B didn't, a shared book has no way to detect that A's view is
now wrong, because B's correct updates would paper over A's gap in the
single shared structure. Keeping books separate per connection means that
when I detect a gap or stall on the *source* connection, I can fail over to
a connection whose book is actually known to be live and consistent, and —
critically — **emit a fresh snapshot from that connection's book**, because
its state might genuinely differ from what was last output.

This is also why the spec's note that "the two websockets have potentially
different orderbook representations in memory" led directly to the
mandatory re-snapshot-on-switch behavior (`switch_source` in
`collector.rs`): since the backup connection's book may have been built from
a different maintenance window or a different reconnect, there is no safe
way to keep emitting deltas against it without first re-anchoring the
downstream consumer with an absolute snapshot.

**Trade-off:** per-connection books mean duplicated memory (2x, for
redundancy factor 2) and duplicated CPU for applying every update twice.
For prediction-market order books — tens to low hundreds of price levels,
nowhere near the depth of a liquid equity or FX book — this cost is
negligible. I judged the correctness guarantee worth far more than the
memory it costs here; for a market with genuinely deep books this trade-off
would need revisiting.

**Source selection** (`manager_logic::choose_source`) picks the healthiest
candidate by *recency of last message*, filtered to only connections not
currently stalled. This is intentionally simple — no weighted scoring, no
historical reliability tracking — because the stall/gap signals that trigger
a switch are already the expensive, infrequent case; optimizing the
selection *among already-healthy* candidates has low marginal value compared
to correctly detecting *un*healthy ones in the first place.

## 7. Extension 2 — Monitoring, and the two kinds of silence

The spec calls out two distinct failure modes that both look like "nothing
is happening," but need different responses:

1. **A whole connection going dead** — no messages at all. Detected via
   `last_msg` age vs. `stall_timeout`. Response: switch every symbol on that
   connection to a backup.
2. **One symbol going quiet on an otherwise-live connection** — the socket
   is fine, the exchange just stopped sending updates for that market
   (illiquid market, or a server-side bug dropping one subscription
   silently, which is exactly the scenario the spec warns about: "exchanges
   won't always be so nice as to tell us"). Detected via
   `last_activity` per `(connection, symbol)` vs. `quiet_timeout`.
   Response: **resubscribe that one symbol** (remove + re-add, which Kalshi
   answers with a fresh snapshot) *and* switch its output source, rather
   than touching the whole connection.

I kept these as separate code paths rather than unifying them into one
"health" concept, because conflating them would mean either recycling a
perfectly healthy connection because one symbol on it went quiet (wasteful —
affects every other symbol on that socket), or failing to notice a quiet
symbol because the connection as a whole still looks alive (exactly the
silent-data-loss failure the spec describes).

**Periodic whole-socket recycling** (`recycle_next`, on a fixed interval
cycling through connections) is separate again from both of the above — it's
not a response to detected unhealthiness, it's a *preventative* measure
against the class of bug the spec explicitly names: cumulative, undetectable
exchange-side issues (memory leaks, degraded internal state) that never
manifest as an observable stall or gap until they do. I implemented it as
"move this connection's sourced symbols to backups, then force a reconnect,"
reusing the exact same re-snapshot-on-switch path as Extension 1 — recycling
is just a scheduled, self-inflicted version of the failover Extension 1
already handles, which kept the code from growing a third, parallel
mechanism.

I verified all three paths live, not just by code inspection: a 70-second
Kalshi run produced 2 real sequence gaps (auto-resynced), 1 scheduled
recycle (31 source switches), and 18 quiet-symbol resubscriptions, with the
post-hoc readback still showing 0 deltas-before-snapshot — i.e., the
failover machinery didn't just fire, it fired *correctly*.

## 8. Why stdout, and the consequence that follows from it

The spec says "write orderbook deltas to stdout, along with a full snapshot
every minute." I was initially tempted to read that loosely — the provided
`AlignedSbeWriter` takes a file path, and binary data on stdout is awkward
to work with interactively. I raised this as an open question rather than
deciding it myself, because it's exactly the kind of instruction where a
"helpful" reinterpretation changes what gets built without the requester's
sign-off. The answer was unambiguous: follow the spec literally. So the
writer is pointed at `/dev/stdout`, and all logs were moved to stderr so
stdout stays a clean, redirectable binary stream (`cargo run ... > capture.sbe
2> run.log`).

That decision has one real consequence worth naming: `AlignedSbeWriter::flush()`
zero-pads the *entire* chunk to the configured chunk size (64 MiB, matching
what `AlignedSbeReader` expects by default). If I called `flush()` on every
minute-snapshot boundary — a natural first instinct, "write it out so it's
not buffered" — every one of those flushes would emit a padded 64 MiB burst,
most of it zeros, once a minute, forever. Instead, the writer only flushes
**once, on shutdown** (when the channel closes after SIGINT); messages
otherwise accumulate and auto-flush only when a chunk genuinely fills. This
is why every one of the captures in this writeup is exactly 67,108,864 bytes
(64 MiB) for a short run — that's expected and correct, not a bug: it's one
chunk, zero-padded once, at the end.

**Trade-off:** this means a crash (not a clean SIGINT) between chunk
boundaries loses whatever's buffered since the last 64 MiB flush. I accepted
this because it's the same trade-off the template's own `AlignedSbeWriter`
design makes — chunk-aligned writes are what let `AlignedSbeReader` achieve
its stated 100M messages/sec/thread target by never needing to coordinate a
read across a chunk boundary. Fighting that design by flushing eagerly would
have broken the reader's fast path for a gain only realized in the crash
case, which SIGINT handling already covers for the common case (operator-
initiated shutdown).

## 9. Why fixed-point, not floats, and the rounding rule

Kalshi and Polymarket both quote prices and sizes as decimal strings
(`"0.5900"`, `"798.00"`). The SBE schema fixes the wire format as `i64` with
`1_000_000 = 1 unit`. I parse those decimal strings directly into fixed-point
integers (`decimal_to_fp`), rather than going through `f64` at any point.

This matters because `0.1 + 0.2 != 0.3` in binary floating point, and a book
that's silently off by a few units of fixed-point precision because of
repeated float arithmetic is a worse failure than a crash — it looks correct
and produces wrong backtests. `decimal_to_fp` does integer arithmetic only:
split on the decimal point, pad/truncate the fractional part to exactly 6
digits, and combine as integers. Kalshi's tightest observed tick (`0.0001`,
i.e. a hundredth of a cent) and finest size granularity (fractional
contracts like `0.25`) both fit exactly in 6 decimal digits of fixed point,
so this conversion is exact for every real input I observed, not just
approximately correct.

## 10. Testing strategy: what's unit-tested vs. what's live-verified

I split verification into two tiers deliberately, rather than trying to make
everything a unit test or trusting everything to a live run:

- **Pure logic** (fixed-point conversion, book apply/delete/re-add, the
  YES/NO and Kalshi/Polymarket decode transforms, sequence-gap detection,
  sharding, source selection, SBE round-trip through the real
  `AlignedSbeWriter`/`AlignedSbeReader`) is unit-tested — 38 tests, all
  deterministic, all using real captured message fixtures from the live
  spikes rather than invented ones. These run in well under a second and
  catch regressions immediately.
- **Integration behavior** (does a real seq gap actually trigger a real
  resync against the real exchange; does a real quiet market actually get
  resubscribed; does shutdown actually flush) is **live-verified against
  production**, because the behavior genuinely depends on exchange timing
  and message patterns that a mock can't honestly reproduce — I'd just be
  testing my own mock's assumptions. Each live run's output is independently
  re-verified by reading the capture back through the challenge's own
  `AlignedSbeReader` and checking the one invariant that matters most:
  zero deltas arrive for a symbol before that symbol's first snapshot.

I didn't write a mock exchange server. I considered it, specifically for
exercising the gap/stall/quiet paths deterministically in CI. I chose not to,
because a hand-rolled mock encodes *my own* assumptions about Kalshi's
behavior — and the entire reason this project spiked against the live
exchange first was to avoid building on assumptions. A green test against a
mock I wrote myself would have told me my code matches my own mental model,
not that it's correct. The live runs, each independently re-verified by an
automated reader, are a stronger and more honest signal for exactly the
failure modes (gaps, stalls, quiet subscriptions) the extensions exist to
handle.

**Trade-off:** live verification isn't reproducible on demand the way a
mock-based test suite is — it depends on production having live markets with
real activity at run time, and it can't run in a sandboxed CI without
credentials. I accepted that for this project because the alternative (trust
a self-authored mock) would have been testing less, not more.

## 11. Scale: why the default changed from capped to uncapped, and what that costs

My first working version defaulted to `MAX_MARKETS=40` — a deliberate choice
to keep a demo run fast and low-risk while everything else was being built
and verified. Once asked to make the submission unambiguous against the
spec's "subscribe to all live symbols," I removed the cap (default `0` =
unlimited) and changed discovery so it only stops early when a limit is
explicitly set.

Running that for real surfaced the actual scale: Kalshi currently lists
roughly **9,800 regular markets alongside ~199,500 auto-generated MVE
markets** (the latter excluded per the spec). At `PER_CONN=150`
and `REDUNDANCY=2`, the full regular-market universe implies on the order of
130 simultaneous WebSocket connections, and a full paginated discovery scan
alone takes minutes (confirmed: 200 pages / 200,000 top-level market objects
in ~149 seconds, and that didn't even reach the end of the listing).

I verified the uncapped *code path* at a representative scale — 600
requested markets, 648 symbols actually subscribed across 12 redundant
connections, 2,340 snapshots and 186,753 deltas on readback, 0
deltas-before-snapshot — rather than running the literal full ~130-connection
scan against production. That's a conscious trade-off, not a shortcut: the
130-connection run uses a real personal API key with real rate-limit and
account-standing risk, for a scale difference (12 vs. 130 connections) that
doesn't exercise any code path the 12-connection run didn't already prove.
The *architecture* has no cap; the *decision not to hammer someone's live
credentials harder than necessary to prove it* is a separate, deliberate
call.

## 12. Extension 4 — Latency, and why the measurement had to match the wire

The challenge's redundancy extension implies a comparison — "connection A
got it in 18ms, connection B in 7ms" — but the honest way to measure that
turned out to be non-obvious. My first instinct was to match events across
two connections by `seq`. That doesn't work: **`seq` is scoped to one
connection's subscription**, not comparable across sockets, which I only
knew because the live spike showed it directly rather than trusting the
docs' mention of a cross-connection `clock` field that, again, doesn't
actually appear on the wire.

So the latency probe matches the *same exchange event* across two parallel
connections by content — `(ticker, side, price, ts_ms)` — since `ts_ms` is
the exchange's own event timestamp and is identical on whichever connection
receives it. Measured live, on 2,415 events matched across two connections
over 25 seconds: median spread 1.83ms, p90 5.15ms, max 92.65ms — and one
connection was faster on 91% of events, not a coin flip. That last number is
the practically useful finding: it's not just that redundant connections
differ, it's that the difference is *consistently* in one direction over a
short window, which is exactly the empirical basis for "race several
connections and keep the fastest" as a real latency-reduction technique, not
just a theoretical one. Full writeup with methodology and caveats is in
`docs/latency.md`.

## 13. What I'd flag as open trade-offs, not hidden gaps

A few decisions are deliberate boundaries of scope, named here rather than
left implicit:

- **Discovery cost at full scale.** A genuinely uncapped run pages through
  Kalshi's entire open-market listing before subscribing to anything, which
  takes minutes given how MVE-dominated the listing currently is. A
  production system might want to subscribe incrementally as pages arrive
  rather than waiting for full discovery; I didn't build that because it
  adds real complexity (partial-subscription state) for a cost that's a
  one-time startup delay, not an ongoing correctness or completeness issue.
- **Polymarket's unsubscribe is a no-op.** Polymarket's public market
  channel has no documented single-asset unsubscribe; `update_remove_frame`
  returns an empty string (skipped by the connection task) and removed
  symbols are relied on to drop out via the next whole-socket recycle
  instead. This is honestly reported in the code and README, not papered
  over — it's a real asymmetry between the two adapters that the generic
  trait doesn't hide.
- **Source selection is recency-only**, not a weighted/historical score (see
  §6). Reasonable for the problem size here; would need revisiting for a
  much larger or more failure-prone connection pool.

None of these affect the core guarantee — every live run's capture, across
both exchanges and across a 10x scale difference in market count, verified
with zero deltas arriving before their symbol's snapshot.
