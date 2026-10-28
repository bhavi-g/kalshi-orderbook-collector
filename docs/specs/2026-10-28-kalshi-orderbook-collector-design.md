# Kalshi Order-Book Collector — Design

**Date:** 2026-10-28
**Status:** Draft for approval
**Language:** Rust (only)

## 1. Intent & why

Build a service that connects to Kalshi, maintains a live order book for every
regular (non-MVE) market, and writes a **complete, time-accurate** stream of
order-book updates to disk in a standard binary format. The data is for
backtesting and high-frequency research, where a single missed delete or a few
milliseconds of timing error can flip a strategy from profitable to losing. So
the two properties that matter above all else are:

- **Completeness** — never silently drop an update. A missed "delete this level"
  message makes a backtest believe liquidity existed that was actually gone.
- **Time accuracy** — record both when the exchange acted and when we received
  the message, so downstream code can reason about latency honestly.

This is the Voluntas engineering challenge, done as a real, production-minded
submission including all four extensions.

## 2. Goals

1. Connect to Kalshi production over authenticated WebSockets (Ed25519).
2. Discover **all live regular markets**, subscribe to all of them, and keep the
   subscription set in sync as markets open and close.
3. Maintain an accurate in-memory order book per market from snapshot + deltas.
4. Emit, through the challenge's SBE helpers only:
   - an order-book **delta** on every price-level change, and
   - a full **snapshot** for every market once per minute.
5. Load-balance markets across multiple WebSocket connections.
6. On **SIGINT**: stop sockets, flush and close the output file cleanly.
7. **Ext 1 — Redundant sockets:** run redundant connections; when the source for
   a market stalls or gaps, switch to a healthy connection and emit a fresh
   snapshot before resuming deltas.
8. **Ext 2 — Subscription monitoring:** track per-symbol update rate; unsubscribe
   + resubscribe symbols that go too quiet; periodically recycle whole sockets.
9. **Ext 3 — Generic core:** all logic above is exchange-agnostic; the only
   exchange-specific piece is an encode/decode adapter. Kalshi is the first
   adapter; the shape must make a Polymarket adapter a drop-in.
10. **Ext 4 — Latency:** experiment with faster connections and write findings to
    a markdown file.

## 3. Non-goals

- MVE (multivariate) markets — explicitly excluded by the challenge.
- Order entry, portfolio, fills, or any authenticated *trading* action. Read-only
  market data only.
- A downstream reader/backtester. We produce the SBE file; consuming it is out of
  scope (the template's `AlignedSbeReader` already exists for verification).
- A general config/plugin system beyond what Ext 3 needs.
- Editing the SBE schema or the generated code (the template forbids it).

## 4. Anti-goals (these would make it a failure even if it "works")

- **Silent data loss.** Dropping messages without detecting the gap is worse than
  crashing. Every gap must be detected (via `seq`) and repaired with a resync +
  fresh snapshot.
- **A book that drifts from the exchange.** Applying deltas to the wrong base, or
  mis-handling the YES/NO transform, produces plausible-looking but wrong data.
- **Blocking the whole service on one slow market or one dead socket.**
- **Losing buffered output on shutdown.** SIGINT must flush; a truncated final
  chunk is data loss.
- **Leaking the private key** into logs, output, or anywhere off-machine.

## 5. Constraints (fixed, not negotiable)

- **Output must go through the template's helpers** (`AlignedSbeWriter`,
  `SnapshotDomain`, `PriceLevelUpdateDomain`, `Side`). The repo is the starting
  point; we replace `main.rs` and add modules under `src/`.
- **Rust only.** No other languages or services.
- **Kalshi production**, Ed25519 key auth. Credentials:
  - Key ID: supplied at runtime (env var `KALSHI_KEY_ID`).
  - Private key: read from a file path at runtime (env var `KALSHI_KEY_PATH`),
    PKCS#8 PEM, never printed or transmitted except in the TLS handshake.
- **SBE fixed-point:** price & quantity are `i64` where `1_000_000 = 1 unit`
  (1 USDC for price, 1 contract for quantity).
- **Timestamps** are `chrono::DateTime<Utc>` (µs on the wire).

## 6. Verified exchange facts (from the live spike on 2026-10-28)

Auth, subscription, snapshot, and delta all confirmed against live production.

**WS URL:** `wss://api.elections.kalshi.com/trade-api/ws/v2`
**REST base:** `https://api.elections.kalshi.com`
**Auth headers** on the WS handshake and every REST GET:
`KALSHI-ACCESS-KEY` (Key ID), `KALSHI-ACCESS-TIMESTAMP` (unix **ms**),
`KALSHI-ACCESS-SIGNATURE` = base64(Ed25519.sign(`"{ts}{METHOD}{path}"`)).
For the WS handshake the signed path is `/trade-api/ws/v2`, method `GET`.
For REST the path is signed **without** query parameters.

**Subscribe frame:**
```json
{"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[...]}}
```
Server replies `{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":N}}`,
then one `orderbook_snapshot` per market, then `orderbook_delta` messages.

**Snapshot `msg`:** `market_ticker`, `market_id`, `yes_dollars_fp`, `no_dollars_fp`,
each `[["0.5900","798.00"], ...]` = `[price_dollars, size]` as strings. Envelope
has `sid`, `seq`, `sending_ts_ms`.

**Delta `msg`:** `market_ticker`, `price_dollars` (e.g. `"0.5900"`),
`delta_fp` (**signed change** in size, e.g. `"-1.00"`), `side` (`"yes"`/`"no"`),
`ts`/`ts_ms` (exchange event time, ms). Envelope has `sid`, `seq`, `sending_ts_ms`.

**Key consequences:**
- `delta_fp` is a *change*, not an absolute. We keep the book in memory, apply the
  change, and emit the resulting **absolute** level quantity (0 ⇒ level deleted).
- `seq` increments per **subscription (sid)** across all its markets; a gap ⇒
  missed messages on that connection.
- `clock` (in the docs) is **not** present on the live wire; we rely on `seq`
  per connection and `ts_ms` per event.
- The `status=open` market list is dominated by thousands of `KXMVE…`
  cross-category markets, which we filter out.

## 7. The Kalshi → SBE transform (decided: YES-referenced book)

Each Kalshi binary market has YES bids and NO bids; both are bids. We publish one
two-sided book per `market_ticker`, referenced to the YES contract:

- **Bid** level: a YES bid at price `p`, size `s` ⇒ `Side::Buy`, `price_level = p`,
  `quantity = s`.
- **Ask** level: a NO bid at price `q`, size `s` ⇒ `Side::Sell`,
  `price_level = 1 − q`, `quantity = s`.

Prices are converted to fixed-point i64: `round(price_dollars × 1_000_000)`
(so `0.5900 → 590_000`; `1 − 0.3000 = 0.7000 → 700_000`). Sizes likewise:
`round(size × 1_000_000)` (fractional sizes like `"0.25"` are supported).

`symbol` = `market_ticker`. The in-memory book stores, per side, a map
`price_level(i64) → quantity(i64)`. A delta updates the running quantity on the
correct derived side; emitting uses the **post-update absolute quantity**.

**Timestamps:**
- `exchange_timestamp` = event time: delta `ts_ms`; snapshot `sending_ts_ms`.
- `received_at` (snapshot) / `backtest_timestamp` (delta) = our local receive
  instant (`Utc::now()` at the moment the frame is read off the socket).

## 8. Architecture (generic core + exchange adapter — Ext 3)

```
            ┌──────────────── Exchange adapter (Kalshi) ────────────────┐
            │  auth signing · REST symbol discovery · WS frame decode    │
            │  → emits normalized events: Snapshot / Delta / Heartbeat   │
            └───────────────────────────┬────────────────────────────────┘
                                         │  normalized events
 ┌───────────────────────── Generic collector core ─────────────────────────┐
 │ Connection manager (N sockets, load balancing, redundancy, recycling)     │
 │ Order-book store (per symbol: absolute levels, source-connection tracking)│
 │ Symbol lifecycle (discover, add, remove)                                  │
 │ Monitor (per-symbol rate, gap detection, resync triggers)                 │
 │ Snapshotter (minute timer) · SIGINT handler                               │
 └───────────────────────────────┬──────────────────────────────────────────┘
                                  │ SnapshotDomain / PriceLevelUpdateDomain
                         ┌────────▼─────────┐
                         │  AlignedSbeWriter │  (template helper; the only sink)
                         └───────────────────┘
```

The exchange-specific trait (names indicative, finalized in the plan):

```rust
trait ExchangeCodec {
    /// Sign/auth material for a new connection handshake.
    fn connect_request(&self, path: &str) -> AuthedRequest;
    /// Discover the current universe of regular symbols.
    async fn list_symbols(&self) -> Result<Vec<Symbol>>;
    /// Build subscribe / update_subscription / unsubscribe frames.
    fn subscribe_frame(&self, markets: &[Symbol]) -> String;
    fn update_frame(&self, add: &[Symbol], remove: &[Symbol]) -> String;
    /// Decode one raw WS text frame into zero or more normalized events.
    fn decode(&self, raw: &str, recv_at: DateTime<Utc>) -> Result<Vec<NormalizedEvent>>;
}

enum NormalizedEvent {
    Snapshot { symbol, bids: Vec<(i64,i64)>, asks: Vec<(i64,i64)>, exch_ts, seq, sid },
    Delta    { symbol, side, price_level: i64, new_qty_hint: Option<i64>, change: i64, exch_ts, seq, sid },
    SubscribeAck { sid }, Heartbeat, ...
}
```

The core never mentions Kalshi, YES/NO, dollars, or `delta_fp`. A Polymarket
adapter implements the same trait (its WS sends absolute sizes keyed by token id,
and REST discovery differs) and nothing else changes.

## 9. Output behavior

- **Output goes to stdout** (per the challenge spec), written with
  `AlignedSbeWriter` pointed at `/dev/stdout`, using the **64 MiB chunk size** the
  template's `AlignedSbeReader` expects so the stream reads back correctly.
  Human-readable logs go to **stderr** only, keeping stdout a clean binary stream
  (redirect `> capture.sbe` to persist it). Decided per requester: follow the
  spec's "stdout" exactly, no file-path assumption.
- One `AlignedSbeWriter` sink (a single writer task owns stdout; the rest send it
  messages over a channel to keep ordering and avoid lock contention).
- Note: `AlignedSbeWriter::flush()` pads to a full chunk, so we do **not** flush
  per minute (that would emit a padded 64 MiB burst); messages auto-flush as each
  64 MiB chunk fills, and we flush exactly once on SIGINT to emit the final chunk.
- **Delta on every change:** when an applied update changes a level's absolute
  quantity, write a `PriceLevelUpdateDomain` with the new absolute quantity
  (quantity `0` = deletion).
- **Snapshot every minute:** a timer walks every live book and writes a
  `SnapshotDomain` with the current absolute levels. Also write a fresh snapshot
  for a market immediately after a source switch or resync (Ext 1).
- **File rotation:** the writer uses 64 MiB aligned chunks (template default).
  Output file path configurable; default under an `out/` dir with a timestamped
  name. (Open question O3 — see §13.)
- **SIGINT:** stop reading sockets, drain in-flight events, `flush()` the writer,
  drop it (closing the file), exit 0.

## 10. Symbol discovery & lifecycle

- At startup and on a periodic interval (e.g. every 30–60 s), page the REST
  `/markets?status=open` endpoint (cursor pagination), collect all tickers whose
  ticker does **not** start with `KXMVE` (MVE filter), as the target set.
- Diff against the currently-subscribed set: newly-open markets are added
  (assigned to the least-loaded connection via `update_subscription`/subscribe);
  markets no longer open are removed (`update_subscription` delete) and their
  book dropped.
- Because the open-market list is huge and MVE-dominated, discovery must page
  efficiently and cheaply; it runs off the hot path.

## 11. Load balancing across sockets

- A configurable max markets per connection (default chosen conservatively, e.g.
  ~200; tuned during implementation against Kalshi limits). Markets are sharded
  across `ceil(total / max)` **primary** connections.
- Assignment is sticky (a market stays on its connection) so `seq` continuity is
  meaningful; rebalancing only on add/remove or socket recycle.

## 12. Extensions in detail

**Ext 1 — Redundant sockets.** Each market is carried on a **primary** and at
least one **backup** connection (separate WS, same subscription content). The
core tracks, per market, which connection is the current **source** of truth for
output. Health per connection = time since last message + `seq` continuity. When
the source stalls (no messages for a timeout) or gaps (`seq` jump), the core
switches that market's source to a healthy connection and **emits a fresh
snapshot** for it before resuming deltas (the two sockets' in-memory books can
differ due to drops). Books are maintained **per connection** so a backup is
always warm.

**Ext 2 — Subscription monitoring.** Track updates/sec (or /min for quiet
markets) per symbol per connection. If a symbol's rate on its source drops below
a threshold relative to its backup (or to a baseline), treat the source as quiet:
unsubscribe + resubscribe that symbol (which forces a fresh snapshot), or switch
source. Separately, **recycle whole sockets** on a periodic timer: stand up a new
connection, resubscribe its markets, confirm it's healthy, then tear down the old
one — mitigating exchange-side memory leaks / cumulative bugs. Recycling and
source-switching reuse the same "fresh snapshot on switch" path.

**Ext 3 — Generic core.** As in §8: `ExchangeCodec` is the only seam. Deliverable
includes the Kalshi adapter wired end-to-end; the trait + normalized event model
proven sufficient by sketching (not necessarily shipping) the Polymarket mapping.

**Ext 4 — Latency.** Experiment and document in `docs/latency.md`: e.g. measuring
receive-time spread across redundant connections (the challenge's "+18 ms vs
+7 ms" picture), trying multiple concurrent connects and keeping the fastest,
source-port variation, and connecting promptly. Correctness stays priority #1;
this is measurement + a writeup, not a core dependency.

## 13. Error handling & resilience

- Every socket runs in its own task with automatic reconnect + exponential
  backoff; on reconnect it resubscribes its markets (⇒ fresh snapshots).
- A panic in one task must not take down the service; tasks are supervised and
  restarted.
- Malformed/unknown frames are logged (rate-limited) and skipped, never fatal.
- Clock skew / auth: timestamp in **ms** (verified); a signed-path or unit error
  shows as 401 at connect — surfaced clearly, not retried blindly.

## 14. Testing strategy

- **Unit:** the Kalshi decode + YES/NO transform (fixtures captured from the
  spike), fixed-point conversion/rounding, book apply (delta → absolute, deletion
  at 0, re-add), `seq` gap detection.
- **Round-trip:** write snapshots/deltas via `AlignedSbeWriter`, read back with
  `AlignedSbeReader`, assert equality (as the template's `main.rs` demonstrates).
- **Book-reconstruction invariant:** replaying our own emitted deltas from a
  snapshot reproduces the live book state.
- **Integration (live, opt-in behind env/credentials):** connect to a handful of
  real markets for a short window, assert snapshot→delta ordering, no `seq` gaps
  on a stable socket, and that a forced resubscribe yields a fresh snapshot.
- TDD where it pays (pure logic); live paths validated with short real runs.

## 15. What's left to the builder

- Concrete module layout under `src/` and task/channel topology (actor-style with
  `tokio` mpsc is the intended shape).
- Exact trait method signatures and the normalized-event enum (names in §8 are
  indicative).
- Tuning constants: markets-per-connection, redundancy factor, stall/quiet
  thresholds, discovery interval, socket-recycle interval, backoff schedule —
  pick sane defaults, make them configurable (consts or a small config struct).
- The precise latency experiments to run and report in Ext 4.
- Logging approach (structured logs to stderr; stdout stays clean — see O1).

## 16. Decisions (resolved with the requester 2026-10-28)

D1. **Output destination → stdout.** Follow the spec literally: write the SBE
stream to **stdout** via `AlignedSbeWriter` (`/dev/stdout`, 64 MiB chunks to stay
reader-compatible); logs to stderr. No file-path assumption. (See §9.)

D2. **Socket scale → capped for the live run.** ~200 markets/socket, 2 connections
per market (1 primary + 1 backup), with a configurable cap on total
markets/connections so the live demo is stable and friendly to Kalshi's limits.
The architecture supports lifting the cap later. (See §11.)

D3. **Output stream granularity → single stdout stream per run** (no file
rotation; persistence is the user's `>` redirect). Builder's call, recorded here.
