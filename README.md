# Kalshi Order-Book Collector

A Rust service that connects to **Kalshi** over redundant authenticated
WebSockets, maintains a live order book for every regular (non-MVE) market, and
streams complete, time-accurate order-book **snapshots + deltas** to stdout in
the challenge's SBE binary format.

Built for the Voluntas engineering challenge, including all four extensions.

## Quick start

```bash
# credentials (Ed25519 API key created at kalshi.com/account/profile)
export KALSHI_KEY_ID="<your key id>"
export KALSHI_KEY_PATH="/path/to/your/private_key.pem"   # PKCS#8 PEM, never printed

# run: SBE binary on stdout, logs on stderr
cargo run --release > capture.sbe 2> run.log
# ... let it run, then Ctrl-C for a clean, flushed shutdown
```

By default it subscribes to **all** regular (non-MVE) markets. Kalshi currently
lists ~10k regular markets (amid ~200k auto-generated MVE markets), so a full run
opens many sockets and discovery takes a few minutes; set `MAX_MARKETS` to bound
it for a quick run.

The **same binary** runs against **Polymarket** — it's the only thing that
changes:

```bash
EXCHANGE=polymarket cargo run --release > capture.sbe 2> run.log   # no API key needed
```

Tuning (env vars, all optional):

| var | default | meaning |
|-----|---------|---------|
| `EXCHANGE` | `kalshi` | `kalshi` or `polymarket` |
| `MAX_MARKETS` | 0 | cap on markets subscribed; **0 = unlimited (all)** |
| `PER_CONN` | 150 | markets per WebSocket connection |
| `REDUNDANCY` | 2 | connections per market (1 primary + N-1 backups) |
| `SNAPSHOT_SECS` | 60 | full-snapshot cadence |
| `DISCOVERY_SECS` | 60 | how often to re-scan for new/closed markets |
| `STALL_SECS` | 15 | no-message timeout before switching source |
| `QUIET_SECS` | 45 | per-symbol silence before resubscribe |
| `RECYCLE_SECS` | 600 | whole-socket recycle interval |
| `KALSHI_ENV` | prod | set to `demo` for the demo environment (Kalshi only) |

## Output format

Output goes to **stdout** via the provided `AlignedSbeWriter` (64 MiB aligned
chunks), so it reads back with the template's `AlignedSbeReader`. Logs go to
**stderr** only, keeping stdout a clean binary stream. Redirect stdout to persist
(`> capture.sbe`).

- `SnapshotDomain` — full absolute book per symbol (emitted on subscribe, once
  per minute, and after every source switch / resync).
- `PriceLevelUpdateDomain` — one price level's new **absolute** quantity
  (`quantity = 0` deletes the level).

Prices/quantities are fixed-point `i64` (`1_000_000 = 1 unit`). `Side::Buy` = bid,
`Side::Sell` = ask.

## How Kalshi maps to a standard book

Kalshi quotes each binary market as YES bids and NO bids — both are bids. We
publish one two-sided book per market, referenced to the YES contract:

- a **YES** bid at price `p` → **bid** at `p`
- a **NO** bid at price `q` → **ask** at `1 − q` (same size)

Kalshi deltas are *signed changes* (`delta_fp`), not absolute sizes, so the
collector maintains each book in memory and emits the resulting absolute
quantity.

## Architecture

```
            Kalshi adapter (src/codec/kalshi.rs)
   auth signing · REST discovery · WS decode (YES/NO → bid/ask)
                         │ NormalizedEvent
        Generic collector core (src/collector.rs)
  connections · per-symbol books · source selection · gap/stall/quiet
  monitoring · minute snapshots · socket recycle · symbol lifecycle
                         │ SnapshotDomain / PriceLevelUpdateDomain
             Output writer (src/output.rs) → stdout
```

The only exchange-specific code is the `ExchangeCodec` adapter; everything else
is generic. **Two adapters ship** — Kalshi and Polymarket — and they exercise
genuinely different exchange models: Kalshi sends signed *change* deltas with a
per-connection sequence; Polymarket sends *absolute* level sizes with none. The
`NormalizedEvent::Delta` carries an `Amount::{Relative,Absolute}` so the shared
book handles both.

| module | responsibility |
|--------|----------------|
| `fixedpoint.rs` | decimal-string ↔ fixed-point |
| `model.rs` | normalized events |
| `book.rs` | in-memory order book (delta → absolute) |
| `codec/` | `ExchangeCodec` trait + Kalshi and Polymarket adapters |
| `lifecycle.rs` | symbol discovery parse + add/remove diff |
| `monitor.rs` | per-connection seq-gap + per-symbol rate tracking |
| `manager_logic.rs` | connection sharding + source selection |
| `emit.rs` | build SBE domain messages |
| `output.rs` | the single SBE writer sink |
| `connection.rs` | one resilient WS connection (reconnect, commands) |
| `collector.rs` | orchestration of all of the above |

## Challenge requirements → where they live

- **Subscribe to all live regular symbols, keep in sync** — `collector::run_discovery`, `lifecycle`
- **Maintain a book per symbol, deltas + minute snapshots via the helpers** — `book`, `emit`, `output`, `collector`
- **Load-balance across multiple sockets** — `manager_logic::shard`, `collector`
- **Clean SIGINT shutdown + flush** — `main.rs`, `output::spawn_writer`
- **Ext 1 Redundant sockets** — primary+backup per market, seq-gap/stall detection, source switch + fresh snapshot (`collector`)
- **Ext 2 Subscription monitoring** — per-symbol quiet detection + resubscribe, periodic whole-socket recycle (`collector`, `monitor`)
- **Ext 3 Generic core** — `ExchangeCodec` is the only seam; **Kalshi and Polymarket** adapters both ship and run on the same core (`codec/kalshi.rs`, `codec/polymarket.rs`)
- **Ext 4 Latency** — `docs/latency.md` (measured receive-time spread across connections)

## Tests

```bash
cargo test                                   # 34 unit tests (pure logic + SBE round-trip)
cargo test --test e2e -- --ignored --nocapture   # verify a captured capture.sbe
```

The e2e verifier reads a live capture back with `AlignedSbeReader` and asserts
completeness invariants (no delta before its symbol's snapshot; minute snapshots
present).

## Design & plan

- **`docs/PROJECT_WRITEUP.md`** — the full narrative: every major decision,
  the trade-offs considered, what the live spikes revealed, and why the
  architecture looks the way it does. Start here for the "why."
- `docs/latency.md` — Extension 4 findings.
- `docs/specs/` (design doc) and `docs/plans/` (implementation plan) — the
  formal spec and task breakdown this was built from.
