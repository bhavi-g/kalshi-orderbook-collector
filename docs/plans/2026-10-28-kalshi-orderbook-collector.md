# Kalshi Order-Book Collector Implementation Plan

Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Rust service that maintains a live order book for every regular (non-MVE) Kalshi market and streams complete, time-accurate SBE snapshots + deltas to stdout.

**Architecture:** An exchange-agnostic collector core (connection manager with redundant sockets, per-symbol order-book store, symbol lifecycle, rate/gap monitor, minute-snapshotter, SIGINT shutdown) behind one `ExchangeCodec` seam. The Kalshi adapter owns auth signing, REST discovery, and WS decode — including the YES/NO → bid/ask transform, so the core only ever sees normalized fixed-point bid/ask events.

**Tech Stack:** Rust 2024; `tokio`, `tokio-tungstenite` (rustls), `reqwest` (rustls), `ed25519-dalek`, `serde_json`, `chrono`, `anyhow`; the template's SBE helpers (`AlignedSbeWriter`, `AlignedSbeReader`, `SnapshotDomain`, `PriceLevelUpdateDomain`, `Side`).

**Spec:** `docs/specs/2026-10-28-kalshi-orderbook-collector-design.md`

## Global Constraints

- Rust only. No other languages or services.
- Output is written **only** through the template's `AlignedSbeWriter`, to **stdout** (`/dev/stdout`), with chunk size **`64 << 20` (64 MiB)** to stay readable by the template's `AlignedSbeReader`. Logs go to **stderr only** — stdout stays a clean binary stream.
- Do **not** edit `src/util/**`, `src/side.rs`, `src/lib.rs`, or the SBE schema/YAML — they are provided and generate the required format.
- SBE fixed-point: price & quantity are `i64`, `1_000_000 = 1 unit` (1 USDC / 1 contract).
- `1.0` in price fixed-point is `1_000_000`; a Kalshi NO level at price `q` maps to a YES ask at fixed-point `1_000_000 - q_fp`.
- Timestamps are `chrono::DateTime<Utc>`.
- Credentials from env: `KALSHI_KEY_ID`, `KALSHI_KEY_PATH` (PKCS#8 PEM, Ed25519). Never log or transmit the key except in the TLS handshake.
- Kalshi production: WS `wss://api.elections.kalshi.com/trade-api/ws/v2`; REST `https://api.elections.kalshi.com`; signed WS path `/trade-api/ws/v2`. Auth headers `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (unix **ms**), `KALSHI-ACCESS-SIGNATURE` = base64(Ed25519.sign(`"{ts}{METHOD}{path}"`)); REST path signed without query string.
- MVE filter: a regular market is any open market whose ticker does **not** start with `KXMVE`.

## Review Focus

- **Delta before snapshot** (delta for a symbol/level with no prior snapshot): the book must create the level from the change rather than drop or panic. → Task 3.
- **`seq` gap on the source connection** (a jump in per-sid sequence): must be detected so the core can resync + emit a fresh snapshot, never silently continue. → Task 7 (detection), Task 11 (resync action).
- **Level emptied then re-added** (qty → 0 delete, then a later add at the same price): emit `quantity=0`, then the new absolute quantity on re-add. → Task 3.
- **Fixed-point boundary & fine ticks** (NO price `0.9900` → ask `0.0100`; `0.0001` steps; fractional sizes like `0.25`): conversion must round correctly with no drift. → Task 2, Task 5.
- **SIGINT mid-chunk** (shutdown with a partially filled 64 MiB buffer): the final chunk must be flushed so no buffered messages are lost. → Task 9.

---

### Task 1: Project setup & baseline

**Files:**
- Create (seed): copy the challenge template into the repo root (`Cargo.toml`, `Cargo.lock`, `src/`, `sbe_schemas/`, `.vscode/`, `.gitignore`).
- Modify: `Cargo.toml` (add runtime deps).
- Create: `.gitignore` entry for `/target` and `/out` (keep existing).

**Interfaces:**
- Produces: a building cargo project named `engineering-challenge` with the SBE helpers intact and all runtime deps resolved.

- [ ] **Step 1:** Copy the template contents from the cloned template into the repo root (do not overwrite `docs/`). Verify `src/util/`, `src/side.rs`, `src/lib.rs`, `sbe_schemas/` are present and unchanged.
- [ ] **Step 2:** `git init` (local checkpoints only — no push), add a `.gitignore` covering `/target` and `/out`, and make an initial commit of the seeded template.
- [ ] **Step 3:** Add to `Cargo.toml` `[dependencies]`: `tokio = { version = "1.40", features = ["full"] }`, `tokio-tungstenite = { version = "0.24", features = ["rustls-tls-webpki-roots"] }`, `futures-util = "0.3"`, `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls","json"] }`, `ed25519-dalek = { version = "2.1", features = ["pkcs8","pem"] }`, `base64 = "0.22"`, `serde_json = "1"`. (`anyhow`, `chrono`, `serde`, `crossbeam`, `tempfile` already present.)
- [ ] **Step 4:** Run `cargo build` and `cargo run` (the template's demo `main`). Expected: build succeeds; prints `Verification success!`.
- [ ] **Step 5:** Commit (`chore: seed template and add runtime dependencies`).

---

### Task 2: Fixed-point decimal conversion

**Files:**
- Create: `src/fixedpoint.rs`
- Modify: `src/lib.rs`? No — add `mod fixedpoint;` to a new `src/collector` tree later; for now declare modules in `main.rs`. Declare `mod fixedpoint;` in `src/main.rs`.
- Test: inline `#[cfg(test)]` in `src/fixedpoint.rs`.

**Interfaces:**
- Produces:
  - `pub const ONE: i64 = 1_000_000;`
  - `pub fn decimal_to_fp(s: &str) -> anyhow::Result<i64>` — parse a decimal string to fixed-point `round(x * 1_000_000)`; supports leading `-`.
  - `pub fn ask_price_fp(no_price_fp: i64) -> i64` — returns `ONE - no_price_fp`.

- [ ] **Step 1: Write failing tests** in `src/fixedpoint.rs`:
```rust
#[test] fn parses_cents() { assert_eq!(decimal_to_fp("0.5900").unwrap(), 590_000); }
#[test] fn parses_size_fraction() { assert_eq!(decimal_to_fp("798.00").unwrap(), 798_000_000); }
#[test] fn parses_negative_change() { assert_eq!(decimal_to_fp("-1.00").unwrap(), -1_000_000); }
#[test] fn parses_quarter() { assert_eq!(decimal_to_fp("0.25").unwrap(), 250_000); }
#[test] fn parses_fine_tick() { assert_eq!(decimal_to_fp("0.0001").unwrap(), 100); }
#[test] fn ask_from_no_boundary() { assert_eq!(ask_price_fp(decimal_to_fp("0.9900").unwrap()), 10_000); }
```
- [ ] **Step 2:** Run `cargo test fixedpoint` — expect FAIL (undefined).
- [ ] **Step 3:** Implement `decimal_to_fp` (parse as `f64` then `(x * 1e6).round() as i64`, or split on `.` for exactness — implementer's call; rounding must match the asserts) and `ask_price_fp`.
- [ ] **Step 4:** Run `cargo test fixedpoint` — expect PASS.
- [ ] **Step 5:** Commit (`feat: fixed-point decimal conversion`).

---

### Task 3: Normalized model + in-memory order book

**Files:**
- Create: `src/model.rs` (types, no logic), `src/book.rs` (`OrderBook`).
- Modify: `src/main.rs` add `mod model; mod book;`.
- Test: inline tests in `src/book.rs`.

**Interfaces:**
- Produces (`model.rs`):
```rust
use chrono::{DateTime, Utc};
use crate::Side; // from template
pub type Fp = i64;
pub enum NormalizedEvent {
    Snapshot { symbol: String, bids: Vec<(Fp,Fp)>, asks: Vec<(Fp,Fp)>,
               exch_ts: DateTime<Utc>, recv_at: DateTime<Utc>, seq: u64, sid: u64 },
    Delta    { symbol: String, side: Side, price_level: Fp, change: Fp,
               exch_ts: DateTime<Utc>, recv_at: DateTime<Utc>, seq: u64, sid: u64 },
    SubscribeAck { sid: u64, id: u64 },
    Other,
}
```
- Produces (`book.rs`):
```rust
pub struct OrderBook { /* bids: BTreeMap<Fp,Fp>, asks: BTreeMap<Fp,Fp> */ }
impl OrderBook {
    pub fn new() -> Self;
    pub fn apply_snapshot(&mut self, bids: &[(Fp,Fp)], asks: &[(Fp,Fp)]);
    /// Apply a signed change; return the resulting absolute qty (>=0) at that level.
    pub fn apply_delta(&mut self, side: Side, price: Fp, change: Fp) -> Fp;
    pub fn levels(&self) -> (Vec<(Fp,Fp)>, Vec<(Fp,Fp)>); // (bids, asks) ascending price
}
```

- [ ] **Step 1: Write failing tests** in `src/book.rs`:
```rust
#[test] fn delta_on_empty_creates_level() {
    let mut b = OrderBook::new();
    assert_eq!(b.apply_delta(Side::Buy, 590_000, 5_000_000), 5_000_000);
}
#[test] fn delta_accumulates_then_deletes() {
    let mut b = OrderBook::new();
    b.apply_delta(Side::Sell, 700_000, 10_000_000);
    assert_eq!(b.apply_delta(Side::Sell, 700_000, -10_000_000), 0);
    let (_, asks) = b.levels();
    assert!(asks.iter().all(|(p,_)| *p != 700_000)); // level removed at 0
}
#[test] fn readd_after_delete() {
    let mut b = OrderBook::new();
    b.apply_delta(Side::Buy, 100, 1_000_000);
    b.apply_delta(Side::Buy, 100, -1_000_000);
    assert_eq!(b.apply_delta(Side::Buy, 100, 2_000_000), 2_000_000);
}
#[test] fn negative_result_clamps_to_zero() {
    let mut b = OrderBook::new();
    assert_eq!(b.apply_delta(Side::Buy, 100, -5_000_000), 0);
}
#[test] fn snapshot_replaces_state() {
    let mut b = OrderBook::new();
    b.apply_delta(Side::Buy, 100, 9);
    b.apply_snapshot(&[(200, 3_000_000)], &[(800, 1_000_000)]);
    let (bids, asks) = b.levels();
    assert_eq!(bids, vec![(200, 3_000_000)]);
    assert_eq!(asks, vec![(800, 1_000_000)]);
}
```
- [ ] **Step 2:** Run `cargo test book` — expect FAIL.
- [ ] **Step 3:** Implement `OrderBook` with `BTreeMap<Fp,Fp>` per side; `apply_delta` adds `change`, clamps at 0, removes the entry when 0, returns the new value; `apply_snapshot` clears and reloads; `levels` returns sorted vecs.
- [ ] **Step 4:** Run `cargo test book` — expect PASS.
- [ ] **Step 5:** Commit (`feat: normalized event model and in-memory order book`).

---

### Task 4: Kalshi auth signing

**Files:**
- Create: `src/codec/mod.rs` (declare `pub mod kalshi;` + the `ExchangeCodec` trait), `src/codec/kalshi.rs` (signing portion).
- Modify: `src/main.rs` add `mod codec;`.
- Test: inline tests in `src/codec/kalshi.rs`.

**Interfaces:**
- Produces (`codec/mod.rs`):
```rust
pub trait ExchangeCodec: Send + Sync + 'static {
    fn ws_url(&self) -> &str;
    fn ws_path(&self) -> &str;
    fn auth_headers(&self, method: &str, path: &str) -> anyhow::Result<Vec<(String,String)>>;
    async fn list_symbols(&self) -> anyhow::Result<Vec<String>>;
    fn subscribe_frame(&self, id: u64, markets: &[String]) -> String;
    fn update_add_frame(&self, id: u64, sid: u64, markets: &[String]) -> String;
    fn update_remove_frame(&self, id: u64, sid: u64, markets: &[String]) -> String;
    fn decode(&self, raw: &str, recv_at: chrono::DateTime<chrono::Utc>) -> anyhow::Result<Vec<crate::model::NormalizedEvent>>;
}
```
- Produces (`codec/kalshi.rs`):
```rust
pub struct KalshiCodec { /* key_id, signing_key: SigningKey, http, ws_url, rest_base */ }
impl KalshiCodec {
    pub fn new(key_id: String, key_pem: &str, production: bool) -> anyhow::Result<Self>;
    pub(crate) fn sign(&self, ts_ms: i64, method: &str, path: &str) -> String; // base64 sig
}
```

- [ ] **Step 1: Write failing tests** in `src/codec/kalshi.rs` (generate an ephemeral key in-test so the signature is verifiable):
```rust
#[test] fn signature_verifies_with_public_key() {
    // build signing key, PEM-encode, construct KalshiCodec::new, sign a known string,
    // base64-decode, verify with the corresponding VerifyingKey over "1700000000000GET/trade-api/ws/v2"
}
#[test] fn auth_headers_present_and_ms_timestamp() {
    // headers include KALSHI-ACCESS-KEY == key_id, a KALSHI-ACCESS-TIMESTAMP parseable as i64 ms (>= 1e12),
    // and a non-empty KALSHI-ACCESS-SIGNATURE
}
```
- [ ] **Step 2:** Run `cargo test kalshi::` — expect FAIL.
- [ ] **Step 3:** Implement `new` (parse PKCS#8 PEM via `SigningKey::from_pkcs8_pem`, pick prod/demo URLs), `sign` (message `"{ts}{method}{path}"`, Ed25519 sign, base64 STANDARD), and `auth_headers` (ts = `Utc::now().timestamp_millis()`).
- [ ] **Step 4:** Run `cargo test kalshi::` — expect PASS.
- [ ] **Step 5:** Commit (`feat: Kalshi Ed25519 auth signing`).

---

### Task 5: Kalshi decode (YES/NO → bid/ask) + subscription frames

**Files:**
- Modify: `src/codec/kalshi.rs` (implement `decode`, `subscribe_frame`, `update_*_frame`).
- Create: `tests/fixtures/kalshi_snapshot.json`, `tests/fixtures/kalshi_delta.json` (captured from the spike; raw single-message JSON).
- Test: inline tests in `src/codec/kalshi.rs` loading the fixtures.

**Interfaces:**
- Consumes: `NormalizedEvent` (Task 3), `decimal_to_fp` / `ask_price_fp` (Task 2), `Side` (template).
- Produces: `decode` returns `Vec<NormalizedEvent>`; frame builders return JSON strings.

- [ ] **Step 1: Capture fixtures** — save one real `orderbook_snapshot` and one `orderbook_delta` line (from the spike output) into the fixture files.
- [ ] **Step 2: Write failing tests**:
```rust
#[test] fn decodes_snapshot_yes_bid_no_ask() {
    // decode fixture; expect one Snapshot; a yes level ["0.5900","798.00"] appears in bids as (590_000, 798_000_000);
    // a no level ["0.3000","703.00"] appears in asks as (700_000, 703_000_000); seq and exch_ts set from sending_ts_ms
}
#[test] fn decodes_delta_no_side_to_ask_change() {
    // delta side "no", price_dollars "0.5900", delta_fp "-1.00"
    // => Delta{ side: Sell, price_level: 410_000, change: -1_000_000 }, exch_ts from ts_ms
}
#[test] fn subscribe_frame_shape() {
    // JSON has cmd=="subscribe", params.channels==["orderbook_delta"], params.market_tickers==markets
}
#[test] fn update_add_remove_frames() {
    // cmd=="update_subscription", action "add_markets"/"delete_markets", sid set
}
```
- [ ] **Step 3:** Run `cargo test kalshi::` — expect FAIL.
- [ ] **Step 4:** Implement `decode`: match on `type`; for `orderbook_snapshot` map `yes_dollars_fp`→bids `(decimal_to_fp(price), decimal_to_fp(size))` and `no_dollars_fp`→asks `(ask_price_fp(decimal_to_fp(price)), decimal_to_fp(size))`, `exch_ts` from `sending_ts_ms`; for `orderbook_delta` map side `"yes"`→`Buy`/price as-is, `"no"`→`Sell`/`ask_price_fp`, `change = decimal_to_fp(delta_fp)`, `exch_ts` from `ts_ms`; `subscribed`→`SubscribeAck`; else `Other`. Implement the three frame builders with `serde_json::json!`.
- [ ] **Step 5:** Run `cargo test kalshi::` — expect PASS.
- [ ] **Step 6:** Commit (`feat: Kalshi WS decode with YES/NO transform and subscription frames`).

---

### Task 6: REST symbol discovery parse + lifecycle diff

**Files:**
- Create: `src/lifecycle.rs`.
- Modify: `src/codec/kalshi.rs` implement `list_symbols` (paginated REST, thin HTTP + reuse the parser), `src/main.rs` add `mod lifecycle;`.
- Create: `tests/fixtures/kalshi_markets_page.json`.
- Test: inline tests in `src/lifecycle.rs`.

**Interfaces:**
- Produces (`lifecycle.rs`):
```rust
pub fn parse_market_tickers(page_json: &serde_json::Value) -> (Vec<String>, Option<String>); // (tickers, next_cursor)
pub fn filter_regular(tickers: impl IntoIterator<Item=String>) -> Vec<String>; // drop KXMVE*
pub fn diff(current: &std::collections::HashSet<String>, target: &std::collections::HashSet<String>)
    -> (Vec<String> /*add*/, Vec<String> /*remove*/);
```
- Consumes: used by `KalshiCodec::list_symbols` (pages REST with `auth_headers`, accumulates via `parse_market_tickers`, then `filter_regular`).

- [ ] **Step 1: Capture fixture** — one `/markets` page (with `markets[]` and `cursor`).
- [ ] **Step 2: Write failing tests**:
```rust
#[test] fn parse_extracts_tickers_and_cursor() { /* from fixture */ }
#[test] fn filter_drops_mve() {
    assert_eq!(filter_regular(["KXMVEX-1".into(),"KXNBA-GSW".into()]), vec!["KXNBA-GSW".to_string()]);
}
#[test] fn diff_adds_and_removes() {
    // current {A,B}, target {B,C} => add [C], remove [A]
}
```
- [ ] **Step 3:** Run `cargo test lifecycle` — expect FAIL.
- [ ] **Step 4:** Implement the three functions; implement `KalshiCodec::list_symbols` to loop pages (`?limit=1000&status=open&cursor=…`, signing path `/trade-api/v2/markets`), collect, `filter_regular`, de-dup.
- [ ] **Step 5:** Run `cargo test lifecycle` — expect PASS.
- [ ] **Step 6:** Commit (`feat: REST symbol discovery and lifecycle diff`).

---

### Task 7: Monitor — seq-gap & rate tracking

**Files:**
- Create: `src/monitor.rs`
- Modify: `src/main.rs` add `mod monitor;`.
- Test: inline tests.

**Interfaces:**
- Produces:
```rust
pub enum SeqStatus { First, Ok, Gap { missing: u64 } }
pub struct SeqTracker { /* last: Option<u64> */ }
impl SeqTracker { pub fn new() -> Self; pub fn check(&mut self, seq: u64) -> SeqStatus; }

pub struct RateTracker { /* window counts per key */ }
impl RateTracker {
    pub fn new(window: std::time::Duration) -> Self;
    pub fn record(&mut self, key: &str, now: std::time::Instant);
    pub fn rate_per_sec(&self, key: &str, now: std::time::Instant) -> f64;
}
```

- [ ] **Step 1: Write failing tests**:
```rust
#[test] fn seq_first_then_ok() { let mut t=SeqTracker::new();
    assert!(matches!(t.check(5), SeqStatus::First)); assert!(matches!(t.check(6), SeqStatus::Ok)); }
#[test] fn seq_detects_gap() { let mut t=SeqTracker::new(); t.check(1);
    assert!(matches!(t.check(4), SeqStatus::Gap{missing:2})); }
#[test] fn rate_counts_in_window() { /* record 3 events at t0; rate_per_sec > 0; after window+, decays to 0 */ }
```
- [ ] **Step 2:** Run `cargo test monitor` — expect FAIL.
- [ ] **Step 3:** Implement `SeqTracker` (gap = `seq - last - 1` when `seq > last+1`) and `RateTracker` (ring/deque of timestamps per key, prune outside window).
- [ ] **Step 4:** Run `cargo test monitor` — expect PASS.
- [ ] **Step 5:** Commit (`feat: sequence-gap and update-rate monitoring`).

---

### Task 8: Connection load-balancing & source selection

**Files:**
- Create: `src/manager_logic.rs` (pure decision functions; the IO manager in Task 11 uses these).
- Modify: `src/main.rs` add `mod manager_logic;`.
- Test: inline tests.

**Interfaces:**
- Produces:
```rust
pub fn shard(symbols: &[String], per_conn: usize) -> Vec<Vec<String>>;
#[derive(Clone)] pub struct ConnHealth { pub id: usize, pub healthy: bool, pub last_msg_age_ms: u64 }
pub fn choose_source(candidates: &[ConnHealth]) -> Option<usize>; // healthy, lowest last_msg_age
```

- [ ] **Step 1: Write failing tests**:
```rust
#[test] fn shard_splits_evenly() { assert_eq!(shard(&v(5), 2).len(), 3); } // 2+2+1
#[test] fn shard_never_exceeds_cap() { for g in shard(&v(201),200){ assert!(g.len()<=200);} }
#[test] fn source_prefers_healthy_freshest() {
    // choose_source([{0,true,50},{1,true,5},{2,false,1}]) == Some(1)
}
#[test] fn source_none_when_all_unhealthy() { assert_eq!(choose_source(&[/*all healthy=false*/]), None); }
```
- [ ] **Step 2:** Run `cargo test manager_logic` — expect FAIL.
- [ ] **Step 3:** Implement `shard` (chunk) and `choose_source` (filter healthy, min `last_msg_age_ms`).
- [ ] **Step 4:** Run `cargo test manager_logic` — expect PASS.
- [ ] **Step 5:** Commit (`feat: shard and source-selection logic`).

---

### Task 9: SBE emit builders + output writer task

**Files:**
- Create: `src/emit.rs` (domain builders), `src/output.rs` (writer task).
- Modify: `src/main.rs` add `mod emit; mod output;`.
- Test: inline tests in `src/output.rs` using `tempfile` + `AlignedSbeReader`.

**Interfaces:**
- Consumes: `SnapshotDomain`, `PriceLevelUpdateDomain`, `AlignedSbeWriter`, `AlignedSbeReader`, `OrderbookSchema`, `AnyMessage` (template); `Side`.
- Produces (`emit.rs`):
```rust
pub fn build_snapshot(symbol: &str, bids: &[(i64,i64)], asks: &[(i64,i64)],
    exch_ts: DateTime<Utc>, recv_at: DateTime<Utc>) -> SnapshotDomain;
pub fn build_update(symbol: &str, side: Side, price_level: i64, abs_qty: i64,
    exch_ts: DateTime<Utc>, recv_at: DateTime<Utc>) -> PriceLevelUpdateDomain;
```
- Produces (`output.rs`):
```rust
pub enum OutMsg { Snapshot(SnapshotDomain), Delta(PriceLevelUpdateDomain) }
/// Owns an AlignedSbeWriter over `path` (64 MiB chunks); drains rx; flushes on channel close.
pub fn spawn_writer(path: std::path::PathBuf, rx: tokio::sync::mpsc::Receiver<OutMsg>) -> tokio::task::JoinHandle<anyhow::Result<()>>;
```

- [ ] **Step 1: Write failing tests** in `src/output.rs`:
```rust
#[tokio::test] async fn roundtrip_through_writer() {
    // temp path; spawn_writer; send one Snapshot + one Delta built via emit::*; drop sender; await handle;
    // open AlignedSbeReader on the file; assert first is Snapshot with matching symbol/levels, second is PriceLevelUpdate;
    // this also proves the final partial chunk is flushed on channel close (Review Focus: SIGINT mid-chunk)
}
```
And in `emit.rs`:
```rust
#[test] fn build_update_maps_fields() {
    let u = build_update("EX", Side::Sell, 410_000, 702_000_000, ts(), ts());
    assert_eq!((u.price_level, u.quantity, u.side), (410_000, 702_000_000, Side::Sell));
}
```
- [ ] **Step 2:** Run `cargo test output emit` — expect FAIL.
- [ ] **Step 3:** Implement `emit::*` (map fields, bids→`SnapshotBidsEntryDomain`, asks→`SnapshotAsksEntryDomain`) and `output::spawn_writer` (blocking writer on a dedicated task via `spawn_blocking` or a std thread fed by the channel; `AlignedSbeWriter::create(path, 64<<20)`; `write_msg` per `OutMsg`; on `rx` close, `flush()` then drop).
- [ ] **Step 4:** Run `cargo test output emit` — expect PASS.
- [ ] **Step 5:** Commit (`feat: SBE emit builders and stdout writer task with flush-on-close`).

---

### Task 10: WS connection task (live smoke)

**Files:**
- Create: `src/connection.rs`
- Modify: `src/main.rs` add `mod connection;`.
- Test: a `#[tokio::test] #[ignore]` live smoke test gated on env credentials.

**Interfaces:**
- Consumes: `ExchangeCodec` (Arc), outputs `NormalizedEvent` on an mpsc channel.
- Produces:
```rust
pub struct ConnConfig { pub id: usize, pub markets: Vec<String> }
/// Connects, authenticates, subscribes to `markets`, reads frames, decodes, forwards events.
/// Reconnects with exponential backoff; on (re)connect it (re)subscribes. Sends a ping on an interval.
pub fn spawn_connection<C: ExchangeCodec>(codec: std::sync::Arc<C>, cfg: ConnConfig,
    tx: tokio::sync::mpsc::Sender<(usize, NormalizedEvent)>,
    shutdown: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()>;
```
(Shutdown is a `tokio::sync::watch::Receiver<bool>`, shared by all tasks and flipped by the SIGINT handler in Task 12.)

- [ ] **Step 1: Write the live smoke test** `#[ignore]`: build `KalshiCodec` from env, `list_symbols`, take 3 regular tickers, `spawn_connection`, assert within 10 s we receive ≥1 `Snapshot` and the connection stays up.
- [ ] **Step 2:** Run `cargo test --  --ignored connection_smoke` with creds exported — expect FAIL (unimplemented).
- [ ] **Step 3:** Implement `spawn_connection`: handshake with `codec.auth_headers(codec.ws_path())` headers via `IntoClientRequest`; `connect_async`; send `codec.subscribe_frame`; loop `ws.next()` → on text `codec.decode(raw, Utc::now())` → forward each event tagged with `cfg.id`; respond to `Ping`; periodic client ping; on error/close, backoff and reconnect (resubscribe). Honor shutdown.
- [ ] **Step 4:** Run the ignored smoke test with creds — expect PASS (observed live). Record the run output in the commit message.
- [ ] **Step 5:** Commit (`feat: resilient WS connection task`).

---

### Task 11: Collector core — redundancy, resync, lifecycle, snapshotter

**Files:**
- Create: `src/collector.rs`
- Modify: `src/main.rs` add `mod collector;`.
- Test: inline unit tests for the pure routing/resync decisions; behavior otherwise validated in Task 13.

**Interfaces:**
- Consumes: `OrderBook`, `SeqTracker`, `RateTracker`, `manager_logic::*`, `emit::*`, `OutMsg`, `NormalizedEvent`, `ExchangeCodec`.
- Produces:
```rust
pub struct CollectorConfig { pub per_conn: usize, pub redundancy: usize, pub max_markets: usize,
    pub discovery_interval: Duration, pub snapshot_interval: Duration, pub stall_timeout: Duration,
    pub quiet_ratio: f64, pub socket_recycle_interval: Duration }
pub async fn run_collector<C: ExchangeCodec>(codec: Arc<C>, cfg: CollectorConfig,
    out: Sender<OutMsg>, shutdown: watch::Receiver<bool>) -> anyhow::Result<()>;
/// pure helper, unit-tested:
pub fn should_resync(status: SeqStatus, is_source: bool) -> bool;
```

- [ ] **Step 1: Write failing tests** for the pure decisions:
```rust
#[test] fn resync_on_gap_from_source() { assert!(should_resync(SeqStatus::Gap{missing:2}, true)); }
#[test] fn no_resync_on_ok() { assert!(!should_resync(SeqStatus::Ok, true)); }
#[test] fn no_resync_from_nonsource() { assert!(!should_resync(SeqStatus::Gap{missing:9}, false)); }
```
- [ ] **Step 2:** Run `cargo test collector` — expect FAIL.
- [ ] **Step 3:** Implement `run_collector`:
  - Discover symbols, cap to `max_markets`, `shard` into primary groups, and create `redundancy` connections per group (Task 10), each on its own `SeqTracker` and per-(conn,symbol) `OrderBook`.
  - Receive `(conn_id, event)`; update that connection's book + trackers; if the connection is the **source** for that symbol, translate to `OutMsg` (snapshot → `build_snapshot`; delta → `apply_delta` then `build_update` with the returned absolute qty) and send to `out`.
  - On `should_resync` true for the source (seq gap/stall), switch the symbol's source via `choose_source`, then emit a fresh `build_snapshot` from the new source's book before resuming (Review Focus: seq gap).
  - **Minute snapshotter:** every `snapshot_interval`, walk each symbol's source book and emit `build_snapshot`.
  - **Monitor (Ext 2):** periodically compare each symbol's source rate vs its backup (`quiet_ratio`); if too quiet, resubscribe that symbol (forces snapshot) or switch source.
  - **Socket recycle (Ext 2):** every `socket_recycle_interval`, stand up a replacement connection for a group, confirm healthy, then retire the old one.
  - **Lifecycle (Task 6):** every `discovery_interval`, `diff` target vs current; add/remove via `update_*_frame` on the least-loaded connection; drop removed books.
- [ ] **Step 4:** Run `cargo test collector` — expect PASS.
- [ ] **Step 5:** Commit (`feat: collector core with redundancy, resync, monitoring, lifecycle`).

---

### Task 12: `main.rs` — config, wiring, graceful shutdown

**Files:**
- Modify: `src/main.rs` (replace the template demo `main`).
- Test: none beyond compile; exercised live in Task 13.

**Interfaces:**
- Consumes: everything above.
- Produces: a binary that reads env config, starts the collector against `/dev/stdout`, and shuts down cleanly on SIGINT.

- [ ] **Step 1:** Implement `main`: read `KALSHI_KEY_ID`, `KALSHI_KEY_PATH` (read PEM file), optional tuning env (`MAX_MARKETS`, `PER_CONN`, `REDUNDANCY`, intervals) with the spec defaults; build `KalshiCodec`; create the `OutMsg` channel and `spawn_writer(PathBuf::from("/dev/stdout"), rx)`; install a SIGINT handler (`tokio::signal::ctrl_c`) that flips the `shutdown` watch; `run_collector(..).await`; on shutdown, drop the `out` sender so the writer flushes, await the writer handle. All logs via `eprintln!`/`tracing` to **stderr**.
- [ ] **Step 2:** Run `cargo build` — expect success.
- [ ] **Step 3:** Run `cargo run 2>/dev/null | head -c 0` with creds for ~2 s then Ctrl-C — expect clean exit, no panic on stderr. (Smoke only.)
- [ ] **Step 4:** Commit (`feat: main wiring and graceful SIGINT shutdown`).

---

### Task 13: Live end-to-end run + read-back verification

**Files:**
- Create: `scripts/verify_capture.rs` as an example/bin, or a `#[ignore]` test `tests/e2e.rs` that reads a captured file with `AlignedSbeReader` and checks invariants.
- Modify: `Cargo.toml` if adding a `[[bin]]` or `[[example]]`.

**Interfaces:**
- Consumes: `AlignedSbeReader<OrderbookSchema>`, `AnyMessage`.

- [ ] **Step 1:** Run the collector live, capped (e.g. `MAX_MARKETS=40 REDUNDANCY=2`), redirecting stdout: `cargo run --release 2>run.log > capture.sbe`; let it run ≥ 2 minutes (to cross ≥1 minute-snapshot boundary); Ctrl-C.
- [ ] **Step 2:** Write a verifier (`tests/e2e.rs`, `#[ignore]`) that opens `capture.sbe` and asserts: file non-empty; messages decode; every symbol seen in a delta first appeared in a snapshot; at least one full snapshot per symbol after the first minute; no decode errors.
- [ ] **Step 3:** Run the verifier against `capture.sbe` — expect PASS. Capture counts (symbols, snapshots, deltas) in the commit message.
- [ ] **Step 4:** Commit (`test: live end-to-end capture and read-back verification`).

---

### Task 14: Ext 4 — latency experiments + writeup

**Files:**
- Create: `docs/latency.md`; optionally `src/bin/latency_probe.rs`.

**Interfaces:**
- Consumes: `KalshiCodec`, `spawn_connection`.

- [ ] **Step 1:** Implement a probe that opens N redundant connections to the same markets and, per `(market, seq)` or `(market, price, ts_ms)`, records the receive-time spread across connections (the challenge's "+18 ms vs +7 ms" comparison).
- [ ] **Step 2:** Run it live briefly; collect numbers.
- [ ] **Step 3:** Write `docs/latency.md`: method, measured receive-time spread across connections, observations on multi-connect "race the fastest," source-port variation, and connect-timing ideas — with the measured data and honest caveats.
- [ ] **Step 4:** Commit (`docs: latency experiments and findings`).

---

## Notes for the executor

- Keep stdout pristine: **never** `println!` in library/runtime paths; logs use stderr.
- The spike project in the scratchpad is a reference for message shapes and auth; it is throwaway and not part of this repo.
- Commit messages record live-run evidence (counts, sample lines) where a task was validated live rather than by a unit test.
