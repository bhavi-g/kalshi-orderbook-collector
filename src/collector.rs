//! Exchange-agnostic collector core: maintains per-connection books, chooses an
//! output source per symbol, detects gaps/stalls/quiet subscriptions and
//! re-snapshots on source switches, recycles sockets, and keeps the subscribed
//! symbol set in sync with the exchange.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::sync::{mpsc, watch};

use crate::codec::ExchangeCodec;
use crate::connection::{spawn_connection, ConnCmd, ConnConfig};
use crate::emit::{build_snapshot, build_update};
use crate::manager_logic::{choose_source, shard, ConnHealth};
use crate::model::NormalizedEvent;
use crate::monitor::{RateTracker, SeqStatus, SeqTracker};
use crate::output::OutMsg;
use crate::book::OrderBook;

#[derive(Clone)]
pub struct CollectorConfig {
    pub per_conn: usize,
    pub redundancy: usize,
    pub max_markets: usize,
    pub discovery_interval: Duration,
    pub snapshot_interval: Duration,
    pub stall_timeout: Duration,
    pub quiet_timeout: Duration,
    pub health_interval: Duration,
    pub socket_recycle_interval: Duration,
}

impl CollectorConfig {
    pub fn from_env() -> Self {
        let num = |k: &str, d: u64| {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        Self {
            per_conn: num("PER_CONN", 150) as usize,
            redundancy: num("REDUNDANCY", 2) as usize,
            // 0 = unlimited: subscribe to every regular market.
            max_markets: num("MAX_MARKETS", 0) as usize,
            discovery_interval: Duration::from_secs(num("DISCOVERY_SECS", 60)),
            snapshot_interval: Duration::from_secs(num("SNAPSHOT_SECS", 60)),
            stall_timeout: Duration::from_secs(num("STALL_SECS", 15)),
            quiet_timeout: Duration::from_secs(num("QUIET_SECS", 45)),
            health_interval: Duration::from_secs(num("HEALTH_SECS", 3)),
            socket_recycle_interval: Duration::from_secs(num("RECYCLE_SECS", 600)),
        }
    }
}

/// A gap on the current source means we may have missed updates; resync.
pub fn should_resync(status: SeqStatus, is_source: bool) -> bool {
    is_source && matches!(status, SeqStatus::Gap { .. })
}

/// Whether enough time has passed since the last quiet-market action on this
/// symbol to take another one. Without this cooldown, switching a quiet
/// symbol to its backup connection can immediately be judged quiet again on
/// the very next health-check tick (the backup's own last-activity timestamp
/// is typically just as stale, since it was subscribed from the start too),
/// causing the two connections to ping-pong every few seconds instead of
/// respecting `quiet_timeout`.
pub fn quiet_action_allowed(last: Option<Instant>, now: Instant, quiet_timeout: Duration) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= quiet_timeout)
}

struct Collector<C: ExchangeCodec> {
    codec: Arc<C>,
    cfg: CollectorConfig,
    out: mpsc::Sender<OutMsg>,
    cmd_txs: Vec<mpsc::Sender<ConnCmd>>,
    conn_group: Vec<usize>,
    group_conns: Vec<Vec<usize>>,
    group_symbols: Vec<HashSet<String>>,
    symbol_group: HashMap<String, usize>,
    books: HashMap<(usize, String), OrderBook>,
    seq: HashMap<usize, SeqTracker>,
    source: HashMap<String, usize>,
    last_msg: HashMap<usize, Instant>,
    last_activity: HashMap<(usize, String), Instant>,
    /// Last time we took quiet-market action (resubscribe + source switch) for
    /// each symbol. Prevents re-triggering faster than `quiet_timeout`: a
    /// backup connection subscribed from startup typically has its own
    /// equally-stale `last_activity` for the symbol, so without this cooldown
    /// the next health-check tick (every few seconds) would see it as still
    /// quiet and switch straight back, ping-ponging forever instead of
    /// respecting the intended cadence.
    last_quiet_action: HashMap<String, Instant>,
    rate: RateTracker,
    recycle_cursor: usize,
}

impl<C: ExchangeCodec> Collector<C> {
    fn conn_healths(&self, carriers: &[usize], now: Instant, avoid: Option<usize>) -> Vec<ConnHealth> {
        carriers
            .iter()
            .filter(|&&c| Some(c) != avoid)
            .map(|&c| {
                let age = now
                    .saturating_duration_since(*self.last_msg.get(&c).unwrap_or(&now))
                    .as_millis() as u64;
                ConnHealth {
                    id: c,
                    healthy: age < self.cfg.stall_timeout.as_millis() as u64,
                    last_msg_age_ms: age,
                }
            })
            .collect()
    }

    /// Send a built message to the output writer, blocking (applying
    /// backpressure) until there's room rather than silently discarding it.
    /// The only legitimate send failure here is the writer having shut down
    /// (e.g. during process exit), which we log loudly instead of pretending
    /// the message went out.
    async fn emit(&mut self, msg: OutMsg) {
        if self.out.send(msg).await.is_err() {
            eprintln!("[collector] output writer is gone; message could not be emitted");
        }
    }

    async fn emit_snapshot_from(&mut self, conn: usize, symbol: &str) {
        let Some(book) = self.books.get(&(conn, symbol.to_string())) else {
            return;
        };
        let (bids, asks) = book.levels();
        let now = Utc::now();
        let snap = build_snapshot(symbol, &bids, &asks, now, now);
        self.emit(OutMsg::Snapshot(snap)).await;
    }

    /// Switch a symbol's output source to the healthiest other carrier and emit
    /// a fresh snapshot from it. Returns true if the source changed.
    async fn switch_source(&mut self, symbol: &str, avoid: Option<usize>, reason: &str) -> bool {
        let Some(&gi) = self.symbol_group.get(symbol) else {
            return false;
        };
        let carriers = self.group_conns[gi].clone();
        let now = Instant::now();
        let healths = self.conn_healths(&carriers, now, avoid);
        let Some(new_src) = choose_source(&healths) else {
            return false;
        };
        if self.source.get(symbol) == Some(&new_src) {
            return false;
        }
        eprintln!("[collector] switch source {symbol} -> conn {new_src} ({reason})");
        self.source.insert(symbol.to_string(), new_src);
        self.emit_snapshot_from(new_src, symbol).await;
        true
    }

    async fn handle_event(&mut self, conn: usize, ev: NormalizedEvent) {
        let now = Instant::now();
        self.last_msg.insert(conn, now);
        match ev {
            NormalizedEvent::Snapshot { symbol, bids, asks, exch_ts, recv_at, seq } => {
                self.last_activity.insert((conn, symbol.clone()), now);
                // Snapshot and deltas share one per-connection seq stream; seed the
                // tracker so the first following delta is continuous.
                self.seq.entry(conn).or_insert_with(SeqTracker::new).check(seq);
                self.books
                    .entry((conn, symbol.clone()))
                    .or_insert_with(OrderBook::new)
                    .apply_snapshot(&bids, &asks);
                if self.source.get(&symbol) == Some(&conn) {
                    let snap = build_snapshot(&symbol, &bids, &asks, exch_ts, recv_at);
                    self.emit(OutMsg::Snapshot(snap)).await;
                }
            }
            NormalizedEvent::Delta {
                symbol, side, price_level, amount, exch_ts, recv_at, seq,
            } => {
                self.last_activity.insert((conn, symbol.clone()), now);
                let status = self.seq.entry(conn).or_insert_with(SeqTracker::new).check(seq);
                let abs = self
                    .books
                    .entry((conn, symbol.clone()))
                    .or_insert_with(OrderBook::new)
                    .apply(side, price_level, amount);
                let is_source = self.source.get(&symbol) == Some(&conn);
                if is_source {
                    let upd = build_update(&symbol, side, price_level, abs, exch_ts, recv_at);
                    self.emit(OutMsg::Delta(upd)).await;
                    self.rate.record(&symbol, now);
                }
                if should_resync(status, is_source) {
                    if let SeqStatus::Gap { missing } = status {
                        eprintln!("[collector] seq gap {missing} on source conn {conn} for {symbol}");
                    }
                    if self.switch_source(&symbol, Some(conn), "seq gap").await {
                        // repair the gapped connection's books with a reconnect
                        if let Some(tx) = self.cmd_txs.get(conn) {
                            let _ = tx.try_send(ConnCmd::Reconnect);
                        }
                    }
                }
            }
            NormalizedEvent::SubscribeAck { .. } | NormalizedEvent::Other => {}
        }
    }

    async fn emit_minute_snapshots(&mut self) {
        let symbols: Vec<(String, usize)> = self
            .source
            .iter()
            .map(|(s, &c)| (s.clone(), c))
            .collect();
        let count = symbols.len();
        for (symbol, conn) in symbols {
            self.emit_snapshot_from(conn, &symbol).await;
        }
        eprintln!("[collector] emitted minute snapshots for {count} symbols");
    }

    async fn health_check(&mut self) {
        let now = Instant::now();
        let symbols: Vec<String> = self.source.keys().cloned().collect();
        for symbol in symbols {
            let Some(&src) = self.source.get(&symbol) else { continue };
            // Stalled whole connection?
            let conn_age = now.saturating_duration_since(*self.last_msg.get(&src).unwrap_or(&now));
            // Quiet subscription on an otherwise-live connection?
            let act_age = now.saturating_duration_since(
                *self.last_activity.get(&(src, symbol.clone())).unwrap_or(&now),
            );
            if conn_age > self.cfg.stall_timeout {
                self.switch_source(&symbol, Some(src), "stall").await;
            } else if act_age > self.cfg.quiet_timeout {
                // Ext 2: the subscription has gone quiet (low data-points/sec on
                // an otherwise-live socket). Don't re-trigger faster than
                // quiet_timeout for the same symbol — see last_quiet_action's
                // doc comment for why the naive per-connection check alone
                // isn't enough.
                let last = self.last_quiet_action.get(&symbol).copied();
                if !quiet_action_allowed(last, now, self.cfg.quiet_timeout) {
                    continue;
                }
                // Resubscribe this symbol on its source to repair it, and
                // switch output to a fresher carrier meanwhile.
                let rate = self.rate.rate_per_sec(&symbol, now);
                eprintln!("[collector] quiet {symbol} (src rate {rate:.3}/s), resubscribing");
                self.last_quiet_action.insert(symbol.clone(), now);
                if let Some(tx) = self.cmd_txs.get(src) {
                    let _ = tx.try_send(ConnCmd::RemoveMarkets(vec![symbol.clone()]));
                    let _ = tx.try_send(ConnCmd::AddMarkets(vec![symbol.clone()]));
                }
                self.switch_source(&symbol, Some(src), "quiet").await;
            }
        }
    }

    /// Recycle one whole socket (Ext 2): move its sourced symbols to backups,
    /// then force it to reconnect and resubscribe fresh.
    async fn recycle_next(&mut self) {
        if self.cmd_txs.is_empty() {
            return;
        }
        let conn = self.recycle_cursor % self.cmd_txs.len();
        self.recycle_cursor = self.recycle_cursor.wrapping_add(1);
        let gi = self.conn_group[conn];
        let symbols: Vec<String> = self.group_symbols[gi].iter().cloned().collect();
        for symbol in symbols {
            if self.source.get(&symbol) == Some(&conn) {
                self.switch_source(&symbol, Some(conn), "recycle").await;
            }
        }
        eprintln!("[collector] recycling conn {conn}");
        if let Some(tx) = self.cmd_txs.get(conn) {
            let _ = tx.try_send(ConnCmd::Reconnect);
        }
    }

    async fn run_discovery(&mut self) {
        let limit = (self.cfg.max_markets > 0).then_some(self.cfg.max_markets);
        let listed = match self.codec.list_symbols(limit).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[collector] discovery failed: {e:#}");
                return;
            }
        };
        let listed_set: HashSet<String> = listed.iter().cloned().collect();
        let current: HashSet<String> = self.symbol_group.keys().cloned().collect();
        // Stable target: keep still-open current symbols, fill remaining capacity with new.
        let cap = if self.cfg.max_markets == 0 {
            usize::MAX
        } else {
            self.cfg.max_markets
        };
        let mut target: Vec<String> = current.intersection(&listed_set).cloned().collect();
        for s in &listed {
            if target.len() >= cap {
                break;
            }
            if !current.contains(s) {
                target.push(s.clone());
            }
        }
        let target_set: HashSet<String> = target.into_iter().collect();
        let (add, remove) = crate::lifecycle::diff(&current, &target_set);
        for s in &remove {
            self.remove_symbol(s);
        }
        for s in &add {
            self.add_symbol(s.clone());
        }
        if !add.is_empty() || !remove.is_empty() {
            eprintln!("[collector] discovery: +{} -{} (total {})", add.len(), remove.len(), self.symbol_group.len());
        }
    }

    fn add_symbol(&mut self, symbol: String) {
        // assign to the least-loaded group
        let Some(gi) = (0..self.group_conns.len()).min_by_key(|&g| self.group_symbols[g].len())
        else {
            return;
        };
        self.group_symbols[gi].insert(symbol.clone());
        self.symbol_group.insert(symbol.clone(), gi);
        self.source.insert(symbol.clone(), self.group_conns[gi][0]);
        for &conn in &self.group_conns[gi].clone() {
            if let Some(tx) = self.cmd_txs.get(conn) {
                let _ = tx.try_send(ConnCmd::AddMarkets(vec![symbol.clone()]));
            }
        }
    }

    fn remove_symbol(&mut self, symbol: &str) {
        if let Some(gi) = self.symbol_group.remove(symbol) {
            self.group_symbols[gi].remove(symbol);
            self.source.remove(symbol);
            for &conn in &self.group_conns[gi].clone() {
                self.books.remove(&(conn, symbol.to_string()));
                self.last_activity.remove(&(conn, symbol.to_string()));
                if let Some(tx) = self.cmd_txs.get(conn) {
                    let _ = tx.try_send(ConnCmd::RemoveMarkets(vec![symbol.to_string()]));
                }
            }
        }
    }
}

pub async fn run_collector<C: ExchangeCodec>(
    codec: Arc<C>,
    cfg: CollectorConfig,
    out: mpsc::Sender<OutMsg>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    // 1. Discover symbols (cap only if max_markets > 0).
    let limit = (cfg.max_markets > 0).then_some(cfg.max_markets);
    let mut symbols = codec.list_symbols(limit).await?;
    if cfg.max_markets > 0 {
        symbols.truncate(cfg.max_markets);
    }
    anyhow::ensure!(!symbols.is_empty(), "no regular symbols discovered");
    eprintln!("[collector] starting with {} symbols", symbols.len());

    // 2. Shard into groups; each group gets `redundancy` connections.
    let groups = shard(&symbols, cfg.per_conn);
    eprintln!(
        "[collector] {} groups x {} redundancy = {} connections",
        groups.len(),
        cfg.redundancy.max(1),
        groups.len() * cfg.redundancy.max(1)
    );
    let (event_tx, mut event_rx) = mpsc::channel::<(usize, NormalizedEvent)>(200_000);

    let mut cmd_txs = Vec::new();
    let mut conn_group = Vec::new();
    let mut group_conns: Vec<Vec<usize>> = Vec::new();
    let mut group_symbols: Vec<HashSet<String>> = Vec::new();
    let mut symbol_group = HashMap::new();
    let mut source = HashMap::new();

    for (gi, group) in groups.iter().enumerate() {
        group_symbols.push(group.iter().cloned().collect());
        for s in group {
            symbol_group.insert(s.clone(), gi);
        }
        let mut conns = Vec::new();
        for _r in 0..cfg.redundancy.max(1) {
            let conn_id = cmd_txs.len();
            let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(64);
            cmd_txs.push(cmd_tx);
            conn_group.push(gi);
            conns.push(conn_id);
            spawn_connection(
                codec.clone(),
                ConnConfig { id: conn_id, markets: group.clone() },
                event_tx.clone(),
                cmd_rx,
                shutdown.clone(),
            );
            // Stagger handshakes so we don't open many sockets in one instant.
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        // first replica is the initial source for every symbol in the group
        for s in group {
            source.insert(s.clone(), conns[0]);
        }
        group_conns.push(conns);
    }
    drop(event_tx); // only the connections hold senders now

    let mut collector = Collector {
        codec: codec.clone(),
        cfg: cfg.clone(),
        out,
        cmd_txs,
        conn_group,
        group_conns,
        group_symbols,
        symbol_group,
        source,
        books: HashMap::new(),
        seq: HashMap::new(),
        last_msg: HashMap::new(),
        last_activity: HashMap::new(),
        last_quiet_action: HashMap::new(),
        rate: RateTracker::new(Duration::from_secs(10)),
        recycle_cursor: 0,
    };

    // 3. Timers.
    let mut snap_t = tokio::time::interval(cfg.snapshot_interval);
    snap_t.tick().await; // consume the immediate first tick
    let mut disc_t = tokio::time::interval(cfg.discovery_interval);
    disc_t.tick().await;
    let mut health_t = tokio::time::interval(cfg.health_interval);
    health_t.tick().await;
    let mut recycle_t = tokio::time::interval(cfg.socket_recycle_interval);
    recycle_t.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                eprintln!("[collector] shutting down");
                break;
            }
            Some((conn, ev)) = event_rx.recv() => collector.handle_event(conn, ev).await,
            _ = snap_t.tick() => collector.emit_minute_snapshots().await,
            _ = health_t.tick() => collector.health_check().await,
            _ = recycle_t.tick() => collector.recycle_next().await,
            _ = disc_t.tick() => collector.run_discovery().await,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::polymarket::PolymarketCodec;
    use crate::model::Amount;
    use engineering_challenge::Side;

    #[test]
    fn resync_on_gap_from_source() {
        assert!(should_resync(SeqStatus::Gap { missing: 2 }, true));
    }
    #[test]
    fn no_resync_on_ok() {
        assert!(!should_resync(SeqStatus::Ok, true));
    }
    #[test]
    fn no_resync_from_nonsource() {
        assert!(!should_resync(SeqStatus::Gap { missing: 9 }, false));
    }

    #[test]
    fn quiet_action_allowed_first_time() {
        assert!(quiet_action_allowed(None, Instant::now(), Duration::from_secs(45)));
    }

    #[test]
    fn quiet_action_blocked_within_cooldown() {
        // Regression test: observed live as a symbol flagged "quiet" 4 times
        // in 110 seconds (expected at most ~2 at a 45s quiet_timeout) — the
        // health-check tick (every few seconds) must not be allowed to
        // re-trigger quiet handling before quiet_timeout has actually elapsed.
        let t0 = Instant::now();
        let three_seconds_later = t0 + Duration::from_secs(3); // one health_check tick
        assert!(
            !quiet_action_allowed(Some(t0), three_seconds_later, Duration::from_secs(45)),
            "must not re-trigger before quiet_timeout elapses"
        );
    }

    #[test]
    fn quiet_action_allowed_after_cooldown_elapses() {
        let t0 = Instant::now();
        let after_timeout = t0 + Duration::from_secs(46);
        assert!(quiet_action_allowed(Some(t0), after_timeout, Duration::from_secs(45)));
    }

    fn dummy_cfg() -> CollectorConfig {
        CollectorConfig {
            per_conn: 1,
            redundancy: 1,
            max_markets: 1,
            discovery_interval: Duration::from_secs(3600),
            snapshot_interval: Duration::from_secs(3600),
            stall_timeout: Duration::from_secs(3600),
            quiet_timeout: Duration::from_secs(3600),
            health_interval: Duration::from_secs(3600),
            socket_recycle_interval: Duration::from_secs(3600),
        }
    }

    fn dummy_delta(seq: u64) -> NormalizedEvent {
        NormalizedEvent::Delta {
            symbol: "SYM".to_string(),
            side: Side::Buy,
            price_level: 100,
            amount: Amount::Relative(1),
            exch_ts: Utc::now(),
            recv_at: Utc::now(),
            seq,
        }
    }

    /// Root-cause regression test: every delta emitted for the source
    /// connection must reach the output channel, even when the writer is
    /// momentarily slower than production. No event may be silently dropped
    /// just because the output channel is temporarily full.
    #[tokio::test]
    async fn delta_emission_never_drops_under_backpressure() {
        const N: usize = 50;
        let codec = Arc::new(PolymarketCodec::new().unwrap());
        let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(1); // tiny: forces backpressure
        let mut source = HashMap::new();
        source.insert("SYM".to_string(), 0usize);

        let mut collector = Collector {
            codec,
            cfg: dummy_cfg(),
            out: out_tx,
            cmd_txs: Vec::new(),
            conn_group: Vec::new(),
            group_conns: Vec::new(),
            group_symbols: Vec::new(),
            symbol_group: HashMap::new(),
            books: HashMap::new(),
            seq: HashMap::new(),
            source,
            last_msg: HashMap::new(),
            last_activity: HashMap::new(),
            last_quiet_action: HashMap::new(),
            rate: RateTracker::new(Duration::from_secs(10)),
            recycle_cursor: 0,
        };

        // Drain concurrently so a correctly-backpressured sender can make
        // progress; a sender that silently drops on a full channel does not
        // need this drainer to "succeed" at sending everything, which is
        // exactly the bug this test catches.
        let drain = tokio::spawn(async move {
            let mut count = 0usize;
            while count < N {
                match out_rx.recv().await {
                    Some(_) => count += 1,
                    None => break,
                }
            }
            count
        });

        for i in 1..=N {
            collector.handle_event(0, dummy_delta(i as u64)).await;
        }
        drop(collector);

        // The channel closes once `collector` (and its sole Sender) is dropped,
        // so the drain task always terminates on its own — no hang is possible
        // either way; the timeout is just a safety net.
        let received = tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("drain task hung unexpectedly")
            .unwrap();
        assert_eq!(
            received, N,
            "every emitted delta must reach the output channel; {} of {N} were silently lost \
             (this is the exact defect reported as \"market syncing bugs that prevented it from \
             capturing properly\" — see src/collector.rs try_send call sites)",
            N - received
        );
    }
}
