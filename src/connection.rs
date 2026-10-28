//! One resilient WebSocket connection: connects, authenticates, subscribes,
//! decodes frames into normalized events, and accepts commands (add/remove
//! markets, forced reconnect). Reconnects with exponential backoff.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;

use crate::codec::ExchangeCodec;
use crate::model::NormalizedEvent;

/// Commands the collector sends to a live connection.
#[derive(Debug, Clone)]
pub enum ConnCmd {
    AddMarkets(Vec<String>),
    RemoveMarkets(Vec<String>),
    /// Drop and re-establish the socket (recycle / repair).
    Reconnect,
}

pub struct ConnConfig {
    pub id: usize,
    pub markets: Vec<String>,
}

/// Spawn a connection task. Events are tagged with the connection id.
pub fn spawn_connection<C: ExchangeCodec>(
    codec: Arc<C>,
    cfg: ConnConfig,
    tx: Sender<(usize, NormalizedEvent)>,
    mut cmd_rx: Receiver<ConnCmd>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut markets = cfg.markets.clone();
        let mut backoff = Duration::from_millis(500);
        loop {
            if *shutdown.borrow() {
                break;
            }
            match run_once(&codec, cfg.id, &mut markets, &tx, &mut cmd_rx, &mut shutdown).await {
                Ok(RunExit::Shutdown) => break,
                Ok(RunExit::Reconnect) => {
                    backoff = Duration::from_millis(500);
                }
                Err(e) => {
                    eprintln!("[conn {}] error: {e:#}", cfg.id);
                }
            }
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.changed() => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
        eprintln!("[conn {}] stopped", cfg.id);
    })
}

enum RunExit {
    Shutdown,
    Reconnect,
}

async fn run_once<C: ExchangeCodec>(
    codec: &Arc<C>,
    id: usize,
    markets: &mut Vec<String>,
    tx: &Sender<(usize, NormalizedEvent)>,
    cmd_rx: &mut Receiver<ConnCmd>,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<RunExit> {
    let headers = codec.auth_headers("GET", codec.ws_path())?;
    let mut req = codec.ws_url().into_client_request()?;
    {
        let h = req.headers_mut();
        for (k, v) in &headers {
            h.insert(HeaderName::from_bytes(k.as_bytes())?, HeaderValue::from_str(v)?);
        }
    }
    let (mut ws, _) = connect_async(req).await?;
    eprintln!("[conn {id}] connected ({} markets)", markets.len());
    ws.send(Message::Text(codec.subscribe_frame(id as u64, markets)))
        .await?;

    let mut sid: u64 = 0;
    let mut next_cmd_id: u64 = 1_000_000 + id as u64 * 1000;
    let mut ping = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                let _ = ws.close(None).await;
                return Ok(RunExit::Shutdown);
            }
            _ = ping.tick() => {
                let _ = ws.send(Message::Ping(Vec::new())).await;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ConnCmd::Reconnect) => {
                        let _ = ws.close(None).await;
                        return Ok(RunExit::Reconnect);
                    }
                    Some(ConnCmd::AddMarkets(m)) => {
                        for s in &m { if !markets.contains(s) { markets.push(s.clone()); } }
                        next_cmd_id += 1;
                        let frame = codec.update_add_frame(next_cmd_id, sid, &m);
                        if !frame.is_empty() { let _ = ws.send(Message::Text(frame)).await; }
                    }
                    Some(ConnCmd::RemoveMarkets(m)) => {
                        markets.retain(|s| !m.contains(s));
                        next_cmd_id += 1;
                        let frame = codec.update_remove_frame(next_cmd_id, sid, &m);
                        if !frame.is_empty() { let _ = ws.send(Message::Text(frame)).await; }
                    }
                    None => { /* collector gone */ return Ok(RunExit::Shutdown); }
                }
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        // A single malformed/control text frame (e.g. a "PONG")
                        // must not tear down the socket — skip it, never fatal.
                        match codec.decode(&t, Utc::now()) {
                            Ok(events) => {
                                for ev in events {
                                    if let NormalizedEvent::SubscribeAck { sid: s } = &ev {
                                        sid = *s;
                                    }
                                    if tx.send((id, ev)).await.is_err() {
                                        return Ok(RunExit::Shutdown);
                                    }
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                    Some(Ok(Message::Close(_))) | None => anyhow::bail!("ws closed by peer"),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                }
            }
        }
    }
}
