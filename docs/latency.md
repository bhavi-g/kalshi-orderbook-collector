# Latency findings (Extension 4)

Correctness and completeness are priority #1 for this collector; latency is a
secondary concern. Still, for high-frequency taker strategies a few milliseconds
decide whether a trade is captured, so this note records what I measured against
**live production Kalshi** and the techniques worth pursuing.

## Measurement method

I ran a probe (`latency.rs`) that opens **two parallel WebSocket connections to
the same ~30 liquid markets** and records the local receive `Instant` of every
`orderbook_delta` on each connection.

A subtlety drives the whole design: Kalshi's `seq` is **per-connection** (it
counts messages on one subscription), so it cannot match the same exchange event
across two connections. The docs mention a cross-connection `clock`, but it is
**not present on the live wire** (confirmed in the spike). So I match an event
across connections by its content key:

```
(market_ticker, side, price_dollars, ts_ms)
```

`ts_ms` is the exchange's event timestamp and is identical on both connections,
so the key identifies the same exchange action. For each matched event I compute
the difference between the two connections' local receive times.

## Results (25 s sample, 2 connections, 30 markets)

```
events matched on both connections : 2415
conn A arrived first               : 210
conn B arrived first               : 2200
abs receive-time spread (ms)       : mean 2.58  median 1.83  p90 5.15  max 92.65
```

Two clear takeaways:

1. **Redundant connections have materially different receive times.** The median
   gap between two sockets to the same exchange is ~1.8 ms, with a p90 of ~5 ms
   and a tail to ~90 ms. For a taker algo, systematically consuming the *earlier*
   of two (or more) connections is a free latency improvement — which is exactly
   what the collector's "fastest healthy source" selection does.

2. **One connection was consistently faster.** Connection B beat A on ~91 % of
   events. The two sockets are not symmetric — routing, the server instance each
   landed on, and buffering differ per connection. This is the empirical basis
   for "spam connections until you find a fast one, then keep it."

The long tail (max ~93 ms) is the more important completeness story: on the slow
connection, individual events occasionally lag by tens of milliseconds, which is
also where silent stalls begin. Running ≥2 connections and switching to the
fresher one both lowers latency and protects completeness.

## Techniques worth pursuing (and how they map to this codebase)

- **Race multiple connections, consume the fastest.** Already implemented:
  the collector keeps a primary + backup per market and picks the freshest
  healthy connection as the output source, re-snapshotting on every switch.
  The data above shows this is worth 1–5 ms typically and much more on the tail.
- **Spam connects, keep the fastest.** Since one socket was ~10× more often
  first, opening several and retiring the slow ones is a cheap win. The
  `spawn_connection` task + source selection make this a natural extension:
  over-provision replicas, then recycle the slowest.
- **Connect right after scheduled maintenance.** Being early onto a freshly
  restarted server instance tends to land a less loaded, lower-latency path.
- **Source-port variation on TCP.** Changing the local source port can change
  the ECMP/routing hash and the server instance you reach; cycling source ports
  across connections is a way to sample for a faster path (OS-level socket
  option, outside the WS library).
- **Co-location / region.** The biggest wins are physical: running close to the
  exchange's endpoints collapses the base RTT that dominates all of the above.

## Caveats

- Local receive time includes this host's scheduler and TLS/WS processing, not
  just wire time; the *relative* spread between two connections on the same host
  is the meaningful number, not the absolute values.
- A 25 s sample at one time of day; absolute figures vary with load, time, and
  network path. The qualitative findings (asymmetric connections, millisecond
  spread, long tail) are the durable part.
- The probe is a throwaway measurement tool, not part of the shipped collector.
