//! End-to-end verification: read back a captured SBE stream with the template's
//! reader and assert completeness invariants.
//!
//! Run after a live capture:
//!   cargo run --release > capture.sbe 2> run.log   (Ctrl-C after >1 min)
//!   cargo test --test e2e -- --ignored --nocapture

use std::collections::{HashMap, HashSet};

use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[test]
#[ignore = "requires a capture.sbe from a live run"]
fn capture_is_complete_and_ordered() {
    let path = "capture.sbe";
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)
        .expect("open capture.sbe (run the collector first)");

    let mut snapshots = 0usize;
    let mut deltas = 0usize;
    let mut snapshotted: HashSet<String> = HashSet::new();
    let mut snap_count_per_symbol: HashMap<String, usize> = HashMap::new();
    let mut delta_before_snapshot = 0usize;

    while let Some(msg) = reader.read_msg().expect("decode message") {
        match msg {
            AnyMessage::Snapshot(s) => {
                let d = SnapshotDomain::try_from_decoder(s).expect("snapshot domain");
                snapshots += 1;
                snapshotted.insert(d.symbol.clone());
                *snap_count_per_symbol.entry(d.symbol).or_default() += 1;
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let d = PriceLevelUpdateDomain::try_from_decoder(u).expect("update domain");
                deltas += 1;
                if !snapshotted.contains(&d.symbol) {
                    delta_before_snapshot += 1;
                }
            }
            _ => panic!("unexpected message type"),
        }
    }

    eprintln!(
        "verified: {snapshots} snapshots, {deltas} deltas, {} symbols, {} deltas-before-snapshot",
        snapshotted.len(),
        delta_before_snapshot
    );

    assert!(snapshots > 0, "expected at least one snapshot");
    assert!(deltas > 0, "expected at least one delta");
    assert_eq!(
        delta_before_snapshot, 0,
        "every symbol with a delta must have been snapshotted first"
    );
    // Each symbol should have an initial snapshot and at least one minute snapshot.
    let multi = snap_count_per_symbol.values().filter(|&&c| c >= 2).count();
    assert!(multi > 0, "expected minute snapshots in addition to the initial ones");
}
