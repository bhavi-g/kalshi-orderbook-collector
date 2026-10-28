//! The single output sink: owns an `AlignedSbeWriter` and serializes all
//! messages to it, flushing the final chunk when the channel closes.

use std::path::PathBuf;

use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::AlignedSbeWriter;
use tokio::sync::mpsc::Receiver;

/// Chunk size the template's `AlignedSbeReader` expects by default.
const CHUNK: usize = 64 << 20;

pub enum OutMsg {
    Snapshot(SnapshotDomain),
    Delta(PriceLevelUpdateDomain),
}

/// Spawn the writer on a blocking thread. It drains `rx`, and when every sender
/// is dropped (e.g. on shutdown) it flushes the final partial chunk and exits.
pub fn spawn_writer(
    path: PathBuf,
    mut rx: Receiver<OutMsg>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut writer = AlignedSbeWriter::create(&path, CHUNK)?;
        while let Some(msg) = rx.blocking_recv() {
            match msg {
                OutMsg::Snapshot(s) => writer.write_msg(&s)?,
                OutMsg::Delta(d) => writer.write_msg(&d)?,
            }
        }
        writer.flush()?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::{build_snapshot, build_update};
    use chrono::{DateTime, Utc};
    use engineering_challenge::util::dtos::challenge_orderbook::AnyMessage;
    use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
    use engineering_challenge::Side;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[tokio::test]
    async fn roundtrip_through_writer_flushes_on_close() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let handle = spawn_writer(path.clone(), rx);

        tx.send(OutMsg::Snapshot(build_snapshot(
            "EX",
            &[(590_000, 798_000_000)],
            &[(700_000, 703_000_000)],
            ts(),
            ts(),
        )))
        .await
        .unwrap();
        tx.send(OutMsg::Delta(build_update(
            "EX",
            Side::Sell,
            410_000,
            702_000_000,
            ts(),
            ts(),
        )))
        .await
        .unwrap();
        drop(tx); // closing the channel must still flush the partial chunk

        handle.await.unwrap().unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        match reader.read_msg().unwrap() {
            Some(AnyMessage::Snapshot(s)) => {
                let d = SnapshotDomain::try_from_decoder(s).unwrap();
                assert_eq!(d.symbol, "EX");
                assert_eq!(d.bids[0].price_level, 590_000);
            }
            _ => panic!("expected snapshot"),
        }
        match reader.read_msg().unwrap() {
            Some(AnyMessage::PriceLevelUpdate(u)) => {
                let d = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
                assert_eq!(d.price_level, 410_000);
                assert_eq!(d.side, Side::Sell);
            }
            _ => panic!("expected update"),
        }
        assert!(reader.read_msg().unwrap().is_none());
    }
}
