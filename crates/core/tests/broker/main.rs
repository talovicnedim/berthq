//! End-to-end behavior of a `Broker` on a real data directory: publishing, long-polling,
//! consumers and replay, acks, backups, and what survives a restart.

// Helpers outside `#[test]` functions aren't covered by clippy's allow-unwrap-in-tests.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use berthq_core::{
    Broker, ConsumeOptions, ConsumerMessageState, DeadLetterReason, Error, MessageState,
    NackOptions, NewMessage, NewQueue, QueueSettingsPatch,
};
use serde_json::json;

mod consumers;
mod delivery;
mod storage;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("berthq-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn open_with_queue(dir: &Path, name: &str, settings: QueueSettingsPatch) -> Broker {
    let broker = Broker::open(dir, 0).unwrap();
    let new = NewQueue {
        name: name.to_string(),
        settings,
    };
    broker.queues_create(new).await.unwrap();
    broker
}

fn message(body: serde_json::Value) -> NewMessage {
    NewMessage {
        body: serde_json::value::to_raw_value(&body).unwrap(),
        priority: None,
        delay_ms: None,
        deliver_at: None,
        dedup_id: None,
    }
}

fn consume(consumer: &str, max: u32) -> ConsumeOptions {
    ConsumeOptions {
        consumer: Some(consumer.to_string()),
        max: Some(max),
        ..ConsumeOptions::default()
    }
}
