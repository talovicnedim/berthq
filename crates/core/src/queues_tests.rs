use std::time::Duration;

use redb::ReadableTableMetadata;

use super::*;
use crate::dedup::DEDUP_WINDOW_MS;
use crate::delivery::{ConsumeOptions, NackOptions};
use crate::tables::{DEDUP, DEDUP_EXPIRING};
use crate::testing::{TempDir, body};

async fn open_with_queue(dir: &TempDir) -> Broker {
    let broker = Broker::open(&dir.0, 0).unwrap();
    let new = NewQueue {
        name: "jobs".to_string(),
        settings: QueueSettingsPatch::default(),
    };
    broker.queues_create(new).await.unwrap();
    broker
}

#[tokio::test]
async fn removing_a_queue_forgets_all_its_dedup_ids() {
    let dir = TempDir::new("remove-dedup");
    let broker = open_with_queue(&dir).await;
    broker
        .store()
        .run(|tx| {
            for n in 0..=DELETE_CHUNK {
                tx.remember("jobs", &n.to_string(), Ulid(n as u128), DEDUP_WINDOW_MS)?;
            }
            Ok(())
        })
        .await
        .unwrap();

    broker.queues_remove("jobs").await.unwrap();
    let txn = broker.store().read().unwrap();
    assert_eq!(txn.open_table(DEDUP).unwrap().len().unwrap(), 0);
    assert_eq!(txn.open_table(DEDUP_EXPIRING).unwrap().len().unwrap(), 0);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn queue_names_ending_in_dlq_are_refused() {
    let dir = TempDir::new("dlq-name");
    let broker = Broker::open(&dir.0, 0).unwrap();
    let new = NewQueue {
        name: "audit.dlq".to_string(),
        settings: QueueSettingsPatch::default(),
    };
    let err = broker.queues_create(new).await.unwrap_err();
    assert_eq!(err.kind(), "invalid", "{err}");
    assert!(broker.queues_list().is_empty());
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn dead_letter_queues_are_created_on_demand_and_can_be_changed() {
    let dir = TempDir::new("dlq-update");
    let broker = open_with_queue(&dir).await;
    broker.publish("jobs", body(1)).await.unwrap();
    let delivery = broker
        .consume("jobs", ConsumeOptions::default())
        .await
        .unwrap()
        .messages
        .remove(0);
    let nack = NackOptions {
        leases: vec![delivery.lease.unwrap()],
        dead_letter: true,
        ..NackOptions::default()
    };
    broker.nack("jobs", nack).await.unwrap();

    let patch = QueueSettingsPatch {
        max_attempts: Some(2),
        ..QueueSettingsPatch::default()
    };
    let dlq = broker.queues_update("jobs.dlq", patch).await.unwrap();
    assert_eq!(dlq.dead_letter_of.as_deref(), Some("jobs"));
    assert_eq!((dlq.settings.max_attempts, dlq.stats.messages), (2, 1));
    broker.shutdown(Duration::ZERO).await;
}
