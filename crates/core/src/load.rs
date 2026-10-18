//! Starting up: reading queues and consumers into memory, and failing the deliveries the broker
//! stopped in the middle of. They were never acked, so each counts as a failed attempt.

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::error::Result;
use crate::failure::Failure;
use crate::memory::{ConsumerMemory, Memory, QueueMemory};
use crate::records::{ConsumerRecord, QueueCounts, QueueRecord};
use crate::tables::{CONSUMERS, COUNTS, QUEUES, Tables};
use crate::time;
use crate::tx::Tx;

/// Leased messages failed per transaction after a restart.
const RECOVER_CHUNK: usize = 10_000;

/// Reads queues, their counts and their consumers.
pub(crate) fn load(db: &Database) -> Result<Memory> {
    let txn = db.begin_read()?;
    let mut memory = Memory::new();
    let counts = txn.open_table(COUNTS)?;
    for entry in txn.open_table(QUEUES)?.iter()? {
        let (_, value) = entry?;
        let record: QueueRecord = serde_json::from_slice(value.value())?;
        let mut queue = QueueMemory::new(&record.name, record.settings, record.created_at);
        if let Some(saved) = counts.get(record.name.as_str())? {
            queue.counts = serde_json::from_slice::<QueueCounts>(saved.value())?;
        }
        // The timer checks every queue once after startup and works out the real times.
        queue.wake_at = 0;
        queue.retention_due = 0;
        memory.queues.insert(record.name, queue);
    }
    for entry in txn.open_table(CONSUMERS)?.iter()? {
        let (key, value) = entry?;
        let (queue, name) = key.value();
        let record: ConsumerRecord = serde_json::from_slice(value.value())?;
        if let Some(queue) = memory.queues.get_mut(queue) {
            let mut consumer = ConsumerMemory::new(record);
            consumer.wake_at = 0;
            queue.consumers.insert(name.to_string(), consumer);
        }
    }
    Ok(memory)
}

/// Replaces the queues with the ones on disk after a failed write, so a queue the failed batch
/// created is gone again. Keeps what only lives in memory (throughput, workers).
pub(crate) fn restore(memory: &mut Memory, fresh: Memory) {
    let mut old = std::mem::replace(&mut memory.queues, fresh.queues);
    for (name, queue) in &mut memory.queues {
        let Some(old) = old.remove(name) else {
            continue;
        };
        queue.counters = old.counters;
        queue.workers = old.workers;
        queue.history = old.history;
        queue.notify = old.notify;
        queue.backlog = old.backlog;
        queue.tails = old.tails;
    }
}

/// Fails every delivery that was still leased when the broker stopped, like an expired lease: it
/// is retried after the backoff, or dead-lettered once out of attempts. So a message that keeps
/// crashing the broker still reaches the dead-letter queue.
pub(crate) fn recover(db: &Database, memory: &mut Memory) -> Result<()> {
    let failure = Failure {
        delay_ms: None,
        dead_letter: false,
        error: Some("the broker restarted before the message was acked".to_string()),
    };
    let mut recovered = 0;
    let consumers: Vec<(String, String)> = memory
        .queues
        .iter()
        .flat_map(|(queue, q)| q.consumers.keys().map(|c| (queue.clone(), c.clone())))
        .collect();
    for (queue, consumer) in consumers {
        // In chunks, so a crash with many messages in flight doesn't need one huge transaction.
        loop {
            let txn = db.begin_write()?;
            let failed = {
                let mut tables = Tables::open(&txn)?;
                let mut tx = Tx::new(&mut tables, memory, time::now_ms());
                let ids =
                    Tx::due_pending(&tx.t.p_leased, &queue, &consumer, u64::MAX, RECOVER_CHUNK)?;
                for id in &ids {
                    if let Some(pending) = tx.pending(&queue, &consumer, *id)? {
                        tx.fail(&queue, &consumer, pending, &failure)?;
                    }
                }
                tx.save_counts()?;
                ids.len()
            };
            txn.commit()?;
            recovered += failed;
            if failed < RECOVER_CHUNK {
                break;
            }
        }
    }
    if recovered > 0 {
        tracing::info!(
            "{recovered} messages were being processed at shutdown; each counts as a failed attempt"
        );
    }
    Ok(())
}
