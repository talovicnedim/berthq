//! Test helpers: jobs run synchronously on a throwaway database with a fixed clock.

use std::path::PathBuf;

use redb::Database;
use serde_json::value::RawValue;
use ulid::Ulid;

use crate::delivery::{Delivery, Settled};
use crate::error::Result;
use crate::failure::Failure;
use crate::memory::{Memory, QueueMemory};
use crate::positions::Start;
use crate::publish::{NewMessage, Published, check};
use crate::records::{ConsumerRecord, QueueCounts};
use crate::settings::QueueSettings;
use crate::tables::Tables;
use crate::take::Take;
use crate::tx::Tx;

pub struct TestDb {
    pub db: Database,
    _dir: TempDir,
    pub mem: Memory,
}

pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "berthq-test-{name}-{}-{}",
            std::process::id(),
            Ulid::from_parts(crate::time::now_ms(), u128::from(crate::ids::random_u64())).0
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl TestDb {
    /// A database with one queue named `jobs`.
    pub fn with_queue(settings: QueueSettings) -> Self {
        let dir = TempDir::new("tx");
        let db = Database::create(dir.0.join("test.redb")).unwrap();
        let mut test = Self {
            db,
            _dir: dir,
            mem: Memory::new(),
        };
        test.run(0, |tx| {
            tx.create_queue(QueueMemory::new("jobs", settings, 0))
        })
        .unwrap();
        test
    }

    /// Runs `job` at time `now` and commits, like the writer thread does.
    pub fn run<T>(&mut self, now: u64, job: impl FnOnce(&mut Tx) -> Result<T>) -> Result<T> {
        let txn = self.db.begin_write().unwrap();
        let result = {
            let mut tables = Tables::open(&txn).unwrap();
            let mut tx = Tx::new(&mut tables, &mut self.mem, now);
            let result = job(&mut tx);
            tx.save_counts().unwrap();
            result
        };
        txn.commit().unwrap();
        result
    }

    pub fn tick(&mut self, now: u64) {
        self.run(now, |tx| tx.tick()).unwrap();
    }

    pub fn counts(&self, queue: &str) -> QueueCounts {
        self.mem.queue(queue).unwrap().counts
    }

    pub fn consumer(&self, name: &str) -> ConsumerRecord {
        self.mem.consumer("jobs", name).unwrap().record.clone()
    }

    pub fn settings(&mut self) -> &mut QueueSettings {
        &mut self.mem.queue_mut("jobs").unwrap().settings
    }

    pub fn backlog(&mut self, now: u64) -> u64 {
        self.run(now, |tx| tx.backlog_now("jobs")).unwrap()
    }
}

pub fn body(n: i64) -> NewMessage {
    NewMessage {
        body: RawValue::from_string(n.to_string()).unwrap(),
        priority: None,
        delay_ms: None,
        deliver_at: None,
        dedup_id: None,
    }
}

pub fn publish(db: &mut TestDb, message: NewMessage, now: u64) -> Result<Published> {
    let checked = vec![check(message, now)?];
    let mut published = db.run(now, |tx| tx.publish("jobs", checked))?;
    Ok(published.remove(0))
}

pub fn publish_bodies(db: &mut TestDb, bodies: impl IntoIterator<Item = i64>, now: u64) {
    for n in bodies {
        publish(db, body(n), now).unwrap();
    }
}

pub fn id(published: &Published) -> Ulid {
    crate::ids::parse(&published.id).unwrap()
}

pub fn request(consumer: &str, max: usize) -> Take {
    Take {
        consumer: consumer.to_string(),
        from: Start::Oldest,
        max,
        wait_ms: 0,
        visibility_ms: None,
        auto_ack: false,
        worker: "worker".to_string(),
    }
}

/// Leases up to `max` messages for `consumer` (created at the start if new).
pub fn take(db: &mut TestDb, consumer: &str, max: usize, now: u64) -> Vec<Delivery> {
    take_with(db, &request(consumer, max), now)
}

pub fn take_with(db: &mut TestDb, take: &Take, now: u64) -> Vec<Delivery> {
    db.run(now, |tx| tx.take("jobs", take)).unwrap()
}

pub fn bodies(deliveries: &[Delivery]) -> Vec<serde_json::Value> {
    deliveries
        .iter()
        .map(|d| serde_json::from_str(d.body.get()).unwrap())
        .collect()
}

pub fn leases(deliveries: &[Delivery]) -> Vec<String> {
    deliveries.iter().filter_map(|d| d.lease.clone()).collect()
}

pub fn ack(db: &mut TestDb, deliveries: &[Delivery], now: u64) -> Settled {
    let leases = leases(deliveries);
    db.run(now, |tx| tx.ack("jobs", &leases)).unwrap()
}

pub fn failure(error: &str) -> Failure {
    Failure {
        delay_ms: None,
        dead_letter: false,
        error: Some(error.to_string()),
    }
}

pub fn nack(db: &mut TestDb, delivery: &Delivery, failure: &Failure, now: u64) -> Settled {
    let leases = leases(std::slice::from_ref(delivery));
    db.run(now, |tx| tx.nack("jobs", &leases, failure)).unwrap()
}

pub fn replay(db: &mut TestDb, consumer: &str, from: Start, now: u64) {
    db.run(now, |tx| {
        let positions = tx.positions_for("jobs", from)?;
        tx.move_consumer("jobs", consumer, positions)
    })
    .unwrap();
}

pub fn message_ids(deliveries: &[Delivery]) -> Vec<String> {
    deliveries.iter().map(|d| d.id.clone()).collect()
}
