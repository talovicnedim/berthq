//! The `Broker` handle: opens storage, runs the background timers and shuts down without
//! losing messages.
//!
//! Feature modules (`queues.rs`, `publish.rs`, `delivery.rs`, …) add their operations as
//! `impl Broker` methods. Changes run as jobs on the writer (`self.store().run(|tx| …)`); reads
//! use the in-memory counts or a read transaction.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::backup::remove_leftovers;
use crate::disk::Disk;
use crate::error::Result;
use crate::memory::Memory;
use crate::store::{Store, lock};
use crate::time;

/// How often delays, retries, lease timeouts and retention are checked.
const TICK: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    store: Store,
    disk: Disk,
    /// Held while a backup copies the database.
    backup: Mutex<()>,
    data_dir: PathBuf,
    started_at: u64,
    timers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Broker {
    /// Opens (or creates) the data directory and starts the background timers. Publishing stops
    /// while the disk has less than `min_free_disk` bytes free (0 turns that off). Messages that
    /// were being processed when the broker last stopped count as a failed attempt. Must be
    /// called inside a Tokio runtime.
    pub fn open(data_dir: &Path, min_free_disk: u64) -> Result<Broker> {
        let store = Store::open(data_dir)?;
        let disk = Disk::open(data_dir, min_free_disk)?;
        remove_leftovers(data_dir);
        let broker = Broker {
            inner: Arc::new(Inner {
                store,
                disk,
                backup: Mutex::new(()),
                data_dir: data_dir.to_path_buf(),
                started_at: time::now_ms(),
                timers: Mutex::new(Vec::new()),
            }),
        };
        broker.spawn_timers();
        Ok(broker)
    }

    /// Stops taking new work, waits up to `timeout` for leased messages to be acked or nacked,
    /// then flushes storage. Unacked messages count as a failed attempt at the next start.
    pub async fn shutdown(&self, timeout: Duration) {
        let leased = {
            let mut memory = self.memory();
            memory.closing = true;
            for queue in memory.queues.values() {
                queue.notify.notify_waiters();
            }
            leased_count(&memory)
        };
        if leased > 0 {
            tracing::info!("waiting for {leased} messages being processed to be acked");
        }
        // A timeout too long to add to the clock means waiting until everything is acked.
        let deadline = tokio::time::Instant::now().checked_add(timeout);
        while leased_count(&self.memory()) > 0
            && deadline.is_none_or(|deadline| tokio::time::Instant::now() < deadline)
        {
            tokio::time::sleep(TICK).await;
        }
        let leased = leased_count(&self.memory());
        if leased > 0 {
            tracing::info!(
                "{leased} messages still being processed; each counts as a failed attempt at the \
                 next start"
            );
        }
        let timers = std::mem::take(&mut *lock(&self.inner.timers));
        for timer in timers {
            // A timer that panicked has nothing left to clean up.
            let _ = timer.await;
        }
        self.inner.store.close();
    }

    /// Whether shutdown has started.
    pub fn is_closing(&self) -> bool {
        self.memory().closing
    }

    /// Set once storage has failed (a disk error, or a bug that panicked a write): every write
    /// fails with this message until Berthq is restarted.
    pub fn storage_failure(&self) -> Option<String> {
        self.inner.store.failure()
    }

    pub(crate) fn memory(&self) -> MutexGuard<'_, Memory> {
        self.inner.store.memory()
    }

    pub(crate) fn store(&self) -> &Store {
        &self.inner.store
    }

    pub(crate) fn disk(&self) -> &Disk {
        &self.inner.disk
    }

    pub(crate) fn backup_lock(&self) -> &Mutex<()> {
        &self.inner.backup
    }

    pub(crate) fn started_at(&self) -> u64 {
        self.inner.started_at
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    fn spawn_timers(&self) {
        let broker = self.clone();
        let tick = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let now = time::now_ms();
                {
                    let memory = broker.memory();
                    if memory.closing {
                        break;
                    }
                    // Most ticks have nothing to do; skip the write transaction then.
                    if !memory.queues.values().any(|q| q.wake_at <= now) {
                        continue;
                    }
                }
                // Every tick would fail the same way; the store logged why once.
                if broker.storage_failure().is_some() {
                    break;
                }
                if let Err(err) = broker.store().run(|tx| tx.tick()).await {
                    tracing::error!("timer failed: {err}");
                }
            }
        });

        let broker = self.clone();
        let sample = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut previous = HashMap::new();
            loop {
                interval.tick().await;
                {
                    let mut memory = broker.memory();
                    if memory.closing {
                        break;
                    }
                    memory.sample(time::now_ms(), &mut previous);
                }
                if let Err(err) = broker.refresh_stats() {
                    tracing::warn!("failed to measure queue backlogs: {err}");
                }
                let measured = broker.clone();
                // Reading free space can block on network file systems.
                let _ = tokio::task::spawn_blocking(move || {
                    measured.disk().measure(measured.data_dir());
                })
                .await;
            }
        });
        lock(&self.inner.timers).extend([tick, sample]);
    }
}

fn leased_count(memory: &Memory) -> u64 {
    memory.queues.values().map(|queue| queue.processing()).sum()
}
