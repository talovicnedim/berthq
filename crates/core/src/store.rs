//! The database file and the writer thread.
//!
//! All changes run as jobs on one writer thread. It runs every job that has queued up since its
//! last commit inside one transaction and fsyncs once ("group commit"), so many concurrent
//! publishes, deliveries and acks share the cost of a disk flush. A job's caller gets its result
//! only after the commit, so nothing is confirmed before it is on disk.
//!
//! The writer fails for good when redb refuses to write until the file is reopened (after an I/O
//! error) or when a job panics. Every later write then fails with the first reason.

use std::fmt::Display;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::JoinHandle;

use redb::{CommitError, Database, ReadTransaction, ReadableDatabase, WriteTransaction};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::format;
use crate::load::{load, recover, restore};
use crate::memory::Memory;
use crate::tables::Tables;
use crate::time;
use crate::tx::Tx;

/// Most jobs folded into one transaction, so one commit never holds up callers for too long.
const MAX_BATCH: usize = 2048;

pub(crate) const FILE_NAME: &str = "berthq.redb";
/// Upper bound for redb's page cache, for the data file and for a backup copy. Hot pages (queue
/// heads, recent messages) fit easily; everything else is read from disk on demand.
pub(crate) const CACHE_BYTES: usize = 64 * 1024 * 1024;
const PANICKED: &str = "a write job panicked";

type Reply = Box<dyn FnOnce(std::result::Result<(), String>) + Send>;
type Job = Box<dyn for<'a, 'txn> FnOnce(&mut Tx<'a, 'txn>) -> (Reply, bool) + Send>;

pub(crate) struct Store {
    db: Arc<Database>,
    memory: Arc<Mutex<Memory>>,
    writer: Mutex<Option<Sender<Job>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Why the writer can no longer write; set once.
    failure: Arc<OnceLock<String>>,
}

impl Store {
    /// Opens or creates the data directory, checks the file's format, loads queues and
    /// consumers, and fails the deliveries still leased from the last run.
    pub fn open(dir: &Path) -> Result<Store> {
        std::fs::create_dir_all(dir).map_err(|err| {
            Error::Storage(format!(
                "failed to create data directory {}: {err}",
                dir.display()
            ))
        })?;
        let db = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create(dir.join(FILE_NAME))
            .map_err(|err| match err {
                redb::DatabaseError::DatabaseAlreadyOpen => Error::Storage(format!(
                    "{} is in use by another broker",
                    dir.join(FILE_NAME).display()
                )),
                err => err.into(),
            })?;
        format::prepare(&db, &dir.join(FILE_NAME))?;
        Store::start(db)
    }

    fn start(db: Database) -> Result<Store> {
        let db = Arc::new(db);
        let mut memory = load(&db)?;
        recover(&db, &mut memory)?;
        let memory = Arc::new(Mutex::new(memory));
        let failure = Arc::new(OnceLock::new());

        let (sender, receiver) = mpsc::channel();
        let (writer_db, writer_memory) = (Arc::clone(&db), Arc::clone(&memory));
        let writer_failure = Arc::clone(&failure);
        let thread = std::thread::Builder::new()
            .name("berthq-writer".to_string())
            .spawn(move || write_loop(&writer_db, &writer_memory, &receiver, &writer_failure))?;

        Ok(Store {
            db,
            memory,
            writer: Mutex::new(Some(sender)),
            thread: Mutex::new(Some(thread)),
            failure,
        })
    }

    pub fn memory(&self) -> MutexGuard<'_, Memory> {
        lock(&self.memory)
    }

    /// Runs `job` in the next write transaction; returns its result once committed.
    pub async fn run<T, F>(&self, job: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a, 'txn> FnOnce(&mut Tx<'a, 'txn>) -> Result<T> + Send + 'static,
    {
        if let Some(failure) = self.failure() {
            return Err(Error::Storage(failure));
        }
        let (sender, receiver) = oneshot::channel();
        let sent = match &*lock(&self.writer) {
            Some(writer) => writer.send(answering(job, sender)).is_ok(),
            None => return Err(Error::Unavailable),
        };
        if !sent {
            return Err(self.dropped());
        }
        receiver.await.unwrap_or_else(|_| Err(self.dropped()))
    }

    /// Set once the writer can no longer write: the error every write gets until a restart.
    pub fn failure(&self) -> Option<String> {
        self.failure.get().map(|reason| stopped(reason))
    }

    /// The writer answers every job it runs, so a job dropped unanswered means the writer has
    /// failed, or is unwinding from a panic that it records a moment later.
    fn dropped(&self) -> Error {
        Error::Storage(stopped(self.failure.get().map_or(PANICKED, String::as_str)))
    }

    /// A consistent snapshot for reads that don't need the writer.
    pub fn read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    /// Stops the writer after it has committed every job already sent.
    pub fn close(&self) {
        lock(&self.writer).take();
        if let Some(thread) = lock(&self.thread).take() {
            // A writer that panicked was logged when it stopped.
            let _ = thread.join();
        }
    }
}

/// Ignores lock poisoning: core code doesn't panic (the lints deny it), and refusing every
/// request after one would turn a bug into an outage.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Wraps a caller's job so that it answers through `sender` once its batch is committed.
fn answering<T, F>(job: F, sender: oneshot::Sender<Result<T>>) -> Job
where
    T: Send + 'static,
    F: for<'a, 'txn> FnOnce(&mut Tx<'a, 'txn>) -> Result<T> + Send + 'static,
{
    Box::new(move |tx| {
        let result = job(tx);
        let failed = result.as_ref().is_err_and(Error::is_storage);
        let reply: Reply = Box::new(move |commit| {
            let _ = sender.send(commit.map_err(Error::Storage).and(result));
        });
        (reply, failed)
    })
}

fn write_loop(
    db: &Database,
    memory: &Mutex<Memory>,
    receiver: &Receiver<Job>,
    failure: &OnceLock<String>,
) {
    let _panicked = PanicGuard(failure);
    while let Ok(first) = receiver.recv() {
        let mut jobs = vec![first];
        while jobs.len() < MAX_BATCH {
            match receiver.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        let (replies, result, wake) = run_batch(db, memory, jobs, failure);
        if result.is_err() {
            // Memory may have been changed by jobs whose writes were lost; start over from disk.
            match load(db) {
                Ok(fresh) => restore(&mut lock(memory), fresh),
                Err(err) => tracing::error!("failed to reload state from storage: {err}"),
            }
        }
        for reply in replies {
            reply(result.clone());
        }
        let memory = lock(memory);
        for name in wake {
            if let Some(queue) = memory.queues.get(&name)
                && !queue.settings.paused
            {
                queue.notify.notify_waiters();
            }
        }
    }
}

/// When redb can't start the transaction, the jobs are dropped unanswered once `failure` is set,
/// so their callers read the reason from it.
fn run_batch(
    db: &Database,
    memory: &Mutex<Memory>,
    jobs: Vec<Job>,
    failure: &OnceLock<String>,
) -> (Vec<Reply>, std::result::Result<(), String>, Vec<String>) {
    let txn = match db.begin_write() {
        Ok(txn) => txn,
        Err(err) => return (Vec::new(), Err(stop(failure, &err)), Vec::new()),
    };
    let mut replies = Vec::with_capacity(jobs.len());
    let mut failed = None;
    let mut wake = Vec::new();
    {
        let mut tables = match Tables::open(&txn) {
            Ok(tables) => tables,
            Err(err) => return (Vec::new(), Err(stop(failure, &err)), Vec::new()),
        };
        let mut memory = lock(memory);
        let mut tx = Tx::new(&mut tables, &mut memory, time::now_ms());
        for job in jobs {
            let (reply, job_failed) = job(&mut tx);
            if job_failed && failed.is_none() {
                failed = Some("a job failed with a storage error".to_string());
            }
            replies.push(reply);
        }
        if let Err(err) = tx.save_counts() {
            failed = Some(err.to_string());
        }
        wake.extend(std::mem::take(&mut tx.wake));
    }
    (replies, finish(txn, failed, failure), wake)
}

/// Commits the batch, or rolls it back when a job failed with a storage error.
fn finish(
    txn: WriteTransaction,
    failed: Option<String>,
    failure: &OnceLock<String>,
) -> std::result::Result<(), String> {
    if let Some(err) = failed {
        tracing::error!("failed to write to storage: {err}");
        let _ = txn.abort();
        return Err(err);
    }
    match txn.commit() {
        Ok(()) => Ok(()),
        // redb rolls a poisoned transaction back cleanly; after any other commit error it
        // refuses writes until the file is reopened.
        Err(err @ CommitError::TransactionPoisoned) => {
            tracing::error!("failed to write to storage: {err}");
            Err(err.to_string())
        }
        Err(err) => Err(stop(failure, &err)),
    }
}

/// Records why the writer failed and logs it, both only the first time. Returns the error for
/// the jobs that are waiting.
fn stop(failure: &OnceLock<String>, reason: &dyn Display) -> String {
    let reason = reason.to_string();
    if failure.set(reason.clone()).is_ok() {
        tracing::error!("{}", stopped(&reason));
    }
    stopped(failure.get().unwrap_or(&reason))
}

fn stopped(reason: &str) -> String {
    format!("writes stopped after an error, restart berthq: {reason}")
}

/// Records the failure when a job panics, which unwinds and ends the writer thread.
struct PanicGuard<'a>(&'a OnceLock<String>);

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            stop(self.0, &PANICKED);
        }
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
