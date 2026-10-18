use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redb::StorageBackend;
use redb::backends::InMemoryBackend;

use super::*;
use crate::broker::Broker;
use crate::memory::QueueMemory;
use crate::publish::check;
use crate::queues::NewQueue;
use crate::settings::QueueSettings;
use crate::testing::{TempDir, body};

#[tokio::test]
async fn a_job_that_panics_fails_later_writes_with_a_storage_error() {
    let dir = TempDir::new("panic");
    let broker = Broker::open(&dir.0, 0).unwrap();
    let panicked = broker.store().run(|_| -> Result<()> { panic!("a bug") });
    assert_eq!(panicked.await.unwrap_err().kind(), "storage");

    let new = NewQueue {
        name: "jobs".to_string(),
        settings: Default::default(),
    };
    let err = broker.queues_create(new).await.unwrap_err();
    assert_eq!(err.kind(), "storage", "{err}");
    let failure = broker.storage_failure().unwrap();
    assert!(failure.contains("panicked"), "{failure}");
    assert!(broker.queues_list().is_empty(), "reads keep working");
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn an_io_error_on_commit_fails_every_later_write() {
    let disk = FlakyDisk::default();
    let broken = Arc::clone(&disk.broken);
    let db = Database::builder().create_with_backend(disk).unwrap();
    format::prepare(&db, Path::new("in memory")).unwrap();
    let store = Store::start(db).unwrap();
    store.run(create("first")).await.unwrap();

    broken.store(true, Ordering::SeqCst);
    let err = store.run(create("second")).await.unwrap_err();
    assert_eq!(err.kind(), "storage", "{err}");
    broken.store(false, Ordering::SeqCst);
    let err = store.run(create("third")).await.unwrap_err();
    assert_eq!(err.kind(), "storage", "{err}");
    let failure = store.failure().unwrap();
    assert!(failure.contains("disk unplugged"), "{failure}");
    store.close();
}

#[test]
fn a_failed_batch_leaves_no_queue_it_created() {
    let dir = TempDir::new("ghost");
    let db = Database::create(dir.0.join(FILE_NAME)).unwrap();
    format::prepare(&db, &dir.0).unwrap();
    let memory = Mutex::new(load(&db).unwrap());
    let checked = vec![check(body(1), 0).unwrap()];
    let publish = job(move |tx| tx.publish("jobs", checked).map(drop));
    let (_, results) = run_together(&db, &memory, vec![job(create("jobs")), publish]);
    assert!(results.iter().all(Result::is_ok));

    let disk_error = job(|_| Err(Error::Storage("disk on fire".to_string())));
    let (failure, results) = run_together(&db, &memory, vec![job(create("ghost")), disk_error]);
    assert_eq!(results[0].as_ref().unwrap_err().kind(), "storage");
    assert!(
        failure.get().is_none(),
        "one failed job doesn't stop the writer"
    );
    let memory = lock(&memory);
    assert!(memory.queue("ghost").is_err());
    let jobs = memory.queue("jobs").unwrap();
    assert_eq!(
        jobs.counters.published, 1,
        "throughput lives only in memory"
    );
}

type Answer = oneshot::Receiver<Result<()>>;

/// Wraps `work` like `Store::run` does, with the receiver for its answer.
fn job<F>(work: F) -> (Job, Answer)
where
    F: for<'a, 'txn> FnOnce(&mut Tx<'a, 'txn>) -> Result<()> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    (answering(work, sender), receiver)
}

/// Runs the jobs in one batch on this thread, the way the writer thread does.
fn run_together(
    db: &Database,
    memory: &Mutex<Memory>,
    jobs: Vec<(Job, Answer)>,
) -> (OnceLock<String>, Vec<Result<()>>) {
    let failure = OnceLock::new();
    let (sender, receiver) = mpsc::channel();
    let mut answers = Vec::new();
    for (job, answer) in jobs {
        sender.send(job).unwrap();
        answers.push(answer);
    }
    drop(sender);
    write_loop(db, memory, &receiver, &failure);
    let results = answers
        .into_iter()
        .map(|answer| answer.blocking_recv().unwrap())
        .collect();
    (failure, results)
}

fn create(
    name: &'static str,
) -> impl for<'a, 'txn> FnOnce(&mut Tx<'a, 'txn>) -> Result<()> + Send + 'static {
    move |tx| tx.create_queue(QueueMemory::new(name, QueueSettings::default(), tx.now))
}

/// Memory that fails to flush while `broken` is set, like a disk that went away.
#[derive(Debug, Default)]
struct FlakyDisk {
    data: InMemoryBackend,
    broken: Arc<AtomicBool>,
}

impl StorageBackend for FlakyDisk {
    fn len(&self) -> io::Result<u64> {
        self.data.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.data.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.data.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(io::Error::other("disk unplugged"));
        }
        self.data.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.data.write(offset, data)
    }
}
