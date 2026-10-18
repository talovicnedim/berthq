//! Backups: a consistent copy of the database while the broker keeps running. Every table is
//! copied inside one read transaction, so the copy shows one moment in time.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, TryLockError};

use redb::{
    Database, Key, ReadTransaction, ReadableTable, TableDefinition, Value, WriteTransaction,
};

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::store::{CACHE_BYTES, FILE_NAME};
use crate::tables;
use crate::time;

const BACKUP_PREFIX: &str = "backup-";
const BACKUP_SUFFIX: &str = ".tmp";

/// A backup copy inside the data directory: same disk as the data, and it works with a
/// read-only root filesystem. The file is deleted when this is dropped.
#[derive(Debug)]
pub struct BackupFile(PathBuf);

impl BackupFile {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for BackupFile {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.0)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                "failed to delete the backup copy {}: {err}",
                self.0.display()
            );
        }
    }
}

impl Broker {
    /// Writes a consistent copy of the database to a new file in the data directory. One backup
    /// copies at a time (`Conflict` otherwise), and only when the copy leaves `--min-free-disk`
    /// free (`DiskFull` otherwise). Blocks while it copies: call it from a blocking thread.
    pub fn backup(&self) -> Result<BackupFile> {
        let _running = one_at_a_time(self.backup_lock())?;
        let data = self.data_dir().join(FILE_NAME);
        let size = std::fs::metadata(&data)
            .map_err(|err| Error::Storage(format!("failed to read {}: {err}", data.display())))?
            .len();
        self.disk().measure(self.data_dir());
        self.disk().check_backup(size)?;
        let file = BackupFile(self.backup_path());
        let source = self.store().read()?;
        let target = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create(file.path())?;
        let txn = target.begin_write()?;
        copy_all(&source, &txn)?;
        txn.commit()?;
        Ok(file)
    }

    fn backup_path(&self) -> PathBuf {
        let id = self.memory().ids.next(time::now_ms());
        self.data_dir()
            .join(format!("{BACKUP_PREFIX}{id}{BACKUP_SUFFIX}"))
    }
}

fn one_at_a_time(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>> {
    match lock.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::Poisoned(poisoned)) => Ok(poisoned.into_inner()),
        Err(TryLockError::WouldBlock) => Err(Error::Conflict(
            "a backup is already running; try again once it is done".to_string(),
        )),
    }
}

/// Deletes backup copies a crash left behind.
pub(crate) fn remove_leftovers(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(BACKUP_PREFIX)
            && name.ends_with(BACKUP_SUFFIX)
            && let Err(err) = std::fs::remove_file(entry.path())
        {
            tracing::warn!("failed to delete old backup {name}: {err}");
        }
    }
}

fn copy_all(from: &ReadTransaction, to: &WriteTransaction) -> Result<()> {
    copy(from, to, tables::META)?;
    copy(from, to, tables::QUEUES)?;
    copy(from, to, tables::COUNTS)?;
    copy(from, to, tables::CONSUMERS)?;
    copy(from, to, tables::MESSAGES)?;
    copy(from, to, tables::BODIES)?;
    copy(from, to, tables::LOG)?;
    copy(from, to, tables::DELAYED)?;
    copy(from, to, tables::DEDUP)?;
    copy(from, to, tables::DEDUP_EXPIRING)?;
    copy(from, to, tables::PENDING)?;
    copy(from, to, tables::P_READY)?;
    copy(from, to, tables::P_SCHEDULED)?;
    copy(from, to, tables::P_LEASED)?;
    copy(from, to, tables::P_FLOOR)?;
    copy(from, to, tables::COPIES)?;
    copy(from, to, tables::COPY_BODIES)
}

fn copy<K: Key + 'static, V: Value + 'static>(
    from: &ReadTransaction,
    to: &WriteTransaction,
    definition: TableDefinition<K, V>,
) -> Result<()> {
    let source = from.open_table(definition)?;
    let mut target = to.open_table(definition)?;
    for entry in source.iter()? {
        let (key, value) = entry?;
        target.insert(key.value(), value.value())?;
    }
    Ok(())
}
