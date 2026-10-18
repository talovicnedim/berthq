//! Free disk space: publishing stops while the disk is almost full, so acks, deletes and
//! retention still have room to write and the backlog can drain.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

use serde::Serialize;

use crate::error::{Error, Result};

const MB: u64 = 1024 * 1024;
const GB: u64 = 1024 * MB;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskStats {
    /// Bytes free on the disk that holds the data directory.
    pub free: u64,
    /// Publishing stops below this many free bytes; 0 turns the check off.
    pub min_free: u64,
    /// True while publishes are refused.
    pub full: bool,
}

pub(crate) struct Disk {
    min_free: u64,
    free: AtomicU64,
    /// The last measurement failed: logged once, not every second.
    failing: AtomicBool,
}

impl Disk {
    /// Measures the free space once. A disk that can't be measured is an error unless the check
    /// is off, because publishing could otherwise fill it unnoticed.
    pub(crate) fn open(dir: &Path, min_free: u64) -> Result<Disk> {
        let free = match fs4::available_space(dir) {
            Ok(free) => free,
            Err(_) if min_free == 0 => 0,
            Err(err) => {
                return Err(Error::Storage(format!(
                    "failed to read the free space of {}: {err} (--min-free-disk 0 turns this \
                     check off)",
                    dir.display()
                )));
            }
        };
        let disk = Disk::new(min_free, free);
        if disk.is_full() {
            tracing::warn!("{}", disk.full_message());
        }
        Ok(disk)
    }

    /// Measures again (once a second) and logs when publishing stops or works again.
    pub(crate) fn measure(&self, dir: &Path) {
        match fs4::available_space(dir) {
            Ok(free) => {
                self.failing.store(false, Relaxed);
                self.update(free);
            }
            Err(err) => {
                if !self.failing.swap(true, Relaxed) {
                    tracing::warn!("failed to read the free space of {}: {err}", dir.display());
                }
            }
        }
    }

    /// Fails with `DiskFull` while the free space is below the minimum.
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_full() {
            return Err(Error::DiskFull(self.full_message()));
        }
        Ok(())
    }

    /// Fails with `DiskFull` unless a backup of `bytes` still leaves the minimum free.
    pub(crate) fn check_backup(&self, bytes: u64) -> Result<()> {
        let free = self.free.load(Relaxed);
        if self.min_free == 0 || free >= self.min_free.saturating_add(bytes) {
            return Ok(());
        }
        Err(Error::DiskFull(format!(
            "not enough disk space for a backup: the copy takes up to {}, Berthq keeps {} free \
             (--min-free-disk) and {} is free; free some space and try again",
            size(bytes),
            size(self.min_free),
            size(free)
        )))
    }

    pub(crate) fn stats(&self) -> DiskStats {
        DiskStats {
            free: self.free.load(Relaxed),
            min_free: self.min_free,
            full: self.is_full(),
        }
    }

    fn new(min_free: u64, free: u64) -> Disk {
        Disk {
            min_free,
            free: AtomicU64::new(free),
            failing: AtomicBool::new(false),
        }
    }

    fn update(&self, free: u64) {
        let was_full = self.is_full();
        self.free.store(free, Relaxed);
        match (was_full, self.is_full()) {
            (false, true) => tracing::warn!("{}", self.full_message()),
            (true, false) => tracing::info!("the disk has room again: publishing works again"),
            _ => {}
        }
    }

    fn is_full(&self) -> bool {
        self.free.load(Relaxed) < self.min_free
    }

    fn full_message(&self) -> String {
        format!(
            "the disk is almost full: {} free, Berthq keeps {} free (--min-free-disk); publishing \
             works again once space is freed",
            size(self.free.load(Relaxed)),
            size(self.min_free)
        )
    }
}

fn size(bytes: u64) -> String {
    if bytes < GB {
        return format!("{} MB", bytes / MB);
    }
    format!("{}.{} GB", bytes / GB, bytes % GB * 10 / GB)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishing_stops_below_the_minimum_and_works_again_above_it() {
        let disk = Disk::new(1000 * MB, 5000 * MB);
        assert!(disk.check().is_ok());

        disk.update(999 * MB);
        let err = disk.check().unwrap_err();
        assert_eq!(err.kind(), "diskFull");
        assert!(err.to_string().contains("999 MB free"), "{err}");
        assert!(disk.stats().full);

        disk.update(1000 * MB);
        assert!(disk.check().is_ok());
        assert!(!disk.stats().full);
    }

    #[test]
    fn sizes_read_as_mb_below_a_gb() {
        assert_eq!(size(999 * MB), "999 MB");
        assert_eq!(size(GB), "1.0 GB");
        assert_eq!(size(194 * GB + GB / 2), "194.5 GB");
    }

    #[test]
    fn a_minimum_of_zero_turns_the_check_off() {
        let disk = Disk::new(0, 0);
        assert!(disk.check().is_ok());
        assert!(disk.check_backup(GB).is_ok());
        assert!(!disk.stats().full);
    }

    #[test]
    fn a_backup_needs_room_for_its_copy_above_the_minimum() {
        let disk = Disk::new(1000 * MB, 1500 * MB);
        assert!(disk.check_backup(500 * MB).is_ok());

        let err = disk.check_backup(501 * MB).unwrap_err();
        assert_eq!(err.kind(), "diskFull");
        assert!(disk.check().is_ok(), "publishing still works");
    }

    #[test]
    fn the_data_directory_is_measured() {
        let disk = Disk::open(&std::env::temp_dir(), 1).unwrap();
        assert!(disk.stats().free > 0);
        assert_eq!(disk.stats().min_free, 1);
    }
}
