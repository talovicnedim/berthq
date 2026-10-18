//! The data file's format version, stored in the `meta` table. A new file gets the current
//! version; files from a newer Berthq or from older development builds are refused rather than
//! misread. Formats a release has written will be upgraded here.

use std::path::Path;

use redb::{Database, ReadableTable, TableHandle};

use crate::error::{Error, Result};
use crate::tables::{META, Tables};

/// Bumped whenever the tables or records change in a way older builds can't read.
pub(crate) const FORMAT: u64 = 2;
const FORMAT_KEY: &str = "format";

/// Checks the file's format and creates any missing tables. The format is read before the other
/// tables are opened, because opening a table whose layout changed fails.
pub(crate) fn prepare(db: &Database, path: &Path) -> Result<()> {
    let txn = db.begin_write()?;
    let names: Vec<String> = txn
        .list_tables()?
        .map(|table| table.name().to_string())
        .collect();
    if !names.is_empty() && !names.iter().any(|name| name == "meta") {
        return Err(Error::Storage(format!(
            "{} was made by a development build of Berthq from before format versions: move it \
             away or delete it to start with an empty data directory",
            path.display()
        )));
    }
    let saved = txn
        .open_table(META)?
        .get(FORMAT_KEY)?
        .map(|value| value.value());
    match saved {
        None | Some(FORMAT) => {}
        Some(format) if format > FORMAT => {
            return Err(Error::Storage(format!(
                "{} has format {format}, made by a newer Berthq; this one reads format \
                 {FORMAT}: upgrade Berthq",
                path.display()
            )));
        }
        Some(format) => {
            return Err(Error::Storage(format!(
                "{} has format {format}, made by an older development build of Berthq that this \
                 one can't read: move it away or delete it to start with an empty data directory",
                path.display()
            )));
        }
    }
    {
        let mut tables = Tables::open(&txn)?;
        if saved.is_none() {
            tables.meta.insert(FORMAT_KEY, FORMAT)?;
        }
    }
    txn.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase, TableDefinition};

    use super::*;
    use crate::testing::TempDir;

    #[test]
    fn new_files_get_the_current_format() {
        let dir = TempDir::new("format-new");
        let path = dir.0.join("berthq.redb");
        let db = Database::create(&path).unwrap();
        prepare(&db, &path).unwrap();
        let txn = db.begin_read().unwrap();
        let format = txn.open_table(META).unwrap().get(FORMAT_KEY).unwrap();
        assert_eq!(format.map(|v| v.value()), Some(FORMAT));
        prepare(&db, &path).unwrap();
    }

    #[test]
    fn files_from_before_format_versions_are_refused() {
        let dir = TempDir::new("format-old");
        let path = dir.0.join("berthq.redb");
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        let old: TableDefinition<&str, &[u8]> = TableDefinition::new("ready");
        txn.open_table(old)
            .unwrap()
            .insert("x", b"y".as_slice())
            .unwrap();
        txn.commit().unwrap();
        let err = prepare(&db, &path).unwrap_err().to_string();
        assert!(err.contains("development build"), "{err}");
    }

    #[test]
    fn files_from_an_older_development_build_are_refused() {
        let dir = TempDir::new("format-older");
        let path = dir.0.join("berthq.redb");
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(META)
            .unwrap()
            .insert(FORMAT_KEY, FORMAT - 1)
            .unwrap();
        txn.commit().unwrap();
        let err = prepare(&db, &path).unwrap_err().to_string();
        assert!(err.contains("older development build"), "{err}");
    }

    #[test]
    fn files_from_a_newer_berthq_are_refused() {
        let dir = TempDir::new("format-newer");
        let path = dir.0.join("berthq.redb");
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(META)
            .unwrap()
            .insert(FORMAT_KEY, FORMAT + 1)
            .unwrap();
        txn.commit().unwrap();
        let err = prepare(&db, &path).unwrap_err().to_string();
        assert!(err.contains("newer Berthq"), "{err}");
    }
}
