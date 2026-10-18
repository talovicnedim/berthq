//! Downloading a backup of the broker's database.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Method;
use tokio::io::AsyncWriteExt;

use crate::{Client, Error, Result, api_error};

impl Client {
    /// Saves a consistent copy of the broker's database to `path` (it must not exist). Returns
    /// the number of bytes written. Needs an admin key. The copy is downloaded to
    /// `<path>.partial` and renamed once it is complete, so `path` never holds half a backup.
    pub async fn backup(&self, path: &Path) -> Result<u64> {
        let exists = tokio::fs::try_exists(path)
            .await
            .map_err(|err| Error::Http(format!("failed to check {}: {err}", path.display())))?;
        if exists {
            return Err(Error::Http(format!("{} already exists", path.display())));
        }
        let request = self
            .request(Method::GET, "/v1/backup")
            .timeout(Duration::from_secs(3600));
        let response = request.send().await.map_err(|err| self.transport(&err))?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        let partial = partial_path(path);
        let saved = self.save(response, &partial, path).await;
        if saved.is_err() {
            let _ = tokio::fs::remove_file(&partial).await;
        }
        saved
    }

    /// Streams the backup into `partial`, then moves it to `path` once all of it is on disk.
    async fn save(
        &self,
        mut response: reqwest::Response,
        partial: &Path,
        path: &Path,
    ) -> Result<u64> {
        let mut file = tokio::fs::File::create(partial)
            .await
            .map_err(|err| failed(partial, err))?;
        let mut written = 0;
        while let Some(chunk) = response.chunk().await.map_err(|err| self.transport(&err))? {
            file.write_all(&chunk)
                .await
                .map_err(|err| failed(partial, err))?;
            written += chunk.len() as u64;
        }
        file.sync_all().await.map_err(|err| failed(partial, err))?;
        // Windows can't rename a file that is still open.
        drop(file);
        tokio::fs::rename(partial, path)
            .await
            .map_err(|err| failed(path, err))?;
        Ok(written)
    }
}

fn partial_path(path: &Path) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(".partial");
    PathBuf::from(name)
}

fn failed(path: &Path, err: std::io::Error) -> Error {
    Error::Http(format!("failed to write {}: {err}", path.display()))
}
