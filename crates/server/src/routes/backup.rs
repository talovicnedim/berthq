//! `GET /v1/backup`: a consistent copy of the database, streamed as a file. The copy is written
//! inside the data directory first and deleted once sent (or when the download is dropped).

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use berthq_core::{BackupFile, Broker, Error};
use tokio::io::AsyncReadExt;

use crate::error::{ApiError, ApiResult};

const CHUNK: usize = 64 * 1024;

pub async fn backup(State(broker): State<Broker>) -> ApiResult<Response> {
    // A client that goes away doesn't stop the copy; tokio then drops the task's result, which
    // deletes the file.
    let copy = tokio::task::spawn_blocking(move || broker.backup())
        .await
        .map_err(|err| storage(format!("backup failed: {err}")))??;
    let file = tokio::fs::File::open(copy.path())
        .await
        .map_err(|err| storage(format!("failed to open the backup: {err}")))?;
    let size = file
        .metadata()
        .await
        .map_err(|err| storage(format!("failed to read the backup: {err}")))?
        .len();
    let name = format!(
        "attachment; filename=\"berthq-{}.redb\"",
        berthq_core::time::format(berthq_core::time::now_ms()).replace(':', "-")
    );
    let headers = [
        (header::CONTENT_TYPE, "application/octet-stream".to_string()),
        (header::CONTENT_LENGTH, size.to_string()),
        (header::CONTENT_DISPOSITION, name),
    ];
    Ok((headers, Body::from_stream(chunks(file, copy))).into_response())
}

/// Reads the file in chunks; the copy, dropped with the stream, deletes it afterwards.
fn chunks(
    file: tokio::fs::File,
    copy: BackupFile,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> {
    futures_util::stream::unfold(Some((file, copy)), |state| async move {
        let (mut file, copy) = state?;
        let mut buffer = vec![0; CHUNK];
        match file.read(&mut buffer).await {
            Ok(0) => {
                // Closed before it is deleted: some systems can't delete an open file.
                drop(file);
                drop(copy);
                None
            }
            Ok(read) => {
                buffer.truncate(read);
                Some((Ok(Bytes::from(buffer)), Some((file, copy))))
            }
            Err(err) => Some((Err(err), None)),
        }
    })
}

fn storage(message: String) -> ApiError {
    ApiError::from(Error::Storage(message))
}
