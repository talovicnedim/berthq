//! Request extractors that report problems in the API's JSON error format, and the size limits
//! for request bodies.

use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::error::ApiError;

/// Every route but publishing: a consume or ack with 1000 leases is about 190 KB.
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
/// A publish or a batch, which can hold several bodies of up to 4 MB.
pub const MAX_PUBLISH_BYTES: usize = 16 * 1024 * 1024;

/// A JSON request body of up to [`MAX_REQUEST_BYTES`] that ignores `Content-Type`, so
/// `curl -d '{…}'` works without `-H 'Content-Type: application/json'`. An empty body reads
/// as `{}`.
pub struct Json<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for Json<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let bytes = read(request, MAX_REQUEST_BYTES).await?;
        parse(&bytes).map(Json)
    }
}

/// A body with messages to publish, up to [`MAX_PUBLISH_BYTES`], read like [`Json`].
pub struct Messages<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for Messages<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let bytes = read(request, MAX_PUBLISH_BYTES).await?;
        parse(&bytes).map(Messages)
    }
}

/// Query parameters, with errors as `{ "error": { "kind": "invalid", … } }`.
pub struct Query<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequestParts<S> for Query<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Query::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(value)| Query(value))
            .map_err(|err| ApiError::invalid(err.body_text()))
    }
}

async fn read(request: Request, max: usize) -> Result<Bytes, ApiError> {
    axum::body::to_bytes(request.into_body(), max)
        .await
        .map_err(|err| ApiError::invalid(format!("failed to read the request body: {err}")))
}

fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    let bytes: &[u8] = if bytes.iter().all(u8::is_ascii_whitespace) {
        b"{}"
    } else {
        bytes
    };
    serde_json::from_slice(bytes)
        .map_err(|err| ApiError::invalid(format!("invalid request body: {err}")))
}
