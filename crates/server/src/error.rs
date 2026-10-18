use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// An error sent as `{ "error": { "kind", "message" } }` with a matching status code.
#[derive(Debug)]
pub struct ApiError(berthq_core::Error);

impl ApiError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self(berthq_core::Error::Invalid(message.into()))
    }

    pub fn unknown_route() -> Self {
        Self(berthq_core::Error::NotFound("api route".to_string()))
    }
}

impl From<berthq_core::Error> for ApiError {
    fn from(err: berthq_core::Error) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use berthq_core::Error;
        let status = match &self.0 {
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::QueueFull(_) | Error::DiskFull(_) => StatusCode::INSUFFICIENT_STORAGE,
            Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Error::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.0.to_string();
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!("{message}");
        }
        let body = json!({ "error": { "kind": self.0.kind(), "message": message } });
        (status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
