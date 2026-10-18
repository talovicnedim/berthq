//! The JSON API under `/v1`. Handlers are thin adapters over `berthq-core`, named like the core
//! function they call.

mod backup;
mod consumers;
mod delivery;
mod messages;
mod queues;
mod stats;

use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use berthq_core::{Broker, Error};

use crate::error::ApiError;

pub fn router() -> Router<Broker> {
    Router::new()
        .route("/queues", get(queues::list).post(queues::create))
        .route(
            "/queues/{queue}",
            get(queues::get)
                .patch(queues::update)
                .delete(queues::remove),
        )
        .route("/queues/{queue}/purge", post(queues::purge))
        .route("/queues/{queue}/pause", post(queues::pause))
        .route("/queues/{queue}/resume", post(queues::resume))
        .route(
            "/queues/{queue}/messages",
            get(messages::list).post(messages::publish),
        )
        .route("/queues/{queue}/batch", post(messages::publish_batch))
        .route(
            "/queues/{queue}/messages/{id}",
            get(messages::get).delete(messages::remove),
        )
        .route("/queues/{queue}/requeue", post(messages::requeue))
        .route("/queues/{queue}/consume", post(delivery::consume))
        .route("/queues/{queue}/ack", post(delivery::ack))
        .route("/queues/{queue}/nack", post(delivery::nack))
        .route("/queues/{queue}/extend", post(delivery::extend))
        .route(
            "/queues/{queue}/consumers",
            get(consumers::list).post(consumers::create),
        )
        .route(
            "/queues/{queue}/consumers/{consumer}",
            get(consumers::get).delete(consumers::remove),
        )
        .route(
            "/queues/{queue}/consumers/{consumer}/replay",
            post(consumers::replay),
        )
        .route(
            "/queues/{queue}/consumers/{consumer}/pause",
            post(consumers::pause),
        )
        .route(
            "/queues/{queue}/consumers/{consumer}/resume",
            post(consumers::resume),
        )
        .route(
            "/queues/{queue}/consumers/{consumer}/messages",
            get(consumers::messages),
        )
        .route("/backup", get(backup::backup))
        .route("/stats", get(stats::overview))
        .route("/events", get(stats::events))
        // Unknown API paths answer in JSON, not with the dashboard's HTML.
        .fallback(|| async { ApiError::unknown_route() })
        .method_not_allowed_fallback(method_not_allowed)
}

/// 405 with the usual JSON error body.
async fn method_not_allowed() -> Response {
    let error = ApiError::from(Error::Invalid(
        "method not allowed on this route".to_string(),
    ));
    let mut response = error.into_response();
    *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
    response
}
