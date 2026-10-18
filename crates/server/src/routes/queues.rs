use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use berthq_core::{Broker, NewQueue, QueueDetails, QueueInfo, QueueSettingsPatch};
use serde_json::{Value, json};

use crate::error::{ApiError, ApiResult};
use crate::json::Json;

pub async fn list(State(broker): State<Broker>) -> axum::Json<Value> {
    axum::Json(json!({ "queues": broker.queues_list() }))
}

/// Body: `{ name, ...settings }`. Parsed in two steps because `deny_unknown_fields` doesn't
/// work through `#[serde(flatten)]`, and a mistyped setting must not be ignored silently.
pub async fn create(
    State(broker): State<Broker>,
    Json(mut body): Json<serde_json::Map<String, Value>>,
) -> ApiResult<impl IntoResponse> {
    let name = match body.remove("name") {
        Some(Value::String(name)) => name,
        _ => return Err(ApiError::invalid("missing queue name")),
    };
    let settings: QueueSettingsPatch = serde_json::from_value(Value::Object(body))
        .map_err(|err| ApiError::invalid(format!("invalid request body: {err}")))?;
    let info = broker.queues_create(NewQueue { name, settings }).await?;
    Ok((StatusCode::CREATED, axum::Json(info)))
}

pub async fn get(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<axum::Json<QueueDetails>> {
    Ok(axum::Json(broker.queues_get(&queue)?))
}

pub async fn update(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(patch): Json<QueueSettingsPatch>,
) -> ApiResult<axum::Json<QueueInfo>> {
    Ok(axum::Json(broker.queues_update(&queue, patch).await?))
}

pub async fn remove(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<StatusCode> {
    broker.queues_remove(&queue).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn purge(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<axum::Json<Value>> {
    let purged = broker.queues_purge(&queue).await?;
    Ok(axum::Json(json!({ "purged": purged })))
}

pub async fn pause(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<axum::Json<QueueInfo>> {
    Ok(axum::Json(broker.queues_pause(&queue).await?))
}

pub async fn resume(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<axum::Json<QueueInfo>> {
    Ok(axum::Json(broker.queues_resume(&queue).await?))
}
