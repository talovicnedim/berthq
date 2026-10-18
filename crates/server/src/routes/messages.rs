use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use berthq_core::{Broker, MessageInfo, MessageState, NewMessage, Requeued};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiResult;
use crate::json::{Json, Messages, Query};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListQuery {
    state: Option<MessageState>,
    limit: Option<u32>,
    before: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Batch {
    messages: Vec<NewMessage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequeueRequest {
    /// Dead letters to move; all of them when missing.
    ids: Option<Vec<String>>,
}

/// 201 when stored, 200 when it was a duplicate and nothing was stored.
pub async fn publish(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Messages(message): Messages<NewMessage>,
) -> ApiResult<impl IntoResponse> {
    let published = broker.publish(&queue, message).await?;
    let status = if published.duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, axum::Json(published)))
}

pub async fn publish_batch(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Messages(batch): Messages<Batch>,
) -> ApiResult<impl IntoResponse> {
    let published = broker.publish_batch(&queue, batch.messages).await?;
    Ok((
        StatusCode::CREATED,
        axum::Json(json!({ "messages": published })),
    ))
}

pub async fn list(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<axum::Json<Value>> {
    let before = query.before.as_deref();
    let messages = broker.messages_list(&queue, query.state, query.limit, before)?;
    Ok(axum::Json(json!({ "messages": messages })))
}

pub async fn get(
    State(broker): State<Broker>,
    Path((queue, id)): Path<(String, String)>,
) -> ApiResult<axum::Json<MessageInfo>> {
    Ok(axum::Json(broker.messages_get(&queue, &id)?))
}

pub async fn remove(
    State(broker): State<Broker>,
    Path((queue, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    broker.messages_remove(&queue, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn requeue(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(request): Json<RequeueRequest>,
) -> ApiResult<axum::Json<Requeued>> {
    Ok(axum::Json(broker.requeue(&queue, request.ids).await?))
}
