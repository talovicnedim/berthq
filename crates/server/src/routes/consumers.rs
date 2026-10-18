use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use berthq_core::{Broker, ConsumerInfo, ConsumerMessageState, NewConsumer};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiResult;
use crate::json::{Json, Query};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReplayRequest {
    from: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesQuery {
    state: Option<ConsumerMessageState>,
    limit: Option<u32>,
}

pub async fn list(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
) -> ApiResult<axum::Json<Value>> {
    let consumers = broker.consumers_list(&queue)?;
    Ok(axum::Json(json!({ "consumers": consumers })))
}

pub async fn create(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(new): Json<NewConsumer>,
) -> ApiResult<impl IntoResponse> {
    let consumer = broker.consumers_create(&queue, new).await?;
    Ok((StatusCode::CREATED, axum::Json(consumer)))
}

pub async fn get(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
) -> ApiResult<axum::Json<ConsumerInfo>> {
    Ok(axum::Json(broker.consumers_get(&queue, &consumer)?))
}

pub async fn remove(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    broker.consumers_remove(&queue, &consumer).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn replay(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
    Json(request): Json<ReplayRequest>,
) -> ApiResult<axum::Json<ConsumerInfo>> {
    let from = request.from.as_deref();
    Ok(axum::Json(
        broker.consumers_replay(&queue, &consumer, from).await?,
    ))
}

pub async fn pause(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
) -> ApiResult<axum::Json<ConsumerInfo>> {
    Ok(axum::Json(broker.consumers_pause(&queue, &consumer).await?))
}

pub async fn resume(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
) -> ApiResult<axum::Json<ConsumerInfo>> {
    Ok(axum::Json(
        broker.consumers_resume(&queue, &consumer).await?,
    ))
}

pub async fn messages(
    State(broker): State<Broker>,
    Path((queue, consumer)): Path<(String, String)>,
    Query(query): Query<MessagesQuery>,
) -> ApiResult<axum::Json<Value>> {
    let messages = broker.consumers_messages(&queue, &consumer, query.state, query.limit)?;
    Ok(axum::Json(json!({ "messages": messages })))
}
