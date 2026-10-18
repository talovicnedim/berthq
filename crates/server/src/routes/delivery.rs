use axum::extract::{Path, State};
use berthq_core::{Broker, ConsumeOptions, Consumed, NackOptions, Settled};
use serde::Deserialize;

use crate::error::ApiResult;
use crate::json::Json;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Leases {
    leases: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExtendRequest {
    leases: Vec<String>,
    visibility_timeout_ms: Option<u64>,
}

pub async fn consume(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(options): Json<ConsumeOptions>,
) -> ApiResult<axum::Json<Consumed>> {
    Ok(axum::Json(broker.consume(&queue, options).await?))
}

pub async fn ack(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(request): Json<Leases>,
) -> ApiResult<axum::Json<Settled>> {
    Ok(axum::Json(broker.ack(&queue, request.leases).await?))
}

pub async fn nack(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(options): Json<NackOptions>,
) -> ApiResult<axum::Json<Settled>> {
    Ok(axum::Json(broker.nack(&queue, options).await?))
}

pub async fn extend(
    State(broker): State<Broker>,
    Path(queue): Path<String>,
    Json(request): Json<ExtendRequest>,
) -> ApiResult<axum::Json<Settled>> {
    let settled = broker
        .extend(&queue, request.leases, request.visibility_timeout_ms)
        .await?;
    Ok(axum::Json(settled))
}
