use std::convert::Infallible;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use berthq_core::{Broker, Overview};
use futures_util::Stream;
use serde_json::json;

pub async fn overview(State(broker): State<Broker>) -> axum::Json<Overview> {
    axum::Json(broker.overview())
}

/// Sends a `stats` event with the overview and every queue right away, then once a second.
pub async fn events(
    State(broker): State<Broker>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = futures_util::stream::unfold(true, move |first| {
        let broker = broker.clone();
        async move {
            if !first {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let data = json!({ "overview": broker.overview(), "queues": broker.queues_list() });
            let event = Event::default().event("stats").data(data.to_string());
            Some((Ok(event), false))
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
