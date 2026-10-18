use berthq_core::MAX_BODY_BYTES;

use super::*;

/// The longest lease: a 128-character consumer name and the largest attempt and token.
fn longest_lease(n: usize) -> String {
    format!(
        "01J0000000000000000000{n:04}.4294967295.ffffffffffffffff.{}",
        "c".repeat(128)
    )
}

#[tokio::test]
async fn acks_fit_1000_leases_and_bigger_requests_are_refused() {
    let app = app("limit-ack", &[]).await;
    app.queue("a").await;
    let ack = |count: usize| {
        let leases: Vec<String> = (0..count).map(longest_lease).collect();
        Call::new(Method::POST, "/v1/queues/a/ack").json(json!({ "leases": leases }))
    };
    let (status, body) = app.call(ack(1000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = app.call(ack(10_000)).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid")),
        "1.8 MB of leases"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_largest_message_fits_and_bigger_single_publishes_are_refused() {
    let app = app("limit-publish", &[]).await;
    app.queue("a").await;
    let largest = "x".repeat(MAX_BODY_BYTES - 2);
    let publish =
        |body: &str| Call::new(Method::POST, "/v1/queues/a/messages").json(json!({ "body": body }));
    assert_eq!(app.call(publish(&largest)).await.0, StatusCode::CREATED);
    let (status, body) = app.call(publish(&"x".repeat(2 * MAX_BODY_BYTES))).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid"))
    );
    let batch =
        json!({ "messages": [{ "body": largest }, { "body": largest }, { "body": largest }] });
    let batch = Call::new(Method::POST, "/v1/queues/a/batch").json(batch);
    assert_eq!(
        app.call(batch).await.0,
        StatusCode::CREATED,
        "a batch takes more"
    );
    app.stop().await;
}

#[tokio::test]
async fn escapes_count_against_the_body_limit_as_sent() {
    let app = app("limit-escaped", &[]).await;
    app.queue("a").await;
    // Python's `json.dumps` sends "é" as `\u00e9`: 6 bytes as sent, where "é" itself is 2.
    let publish = |escapes: usize| {
        let text = format!(r#"{{"body":"{}"}}"#, "\\u00e9".repeat(escapes));
        Call::new(Method::POST, "/v1/queues/a/messages").raw(text)
    };
    let fits = (MAX_BODY_BYTES - 2) / 6;
    let (status, body) = app.call(publish(fits)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = app.call(publish(fits + 1)).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid")),
        "{body}"
    );
    app.stop().await;
}

#[tokio::test]
async fn stalled_uploads_do_not_hold_up_other_requests() {
    let app = app("limit-stalled", &[]).await;
    app.queue("a").await;
    for _ in 0..4 {
        let never = futures_util::stream::pending::<Result<axum::body::Bytes, std::io::Error>>();
        let stalled = Request::post("/v1/queues/a/batch")
            .body(Body::from_stream(never))
            .unwrap();
        tokio::spawn(app.router.clone().oneshot(stalled));
    }
    // The test runtime has one thread: this lets the uploads start first.
    tokio::task::yield_now().await;
    let others = [
        Call::new(Method::POST, "/v1/queues/a/messages").json(json!({ "body": 1 })),
        Call::new(Method::POST, "/v1/queues/a/ack").json(json!({ "leases": [] })),
    ];
    for call in others {
        let answer = tokio::time::timeout(std::time::Duration::from_secs(10), app.call(call));
        assert!(answer.await.unwrap().0.is_success());
    }
    app.stop().await;
}
