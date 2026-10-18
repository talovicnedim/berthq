use super::*;

#[tokio::test]
async fn errors_use_the_json_envelope() {
    let app = app("errors", &[]).await;
    let mistyped = json!({ "name": "a", "maxAtempts": 3 });
    let (status, body) = app
        .call(Call::new(Method::POST, "/v1/queues").json(mistyped))
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a mistyped setting is rejected: {body}"
    );
    assert_eq!(body["error"]["kind"], "invalid");
    let removed = json!({ "name": "a", "ttlMs": 1000 });
    let (status, _) = app
        .call(Call::new(Method::POST, "/v1/queues").json(removed))
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "removed settings are unknown"
    );

    app.queue("a").await;
    let (status, body) = app
        .call(Call::new(Method::GET, "/v1/queues/a/messages?state=bogus"))
        .await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid"))
    );
    let (status, body) = app.call(Call::new(Method::GET, "/v1/nothing/here")).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::NOT_FOUND, json!("notFound"))
    );
    let (status, body) = app
        .call(Call::new(Method::PUT, "/v1/queues/a/consume"))
        .await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::METHOD_NOT_ALLOWED, json!("invalid"))
    );
    app.stop().await;
}

#[tokio::test]
async fn publish_answers_201_and_200_for_duplicates_and_consume_names_the_consumer() {
    let app = app("publish", &[]).await;
    app.queue("a").await;
    let message = json!({ "body": { "n": 1 }, "dedupId": "x" });
    let publish = || Call::new(Method::POST, "/v1/queues/a/messages").json(message.clone());
    let (first, _) = app.call(publish()).await;
    let (second, body) = app.call(publish()).await;
    assert_eq!((first, second), (StatusCode::CREATED, StatusCode::OK));
    assert_eq!(body["duplicate"], true);

    let consume = json!({ "consumer": "billing", "worker": "w1" });
    let (_, body) = app
        .call(Call::new(Method::POST, "/v1/queues/a/consume").json(consume))
        .await;
    let delivery = &body["messages"][0];
    assert_eq!(delivery["consumer"], "billing");
    assert_eq!(delivery["dedupId"], "x");
    assert!(delivery["lease"].as_str().unwrap().ends_with(".billing"));
    assert_eq!(
        (body["settled"].clone(), body["unknown"].clone()),
        (json!(0), json!([]))
    );

    let ack = json!({ "consumer": "billing", "ack": [delivery["lease"]] });
    let (status, body) = app
        .call(Call::new(Method::POST, "/v1/queues/a/consume").json(ack))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "messages": [], "settled": 1, "unknown": [] }));
    app.stop().await;
}

#[tokio::test]
async fn a_delay_past_a_year_is_rejected_and_publishing_keeps_working() {
    let app = app("huge-delay", &[]).await;
    app.queue("a").await;
    let publish = |message: Value| Call::new(Method::POST, "/v1/queues/a/messages").json(message);
    let (status, body) = app
        .call(publish(json!({ "body": 1, "delayMs": u64::MAX })))
        .await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid")),
        "{body}"
    );
    let (status, body) = app.call(publish(json!({ "body": 2 }))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    app.stop().await;
}

#[tokio::test]
async fn api_typos_get_the_json_404_and_dashboard_routes_do_not() {
    let app = app("typos", &[]).await;
    for uri in [
        "/metrics/",
        "/healthz/",
        "/v2/queues",
        "/v10",
        "//v1/queues",
    ] {
        let (status, body) = app.call(Call::new(Method::GET, uri)).await;
        assert_eq!(
            (status, body["error"]["kind"].clone()),
            (StatusCode::NOT_FOUND, json!("notFound")),
            "{uri}"
        );
    }
    for uri in ["/", "/queues/emails", "/version"] {
        let (_, body) = app.call(Call::new(Method::GET, uri)).await;
        assert!(body["error"].is_null(), "{uri}: {body}");
    }
    app.stop().await;
}

#[tokio::test]
async fn dashboard_pages_cannot_be_framed_or_sniffed() {
    let app = app("dashboard-headers", &[]).await;
    let request = Request::get("/").body(Body::empty()).unwrap();
    let response = app.router.clone().oneshot(request).await.unwrap();
    let headers = response.headers();
    assert_eq!(headers["x-frame-options"], "DENY");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    app.stop().await;
}

#[tokio::test]
async fn bodies_come_back_exactly_as_sent() {
    let app = app("as-sent", &[]).await;
    app.queue("a").await;
    let body = r#"{ "b": [1.50, 2e3, "é"], "a": 12345678901234567890123 }"#;
    let publish = Call::new(Method::POST, "/v1/queues/a/messages");
    let (status, published) = app
        .call(publish.raw(format!(r#"{{"body": {body}}}"#)))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");

    let path = format!(
        "/v1/queues/a/messages/{}",
        published["id"].as_str().unwrap()
    );
    let (_, got) = app.send(Call::new(Method::GET, &path)).await;
    let (_, consumed) = app
        .send(Call::new(Method::POST, "/v1/queues/a/consume"))
        .await;
    let size = serde_json::from_slice::<Value>(&got).unwrap()["size"].clone();
    assert_eq!(size, json!(body.len()), "the size counts the body as sent");
    for answer in [got, consumed] {
        let text = String::from_utf8(answer).unwrap();
        assert!(text.contains(&format!(r#""body":{body}"#)), "{text}");
    }
    app.stop().await;
}

#[tokio::test]
async fn a_body_that_is_not_json_is_refused() {
    let app = app("not-json", &[]).await;
    app.queue("a").await;
    let requests: [&'static [u8]; 5] = [
        br#"{"body": [1,]}"#,
        br#"{"body": nope}"#,
        br#"{"body": }"#,
        b"{\"body\": \"\xff\"}",
        b"{}",
    ];
    for request in requests {
        let publish = Call::new(Method::POST, "/v1/queues/a/messages").raw(request);
        let (status, body) = app.call(publish).await;
        assert_eq!(
            (status, body["error"]["kind"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid")),
            "{}",
            String::from_utf8_lossy(request)
        );
    }
    app.stop().await;
}

#[tokio::test]
async fn bodies_that_json_parsers_refuse_are_refused() {
    let app = app("unreadable", &[]).await;
    app.queue("a").await;
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    for body in ["1e400", r#""\ud800""#, &deep] {
        let publish = Call::new(Method::POST, "/v1/queues/a/messages");
        let (status, answer) = app.call(publish.raw(format!(r#"{{"body":{body}}}"#))).await;
        assert_eq!(
            (status, answer["error"]["kind"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid")),
            "{}",
            &body[..body.len().min(20)]
        );
    }
    app.stop().await;
}
