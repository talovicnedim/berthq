use super::*;

#[tokio::test]
async fn api_keys_and_scopes_are_enforced() {
    let app = app(
        "auth",
        &[
            "adminadminadmin1",
            "publish:publishpublish12",
            "read:readreadreadread",
        ],
    )
    .await;
    let (status, body) = app.call(Call::new(Method::GET, "/v1/queues")).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::UNAUTHORIZED, json!("unauthorized"))
    );

    let create = Call::new(Method::POST, "/v1/queues").json(json!({ "name": "a" }));
    assert_eq!(
        app.call(create.key("adminadminadmin1")).await.0,
        StatusCode::CREATED
    );
    let publish = Call::new(Method::POST, "/v1/queues/a/messages").json(json!({ "body": 1 }));
    assert_eq!(
        app.call(publish.key("publishpublish12")).await.0,
        StatusCode::CREATED
    );
    let other_header = Call::new(Method::GET, "/v1/queues").header("x-api-key", "adminadminadmin1");
    assert_eq!(
        app.call(other_header).await.0,
        StatusCode::UNAUTHORIZED,
        "keys come as Bearer tokens"
    );

    let forbidden = [
        Call::new(Method::POST, "/v1/queues/a/consume").key("publishpublish12"),
        Call::new(Method::POST, "/v1/queues/a/consumers")
            .json(json!({ "name": "x" }))
            .key("publishpublish12"),
        Call::new(Method::GET, "/v1/backup").key("readreadreadread"),
    ];
    for call in forbidden {
        let uri = call.uri.to_string();
        let (status, body) = app.call(call).await;
        assert_eq!(
            (status, body["error"]["kind"].clone()),
            (StatusCode::FORBIDDEN, json!("forbidden")),
            "{uri}"
        );
    }
    let query_key = Call::new(Method::GET, "/v1/queues?apiKey=readreadreadread");
    assert_eq!(
        app.call(query_key).await.0,
        StatusCode::UNAUTHORIZED,
        "query keys only for events"
    );
    let events = Call::new(Method::GET, "/v1/events?apiKey=readreadreadread");
    assert_eq!(app.status(events).await, StatusCode::OK);

    let (status, _) = app.call(Call::new(Method::GET, "/healthz")).await;
    assert_eq!(status, StatusCode::OK, "health checks need no key");
    app.stop().await;
}

#[tokio::test]
async fn query_keys_work_only_for_reading_v1_events() {
    let app = app("auth-query", &["adminadminadmin1", "read:readreadreadread"]).await;
    let create = Call::new(Method::POST, "/v1/queues").json(json!({ "name": "events" }));
    assert_eq!(
        app.call(create.key("adminadminadmin1")).await.0,
        StatusCode::CREATED
    );
    let refused = [
        (Method::DELETE, "/v1/queues/events?apiKey=adminadminadmin1"),
        (Method::GET, "/v1/queues/events?apiKey=readreadreadread"),
        (Method::POST, "/v1/events?apiKey=adminadminadmin1"),
    ];
    for (method, uri) in refused {
        let (status, body) = app.call(Call::new(method.clone(), uri)).await;
        assert_eq!(
            (status, body["error"]["kind"].clone()),
            (StatusCode::UNAUTHORIZED, json!("unauthorized")),
            "{method} {uri}"
        );
    }
    let events = Call::new(Method::GET, "/v1/events?apiKey=readreadreadread");
    assert_eq!(app.status(events).await, StatusCode::OK);
    let (status, _) = app
        .call(Call::new(Method::GET, "/v1/queues/events").key("readreadreadread"))
        .await;
    assert_eq!(status, StatusCode::OK, "the queue is still there");
    app.stop().await;
}

#[tokio::test]
async fn head_bearer_case_and_plus_in_query_keys_work() {
    let app = app("auth-forms", &["read:read+key+with+plus"]).await;
    let head = Call::new(Method::HEAD, "/v1/queues").key("read+key+with+plus");
    assert_eq!(
        app.status(head).await,
        StatusCode::OK,
        "HEAD needs what GET needs"
    );
    let lower =
        Call::new(Method::GET, "/v1/queues").header("authorization", "bearer read+key+with+plus");
    assert_eq!(
        app.status(lower).await,
        StatusCode::OK,
        "the scheme is case-insensitive"
    );
    let events = Call::new(Method::GET, "/v1/events?apiKey=read+key+with+plus");
    assert_eq!(app.status(events).await, StatusCode::OK, "+ stays a plus");
    app.stop().await;
}
