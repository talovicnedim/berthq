use super::*;

#[tokio::test]
async fn consumers_are_created_listed_replayed_and_removed() {
    let app = app("consumers", &[]).await;
    app.queue("orders").await;
    for n in 1..=2 {
        let publish =
            Call::new(Method::POST, "/v1/queues/orders/messages").json(json!({ "body": n }));
        app.call(publish).await;
    }
    let create = || {
        Call::new(Method::POST, "/v1/queues/orders/consumers").json(json!({ "name": "billing" }))
    };
    let (status, consumer) = app.call(create()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        consumer["stats"],
        json!({ "waiting": 2, "processing": 0, "retrying": 0 })
    );
    assert_eq!(consumer["idle"], false);
    assert_eq!(app.call(create()).await.0, StatusCode::CONFLICT);

    let consume = Call::new(Method::POST, "/v1/queues/orders/consume")
        .json(json!({ "consumer": "billing", "max": 1 }));
    app.call(consume).await;
    let (_, processing) = app
        .call(Call::new(
            Method::GET,
            "/v1/queues/orders/consumers/billing/messages",
        ))
        .await;
    assert_eq!(processing["messages"][0]["state"], "processing");
    let (_, waiting) = app
        .call(Call::new(
            Method::GET,
            "/v1/queues/orders/consumers/billing/messages?state=waiting",
        ))
        .await;
    assert_eq!(waiting["messages"].as_array().unwrap().len(), 1);

    let replay = Call::new(Method::POST, "/v1/queues/orders/consumers/billing/replay")
        .json(json!({ "from": "start" }));
    let (status, replayed) = app.call(replay).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        replayed["stats"],
        json!({ "waiting": 2, "processing": 0, "retrying": 0 })
    );
    let bad = Call::new(Method::POST, "/v1/queues/orders/consumers/billing/replay")
        .json(json!({ "from": "soon" }));
    assert_eq!(app.call(bad).await.0, StatusCode::BAD_REQUEST);

    let (_, list) = app
        .call(Call::new(Method::GET, "/v1/queues/orders/consumers"))
        .await;
    assert_eq!(list["consumers"][0]["name"], "billing");
    let remove = Call::new(Method::DELETE, "/v1/queues/orders/consumers/billing");
    assert_eq!(app.call(remove).await.0, StatusCode::NO_CONTENT);
    app.stop().await;
}
