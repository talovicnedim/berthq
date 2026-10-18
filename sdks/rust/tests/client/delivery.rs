use super::*;

#[tokio::test]
async fn publish_consume_ack() {
    let server = start("round-trip", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("emails", &QueueSettingsPatch::default())
        .await
        .unwrap();

    let published = client
        .publish("emails", &NewMessage::new(json!({ "user": 1 })))
        .await
        .unwrap();
    let deliveries = client
        .consume("emails", &ConsumeOptions::default())
        .await
        .unwrap()
        .messages;
    assert_eq!(deliveries[0].id, published.id);
    assert_eq!(deliveries[0].body, json!({ "user": 1 }));
    assert_eq!(deliveries[0].visibility_timeout_ms, 30_000);
    assert_eq!(deliveries[0].consumer, "default");
    let lease = deliveries[0].lease.clone().unwrap();
    assert_eq!(client.ack("emails", &[lease]).await.unwrap().settled, 1);

    let err = client.queues_get("missing").await.unwrap_err();
    assert_eq!(err.kind(), "notFound");
    server.stop().await;
}

#[tokio::test]
async fn consume_acks_the_last_batch_on_the_way() {
    let server = start("ack-in-consume", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    for n in 1..=2 {
        client.publish("jobs", &NewMessage::new(n)).await.unwrap();
    }
    let first = client
        .consume("jobs", &ConsumeOptions::default())
        .await
        .unwrap();
    let lease = first.messages[0].lease.clone().unwrap();

    let next = ConsumeOptions {
        ack: vec![lease.clone()],
        ..ConsumeOptions::default()
    };
    let second = client.consume("jobs", &next).await.unwrap();
    assert_eq!((second.settled, second.messages.len()), (1, 1));
    assert_eq!(second.messages[0].body, json!(2));

    let again = client.consume("jobs", &next).await.unwrap();
    assert_eq!((again.settled, again.unknown), (0, vec![lease]));
    let stats = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(stats.counters.acked, 1);
    server.stop().await;
}

#[tokio::test]
async fn process_acks_successes_and_nacks_failures() {
    let server = start("process", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client.queues_create("jobs", &settings(1)).await.unwrap();
    for body in ["ok", "fail"] {
        client
            .publish("jobs", &NewMessage::new(body))
            .await
            .unwrap();
    }

    let (done, stopped) = tokio::sync::oneshot::channel::<()>();
    let done = std::sync::Mutex::new(Some(done));
    let handled = std::sync::atomic::AtomicU32::new(0);
    let stop = async {
        let _ = stopped.await;
    };
    client
        .process("jobs", Default::default(), stop, |delivery| {
            let count = handled.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if count == 2 {
                done.lock().unwrap().take().map(|done| done.send(()));
            }
            async move {
                match delivery.body.as_str() {
                    Some("fail") => Err("could not do it"),
                    _ => Ok(()),
                }
            }
        })
        .await
        .unwrap();

    let jobs = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(jobs.counters.acked, 1);
    let dead = client
        .messages_list("jobs.dlq", None, None, None)
        .await
        .unwrap();
    assert_eq!(dead[0].last_error.as_deref(), Some("could not do it"));
    server.stop().await;
}

#[tokio::test]
async fn process_stops_after_its_limit_and_leases_no_more() {
    let server = start("limit", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    for n in 1..=3 {
        client.publish("jobs", &NewMessage::new(n)).await.unwrap();
    }
    let options = ProcessOptions {
        limit: Some(2),
        ..ProcessOptions::default()
    };
    client
        .process("jobs", options, std::future::pending(), |_| async {
            Ok::<(), String>(())
        })
        .await
        .unwrap();

    let stats = client.consumers_get("jobs", "default").await.unwrap().stats;
    assert_eq!((stats.waiting, stats.processing), (1, 0));
    server.stop().await;
}

#[tokio::test]
async fn process_keeps_going_while_the_broker_restarts() {
    let server = start("restart", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client.queues_create("jobs", &settings(5)).await.unwrap();
    client
        .publish("jobs", &NewMessage::new("job"))
        .await
        .unwrap();

    let (restart, restarting) = oneshot::channel::<()>();
    let restart = std::sync::Mutex::new(Some(restart));
    let back = tokio::sync::Notify::new();
    let (done, finished) = oneshot::channel::<()>();
    let done = std::sync::Mutex::new(Some(done));
    let attempts = std::sync::Mutex::new(Vec::new());
    let options = ProcessOptions {
        visibility_timeout_ms: Some(1000),
        ..ProcessOptions::default()
    };
    let stop = async {
        let _ = finished.await;
    };
    let worker = client.process("jobs", options, stop, |delivery| {
        attempts.lock().unwrap().push(delivery.attempt);
        let restart = restart.lock().unwrap().take();
        let done = if restart.is_none() {
            done.lock().unwrap().take()
        } else {
            None
        };
        let back = &back;
        async move {
            if let Some(restart) = restart {
                let _ = restart.send(());
                back.notified().await;
            }
            done.map(|done| done.send(()));
            Ok::<(), String>(())
        }
    });
    let restarter = async {
        let _ = restarting.await;
        let server = server.restart().await;
        back.notify_one();
        server
    };
    let run = async {
        tokio::pin!(worker);
        let server = tokio::select! {
            result = &mut worker => panic!("the worker stopped while the broker was down: {result:?}"),
            server = restarter => server,
        };
        (worker.await, server)
    };
    let (result, server) = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the worker finishes");

    result.unwrap();
    assert_eq!(
        *attempts.lock().unwrap(),
        [1, 2],
        "the restart lost the lease"
    );
    let stats = client.consumers_get("jobs", "default").await.unwrap().stats;
    assert_eq!((stats.waiting, stats.processing, stats.retrying), (0, 0, 0));
    let jobs = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(jobs.counters.acked, 1);
    server.stop().await;
}

#[tokio::test]
async fn process_acks_finished_messages_when_it_stops_with_an_error() {
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::Value;

    // A broker that refuses every nack with an error retrying can't fix.
    let acked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/v1/queues/jobs/consume",
            post(|| async {
                let messages = [fake_delivery("ok"), fake_delivery("fail")];
                Json(json!({ "messages": messages, "settled": 0 }))
            }),
        )
        .route(
            "/v1/queues/jobs/nack",
            post(|| async {
                let error = json!({ "error": { "kind": "invalid", "message": "refused" } });
                (axum::http::StatusCode::BAD_REQUEST, Json(error))
            }),
        )
        .route(
            "/v1/queues/jobs/ack",
            post({
                let acked = acked.clone();
                move |Json(request): Json<Value>| async move {
                    acked.lock().unwrap().push(request["leases"].clone());
                    Json(json!({ "settled": 1, "unknown": [] }))
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let err = client
        .process(
            "jobs",
            Default::default(),
            std::future::pending(),
            |delivery| async move {
                match delivery.body.as_str() {
                    Some("fail") => Err("could not do it"),
                    _ => Ok(()),
                }
            },
        )
        .await
        .unwrap_err();

    assert_eq!(err.kind(), "invalid");
    assert_eq!(*acked.lock().unwrap(), [json!(["lease-ok"])]);
}

fn fake_delivery(body: &str) -> serde_json::Value {
    json!({
        "id": body, "lease": format!("lease-{body}"), "queue": "jobs", "consumer": "default",
        "body": body, "priority": 0, "attempt": 1, "publishedAt": "2026-10-17T00:00:00Z",
        "leaseExpiresAt": null, "visibilityTimeoutMs": 30000, "dedupId": null, "lastError": null
    })
}
