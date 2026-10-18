use super::*;

#[tokio::test]
async fn the_publisher_keeps_order_and_isolates_a_bad_message() {
    let server = start("publisher", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("events", &QueueSettingsPatch::default())
        .await
        .unwrap();
    let publisher = client.publisher("events", PublisherOptions::default());

    let tasks: Vec<_> = (0..250)
        .map(|n| {
            let publisher = publisher.clone();
            tokio::spawn(async move { publisher.publish(NewMessage::new(n)).await })
        })
        .collect();
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap().unwrap().id);
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "in order");

    let bad = NewMessage {
        priority: Some(42),
        ..NewMessage::new("bad")
    };
    let results: Vec<_> = [NewMessage::new("a"), bad, NewMessage::new("b")]
        .into_iter()
        .map(|message| {
            let publisher = publisher.clone();
            tokio::spawn(async move { publisher.publish(message).await })
        })
        .collect();
    let mut kinds = Vec::new();
    for task in results {
        kinds.push(
            task.await
                .unwrap()
                .map_or_else(|err| err.kind().to_string(), |_| "ok".into()),
        );
    }
    assert_eq!(kinds, ["ok", "invalid", "ok"]);

    publisher.close().await.unwrap();
    let stats = client.queues_get("events").await.unwrap().info.stats;
    assert_eq!(stats.counters.published, 252);
    server.stop().await;
}
