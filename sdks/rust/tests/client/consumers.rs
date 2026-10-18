use super::*;

#[tokio::test]
async fn dead_letters_come_back_to_their_consumer_with_their_id() {
    let server = start("dlq", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client.queues_create("jobs", &settings(1)).await.unwrap();
    client
        .publish("jobs", &NewMessage::new("work"))
        .await
        .unwrap();
    let delivery = client
        .consume("jobs", &ConsumeOptions::default())
        .await
        .unwrap()
        .messages
        .remove(0);
    let nack = NackOptions {
        leases: vec![delivery.lease.clone().unwrap()],
        error: Some("boom".to_string()),
        ..NackOptions::default()
    };
    client.nack("jobs", &nack).await.unwrap();

    let dead = client
        .messages_list("jobs.dlq", None, None, None)
        .await
        .unwrap();
    assert_eq!(dead[0].last_error.as_deref(), Some("boom"));
    assert_eq!(
        dead[0].dead_letter.as_ref().unwrap().message_id,
        delivery.id
    );
    assert_eq!(client.requeue("jobs.dlq", None).await.unwrap().requeued, 1);
    let retrying = client
        .consumers_messages(
            "jobs",
            "default",
            Some(ConsumerMessageState::Retrying),
            None,
        )
        .await
        .unwrap();
    assert_eq!(retrying.len(), 1);
    let again = client
        .consume("jobs", &ConsumeOptions::default())
        .await
        .unwrap()
        .messages;
    assert_eq!(again[0].id, delivery.id);
    server.stop().await;
}

#[tokio::test]
async fn consumers_each_get_every_message_and_replay() {
    let server = start("consumers", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("orders", &QueueSettingsPatch::default())
        .await
        .unwrap();
    client
        .publish("orders", &NewMessage::new(json!({ "id": 7 })))
        .await
        .unwrap();
    client
        .consumers_create("orders", "audit", Some("start"))
        .await
        .unwrap();
    for consumer in ["billing", "audit"] {
        let options = ConsumeOptions {
            consumer: Some(consumer.to_string()),
            auto_ack: true,
            ..ConsumeOptions::default()
        };
        let got = client.consume("orders", &options).await.unwrap().messages;
        assert_eq!(got[0].body, json!({ "id": 7 }));
        assert!(got[0].lease.is_none());
    }
    let replayed = client
        .consumers_replay("orders", "billing", "start")
        .await
        .unwrap();
    assert_eq!(replayed.stats.waiting, 1);
    let names: Vec<_> = client
        .consumers_list("orders")
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(names, ["audit", "billing"]);
    client.consumers_remove("orders", "audit").await.unwrap();
    let err = client.consumers_get("orders", "audit").await.unwrap_err();
    assert_eq!(err.kind(), "notFound");
    server.stop().await;
}
