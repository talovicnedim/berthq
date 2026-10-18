use super::*;

#[tokio::test]
async fn publish_consume_ack_round_trip() {
    let dir = temp_dir("round-trip");
    let broker = open_with_queue(&dir, "emails", QueueSettingsPatch::default()).await;
    let published = broker
        .publish("emails", message(json!({ "user": 123 })))
        .await
        .unwrap();

    let deliveries = broker
        .consume("emails", ConsumeOptions::default())
        .await
        .unwrap()
        .messages;
    assert_eq!(deliveries[0].id, published.id);
    assert_eq!(deliveries[0].consumer, "default");
    assert_eq!(deliveries[0].body.get(), r#"{"user":123}"#);
    let lease = deliveries[0].lease.clone().unwrap();
    assert_eq!(broker.ack("emails", vec![lease]).await.unwrap().settled, 1);

    let stats = broker.queues_get("emails").unwrap().info.stats;
    assert_eq!((stats.messages, stats.backlog), (1, 0), "kept for replay");
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn long_poll_wakes_up_when_a_message_arrives() {
    let dir = temp_dir("long-poll");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    let consumer = {
        let broker = broker.clone();
        let options = ConsumeOptions {
            wait_ms: Some(10_000),
            ..ConsumeOptions::default()
        };
        tokio::spawn(async move { broker.consume("jobs", options).await })
    };
    tokio::task::yield_now().await;
    broker.publish("jobs", message(json!("hi"))).await.unwrap();

    let deliveries = tokio::time::timeout(Duration::from_secs(5), consumer)
        .await
        .expect("worker should wake up long before its wait ends")
        .unwrap()
        .unwrap();
    assert_eq!(deliveries.messages[0].body.get(), r#""hi""#);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn consume_acks_the_last_batch_and_takes_the_next_in_one_call() {
    let dir = temp_dir("consume-ack");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    for n in 1..=2 {
        broker.publish("jobs", message(json!(n))).await.unwrap();
    }
    let first = broker.consume("jobs", consume("default", 1)).await.unwrap();
    let options = ConsumeOptions {
        ack: vec![
            first.messages[0].lease.clone().unwrap(),
            "stale.1.0.default".to_string(),
        ],
        ..consume("default", 1)
    };
    let next = broker.consume("jobs", options).await.unwrap();
    assert_eq!(next.messages[0].body.get(), "2");
    assert_eq!(next.acked.settled, 1);
    assert_eq!(next.acked.unknown, ["stale.1.0.default"]);
    let consumer = broker.consumers_get("jobs", "default").unwrap();
    assert_eq!(consumer.stats.processing, 1);

    let too_many = ConsumeOptions {
        ack: vec!["x".to_string(); berthq_core::MAX_ACK + 1],
        ..consume("default", 1)
    };
    assert!(matches!(
        broker.consume("jobs", too_many).await,
        Err(Error::Invalid(_))
    ));
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn acks_are_on_disk_while_the_consume_call_waits() {
    let dir = temp_dir("consume-ack-wait");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    broker.publish("jobs", message(json!(1))).await.unwrap();
    let first = broker.consume("jobs", consume("default", 1)).await.unwrap();
    let waiting = {
        let broker = broker.clone();
        let options = ConsumeOptions {
            ack: vec![first.messages[0].lease.clone().unwrap()],
            wait_ms: Some(30_000),
            ..consume("default", 1)
        };
        tokio::spawn(async move { broker.consume("jobs", options).await })
    };
    // The processing view reads a committed snapshot: empty means the ack is on disk.
    let processing = || {
        let state = Some(berthq_core::ConsumerMessageState::Processing);
        broker
            .consumers_messages("jobs", "default", state, None)
            .unwrap()
    };
    let acked = async {
        while !processing().is_empty() {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), acked)
        .await
        .expect("the ack is saved before the call starts waiting");
    assert!(
        !waiting.is_finished(),
        "the call is still waiting for messages"
    );

    broker.publish("jobs", message(json!(2))).await.unwrap();
    let woke = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((woke.acked.settled, woke.messages[0].body.get()), (1, "2"));
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn browsing_lists_newest_first_and_pages_back() {
    let dir = temp_dir("browse");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    for n in 1..=3 {
        broker.publish("jobs", message(json!(n))).await.unwrap();
    }
    let delayed = NewMessage {
        delay_ms: Some(60_000),
        ..message(json!("later"))
    };
    broker.publish("jobs", delayed).await.unwrap();

    let page = broker.messages_list("jobs", None, Some(2), None).unwrap();
    let older = broker
        .messages_list("jobs", None, Some(2), Some(&page[1].id))
        .unwrap();
    let ids: Vec<_> = page.iter().chain(&older).map(|m| m.id.clone()).collect();
    let bodies: Vec<_> = ids
        .iter()
        .map(|id| broker.messages_get("jobs", id).unwrap().body.unwrap())
        .map(|body| body.get().to_string())
        .collect();
    assert_eq!(bodies, ["3", "2", "1"]);
    let waiting = broker
        .messages_list("jobs", Some(MessageState::Delayed), None, None)
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert!(waiting[0].deliver_at.is_some());
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn nack_delays_past_a_year_are_rejected() {
    const YEAR_MS: u64 = 365 * 24 * 60 * 60 * 1000;
    let dir = temp_dir("nack-delay");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    broker.publish("jobs", message(json!(1))).await.unwrap();
    let delivered = broker.consume("jobs", consume("default", 1)).await.unwrap();
    let lease = delivered.messages[0].lease.clone().unwrap();
    let nack = |delay_ms| NackOptions {
        leases: vec![lease.clone()],
        delay_ms: Some(delay_ms),
        ..NackOptions::default()
    };
    for too_late in [YEAR_MS + 1, u64::MAX] {
        let err = broker.nack("jobs", nack(too_late)).await.unwrap_err();
        assert_eq!(err.kind(), "invalid", "{err}");
    }
    assert_eq!(broker.nack("jobs", nack(YEAR_MS)).await.unwrap().settled, 1);
    let consumer = broker.consumers_get("jobs", "default").unwrap();
    assert_eq!(consumer.stats.retrying, 1);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn one_call_settles_or_requeues_at_most_1000_items() {
    let dir = temp_dir("settle-cap");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    broker.publish("jobs", message(json!(1))).await.unwrap();
    let lease = broker
        .consume("jobs", consume("default", 1))
        .await
        .unwrap()
        .messages[0]
        .lease
        .clone()
        .unwrap();
    let nack = NackOptions {
        leases: vec![lease],
        dead_letter: true,
        ..NackOptions::default()
    };
    broker.nack("jobs", nack).await.unwrap();
    let most = vec!["stale.1.0.default".to_string(); berthq_core::MAX_ACK];
    let too_many = vec!["stale.1.0.default".to_string(); berthq_core::MAX_ACK + 1];

    assert!(broker.ack("jobs", most.clone()).await.is_ok());
    assert!(broker.extend("jobs", most.clone(), None).await.is_ok());
    let nack = |leases: Vec<String>| NackOptions {
        leases,
        ..NackOptions::default()
    };
    assert!(broker.nack("jobs", nack(most.clone())).await.is_ok());
    let ids = vec!["01J00000000000000000000000".to_string(); berthq_core::MAX_REQUEUE];
    assert!(broker.requeue("jobs.dlq", Some(ids.clone())).await.is_ok());
    let refused = [
        broker.ack("jobs", too_many.clone()).await,
        broker.extend("jobs", too_many.clone(), None).await,
        broker.nack("jobs", nack(too_many.clone())).await,
    ];
    for result in refused {
        let settled = result.map(|settled| settled.settled);
        assert!(matches!(settled, Err(Error::Invalid(_))), "{settled:?}");
    }
    let mut ids = ids;
    ids.push(ids[0].clone());
    let requeue = broker.requeue("jobs.dlq", Some(ids)).await;
    assert!(matches!(requeue, Err(Error::Invalid(_))), "{requeue:?}");
    broker.shutdown(Duration::ZERO).await;
}
