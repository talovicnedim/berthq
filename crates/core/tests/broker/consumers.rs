use super::*;

#[tokio::test]
async fn dead_letters_come_back_to_their_consumer_with_the_original_id() {
    let dir = temp_dir("dlq");
    let settings = QueueSettingsPatch {
        max_attempts: Some(1),
        ..QueueSettingsPatch::default()
    };
    let broker = open_with_queue(&dir, "jobs", settings).await;
    broker.publish("jobs", message(json!(1))).await.unwrap();
    let delivery = broker
        .consume("jobs", consume("billing", 1))
        .await
        .unwrap()
        .messages
        .remove(0);
    let nack = NackOptions {
        leases: vec![delivery.lease.clone().unwrap()],
        error: Some("smtp timeout".to_string()),
        ..NackOptions::default()
    };
    broker.nack("jobs", nack).await.unwrap();

    let dead = broker.messages_list("jobs.dlq", None, None, None).unwrap();
    assert_eq!(dead[0].last_error.as_deref(), Some("smtp timeout"));
    let info = dead[0].dead_letter.as_ref().unwrap();
    assert_eq!(
        (info.consumer.as_str(), info.message_id.as_str()),
        ("billing", delivery.id.as_str())
    );

    assert_eq!(broker.requeue("jobs.dlq", None).await.unwrap().requeued, 1);
    let again = broker
        .consume("jobs", consume("billing", 1))
        .await
        .unwrap()
        .messages;
    assert_eq!(again[0].id, delivery.id);
    assert_eq!(again[0].attempt, 1);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn consumers_each_get_every_message_and_can_replay() {
    let dir = temp_dir("replay");
    let broker = open_with_queue(&dir, "orders", QueueSettingsPatch::default()).await;
    for n in 1..=3 {
        broker.publish("orders", message(json!(n))).await.unwrap();
    }
    for consumer in ["billing", "audit"] {
        let options = ConsumeOptions {
            auto_ack: true,
            ..consume(consumer, 10)
        };
        assert_eq!(
            broker
                .consume("orders", options)
                .await
                .unwrap()
                .messages
                .len(),
            3
        );
    }
    let start = berthq_core::time::format(berthq_core::time::now_ms() - 60_000);
    broker
        .consumers_replay("orders", "billing", Some(&start))
        .await
        .unwrap();
    let info = broker.consumers_get("orders", "billing").unwrap();
    assert_eq!(info.stats.waiting, 3);
    let replayed = broker
        .consume("orders", consume("billing", 10))
        .await
        .unwrap()
        .messages;
    assert_eq!(replayed.len(), 3);
    let audit = broker
        .consume("orders", consume("audit", 10))
        .await
        .unwrap()
        .messages;
    assert!(audit.is_empty(), "replay moves only that consumer");
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_replay_to_a_message_no_longer_kept_drops_nothing() {
    let dir = temp_dir("replay-gone");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    let gone = broker.publish("jobs", message(json!(1))).await.unwrap().id;
    broker.publish("jobs", message(json!(2))).await.unwrap();
    broker.messages_remove("jobs", &gone).await.unwrap();
    broker.consume("jobs", consume("billing", 1)).await.unwrap();

    let replayed = broker
        .consumers_replay("jobs", "billing", Some(&gone))
        .await;
    assert!(matches!(replayed, Err(Error::Invalid(_))), "{replayed:?}");
    let stats = broker.consumers_get("jobs", "billing").unwrap().stats;
    assert_eq!(stats.processing, 1);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_replay_finishes_when_its_message_is_deleted_midway() {
    let dir = temp_dir("replay-deleted");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    let mut ids = Vec::new();
    for n in 1..=3 {
        ids.push(broker.publish("jobs", message(json!(n))).await.unwrap().id);
    }
    broker.consume("jobs", consume("billing", 3)).await.unwrap();

    let replay = {
        let (broker, from) = (broker.clone(), ids[1].clone());
        tokio::spawn(async move {
            broker
                .consumers_replay("jobs", "billing", Some(&from))
                .await
        })
    };
    // Lets the replay send its first job, so the delete runs after it.
    tokio::task::yield_now().await;
    broker.messages_remove("jobs", &ids[1]).await.unwrap();

    let replayed = replay.await.unwrap();
    assert!(replayed.is_ok(), "{replayed:?}");
    let again = broker
        .consume("jobs", consume("billing", 10))
        .await
        .unwrap()
        .messages;
    let bodies: Vec<_> = again.iter().map(|d| d.body.get()).collect();
    assert_eq!(bodies, ["3"]);
    broker.shutdown(Duration::ZERO).await;
}
