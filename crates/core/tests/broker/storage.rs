use super::*;

#[tokio::test]
async fn positions_and_unacked_messages_survive_a_restart() {
    let dir = temp_dir("restart");
    let no_backoff = QueueSettingsPatch {
        retry_delay_ms: Some(0),
        ..QueueSettingsPatch::default()
    };
    let broker = open_with_queue(&dir, "jobs", no_backoff).await;
    let dedup = NewMessage {
        dedup_id: Some("order-7".to_string()),
        ..message(json!(1))
    };
    let first = broker.publish("jobs", dedup.clone()).await.unwrap();
    broker.publish("jobs", message(json!(2))).await.unwrap();
    broker.publish("jobs", message(json!(3))).await.unwrap();
    let done = broker
        .consume("jobs", consume("default", 1))
        .await
        .unwrap()
        .messages;
    broker
        .ack("jobs", vec![done[0].lease.clone().unwrap()])
        .await
        .unwrap();
    let leased = broker
        .consume("jobs", consume("default", 1))
        .await
        .unwrap()
        .messages;
    assert_eq!(leased[0].body.get(), "2");
    broker.shutdown(Duration::ZERO).await;
    drop(broker);

    let broker = Broker::open(&dir, 0).unwrap();
    let again = broker.publish("jobs", dedup).await.unwrap();
    assert!(again.duplicate, "dedup ids are remembered across restarts");
    assert_eq!(again.id, first.id);

    let deliveries = broker
        .consume("jobs", consume("default", 10))
        .await
        .unwrap()
        .messages;
    let bodies: Vec<_> = deliveries.iter().map(|d| d.body.get()).collect();
    assert_eq!(bodies, ["2", "3"], "acked messages stay done");
    assert_eq!(
        deliveries[0].attempt, 2,
        "a delivery interrupted by a restart counts as an attempt"
    );
    broker.shutdown(Duration::ZERO).await;
}

/// Leases one message of a new `jobs` queue to `default` and stops the broker before the ack.
async fn restart_while_processing(dir: &Path, settings: QueueSettingsPatch) -> String {
    let broker = open_with_queue(dir, "jobs", settings).await;
    let published = broker.publish("jobs", message(json!(1))).await.unwrap();
    broker.consume("jobs", consume("default", 1)).await.unwrap();
    broker.shutdown(Duration::ZERO).await;
    published.id
}

#[tokio::test]
async fn a_restart_uses_up_the_attempt_and_dead_letters_the_last_one() {
    let dir = temp_dir("restart-attempts");
    let settings = QueueSettingsPatch {
        max_attempts: Some(1),
        ..QueueSettingsPatch::default()
    };
    let id = restart_while_processing(&dir, settings).await;

    let broker = Broker::open(&dir, 0).unwrap();
    let again = broker
        .consume("jobs", consume("default", 10))
        .await
        .unwrap()
        .messages;
    assert!(again.is_empty(), "its only attempt was used up: {again:?}");
    let dead = broker.messages_list("jobs.dlq", None, None, None).unwrap();
    let info = dead[0].dead_letter.as_ref().unwrap();
    assert_eq!(
        (info.message_id.as_str(), info.reason),
        (id.as_str(), DeadLetterReason::MaxAttempts)
    );
    let error = dead[0].last_error.as_deref().unwrap_or_default();
    assert!(error.contains("restarted"), "{error}");
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_restart_retries_after_the_backoff() {
    let dir = temp_dir("restart-backoff");
    let settings = QueueSettingsPatch {
        retry_delay_ms: Some(60_000),
        ..QueueSettingsPatch::default()
    };
    restart_while_processing(&dir, settings).await;

    let reopened_at = berthq_core::time::now_ms();
    let broker = Broker::open(&dir, 0).unwrap();
    let again = broker
        .consume("jobs", consume("default", 10))
        .await
        .unwrap()
        .messages;
    assert!(
        again.is_empty(),
        "the retry waits for its backoff: {again:?}"
    );
    let retrying = broker
        .consumers_messages(
            "jobs",
            "default",
            Some(ConsumerMessageState::Retrying),
            None,
        )
        .unwrap();
    assert_eq!(retrying[0].attempts, 1);
    let retry_at = retrying[0].retry_at.as_deref().unwrap();
    assert!(berthq_core::time::parse(retry_at).unwrap() >= reopened_at + 60_000);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn shutdown_waits_for_leased_messages() {
    let dir = temp_dir("graceful");
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

    let shutdown = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.shutdown(Duration::from_secs(10)).await })
    };
    tokio::task::yield_now().await;
    assert!(matches!(
        broker.publish("jobs", message(json!(2))).await,
        Err(Error::Unavailable)
    ));
    assert!(matches!(
        broker.consume("jobs", consume("default", 1)).await,
        Err(Error::Unavailable)
    ));
    broker.ack("jobs", vec![lease]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown should finish once nothing is leased")
        .unwrap();
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn backups_are_consistent_copies_that_open_as_a_data_directory() {
    let dir = temp_dir("backup");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    for n in 1..=2 {
        broker.publish("jobs", message(json!(n))).await.unwrap();
    }
    let done = broker
        .consume("jobs", consume("billing", 1))
        .await
        .unwrap()
        .messages;
    broker
        .ack("jobs", vec![done[0].lease.clone().unwrap()])
        .await
        .unwrap();
    let backup = broker.clone();
    let copy = tokio::task::spawn_blocking(move || backup.backup())
        .await
        .unwrap()
        .unwrap();
    let path = copy.path().to_path_buf();
    assert!(
        path.starts_with(&dir),
        "backups are written inside the data directory"
    );
    let restored_dir = temp_dir("backup-restored");
    std::fs::create_dir_all(&restored_dir).unwrap();
    std::fs::copy(&path, restored_dir.join("berthq.redb")).unwrap();
    // As if the broker crashed before the copy was sent and deleted.
    std::mem::forget(copy);
    broker.shutdown(Duration::ZERO).await;
    drop(broker);

    let restored = Broker::open(&restored_dir, 0).unwrap();
    let rest = restored
        .consume("jobs", consume("billing", 10))
        .await
        .unwrap()
        .messages;
    assert_eq!(rest.len(), 1, "the consumer's position came along");
    assert_eq!(rest[0].body.get(), "2");
    restored.shutdown(Duration::ZERO).await;

    let reopened = Broker::open(&dir, 0).unwrap();
    assert!(
        !path.exists(),
        "leftover backup copies are deleted on start"
    );
    reopened.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_full_disk_stops_publishing_but_not_acks() {
    let dir = temp_dir("disk-full");
    let broker = open_with_queue(&dir, "emails", QueueSettingsPatch::default()).await;
    broker.publish("emails", message(json!(1))).await.unwrap();
    broker.shutdown(Duration::ZERO).await;
    drop(broker);

    // No disk has this much free space, so publishing is stopped from the start.
    let broker = Broker::open(&dir, u64::MAX).unwrap();
    let err = broker
        .publish("emails", message(json!(2)))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), "diskFull");
    assert!(broker.overview().disk.full);

    let delivered = broker
        .consume("emails", consume("default", 1))
        .await
        .unwrap()
        .messages;
    let lease = delivered[0].lease.clone().unwrap();
    assert_eq!(broker.ack("emails", vec![lease]).await.unwrap().settled, 1);
    broker.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_huge_shutdown_timeout_is_not_an_error() {
    let dir = temp_dir("huge-shutdown-timeout");
    let broker = open_with_queue(&dir, "jobs", QueueSettingsPatch::default()).await;
    broker.shutdown(Duration::MAX).await;
}
