use super::*;

#[tokio::test]
async fn a_full_disk_refuses_publishes_with_507() {
    let app = app_with_disk("disk-full", &[], u64::MAX).await;
    app.queue("a").await;
    let publish = Call::new(Method::POST, "/v1/queues/a/messages").json(json!({ "body": 1 }));
    let (status, body) = app.call(publish).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::INSUFFICIENT_STORAGE, json!("diskFull"))
    );
    let (_, stats) = app.call(Call::new(Method::GET, "/v1/stats")).await;
    assert_eq!(stats["disk"]["full"], true);
    let (_, metrics) = app.send(Call::new(Method::GET, "/metrics")).await;
    let metrics = String::from_utf8(metrics).unwrap();
    assert!(metrics.contains("\nberthq_disk_full 1\n"), "{metrics}");
    app.stop().await;
}

#[tokio::test]
async fn backups_download_as_a_working_data_file() {
    let app = app("backup", &[]).await;
    app.queue("jobs").await;
    let publish =
        Call::new(Method::POST, "/v1/queues/jobs/messages").json(json!({ "body": "kept" }));
    app.call(publish).await;
    let (status, bytes) = app.send(Call::new(Method::GET, "/v1/backup")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        backup_files(&app.dir),
        0,
        "the temporary copy is deleted after the download"
    );

    let restored = app.dir.with_extension("restored");
    let _ = std::fs::remove_dir_all(&restored);
    std::fs::create_dir_all(&restored).unwrap();
    std::fs::write(restored.join("berthq.redb"), bytes).unwrap();
    let broker = Broker::open(&restored, 0).unwrap();
    let delivery = broker
        .consume("jobs", berthq_core::ConsumeOptions::default())
        .await
        .unwrap()
        .messages;
    assert_eq!(delivery[0].body.get(), r#""kept""#);
    broker.shutdown(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&restored);
    app.stop().await;
}

#[tokio::test]
async fn one_backup_runs_at_a_time() {
    let app = app("backup-one", &[]).await;
    app.queue("jobs").await;
    app.fill("jobs").await;
    let started = (0..8).map(|_| app.call(Call::new(Method::GET, "/v1/backup")));
    let answers = futures_util::future::join_all(started).await;
    let conflicts = answers
        .iter()
        .filter(|(status, _)| *status == StatusCode::CONFLICT)
        .count();
    assert!(conflicts > 0, "backups started together don't all run");
    for (status, body) in &answers {
        if *status != StatusCode::OK {
            assert_eq!(
                (*status, &body["error"]["kind"]),
                (StatusCode::CONFLICT, &json!("conflict"))
            );
        }
    }
    assert_eq!(backup_files(&app.dir), 0);
    let (status, _) = app.send(Call::new(Method::GET, "/v1/backup")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the next backup runs once they are done"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_dropped_backup_request_leaves_no_file() {
    let app = app("backup-dropped", &[]).await;
    app.queue("jobs").await;
    app.fill("jobs").await;
    let request = Request::get("/v1/backup").body(Body::empty()).unwrap();
    let download = tokio::spawn(app.router.clone().oneshot(request));
    assert!(
        eventually(|| backup_files(&app.dir) == 1).await,
        "the copy started"
    );
    download.abort();
    let _ = download.await;
    assert!(
        eventually(|| backup_files(&app.dir) == 0).await,
        "the copy is deleted once written"
    );
    let (status, _) = app.send(Call::new(Method::GET, "/v1/backup")).await;
    assert_eq!(status, StatusCode::OK, "the next backup can run");
    app.stop().await;
}

#[tokio::test]
async fn a_backup_needs_room_for_its_copy() {
    let app = app_with_disk("backup-disk-full", &[], u64::MAX).await;
    let (status, body) = app.call(Call::new(Method::GET, "/v1/backup")).await;
    assert_eq!(
        (status, body["error"]["kind"].clone()),
        (StatusCode::INSUFFICIENT_STORAGE, json!("diskFull"))
    );
    assert_eq!(backup_files(&app.dir), 0);
    app.stop().await;
}
