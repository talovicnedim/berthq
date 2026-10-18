use super::*;

#[tokio::test]
async fn publish_from_stdin_reports_a_bad_line_and_goes_on() {
    let server = start("bad-line").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    let too_large = "x".repeat(5 * 1024 * 1024);
    let input = format!("first\n{too_large}\nthird\n");

    let mut publish = berthq(&server.url, &["publish", "jobs", "-"]);
    let mut child = publish.stdin(Stdio::piped()).spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(input.as_bytes()).await.unwrap();
    drop(stdin);
    let output = child.wait_with_output().await.unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("line 2: body is"), "{stderr}");
    assert!(stderr.contains("Published 2 messages"), "{stderr}");
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(queue.messages, 2);
    server.stop().await;
}

#[tokio::test]
async fn publish_from_stdin_stops_on_a_full_queue_and_says_where() {
    let server = start("full-queue").await;
    let client = Client::new(&server.url).unwrap();
    let settings = QueueSettingsPatch {
        max_messages: Some(2),
        ..QueueSettingsPatch::default()
    };
    client.queues_create("jobs", &settings).await.unwrap();
    // Too large to share a batch, so each line is stored or refused on its own.
    let line = "x".repeat(2_500_000);
    let input = format!("{line}\n{line}\n{line}\n");

    let output = with_open_stdin(&server.url, &["publish", "jobs", "-"], input).await;

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("Published 2 messages"), "{stderr}");
    assert!(
        stderr.contains("Stopped at line 3, the first line not published"),
        "{stderr}"
    );
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(queue.messages, 2);
    server.stop().await;
}

#[tokio::test]
async fn publish_from_stdin_refuses_a_dedup_id() {
    let server = start("stdin-dedup").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();

    let args = ["publish", "jobs", "-", "--dedup-id", "order-7"];
    let output = with_open_stdin(&server.url, &args, "a\nb\nc\n".to_string()).await;

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("--dedup-id"), "{stderr}");
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(queue.messages, 0);
    server.stop().await;
}
