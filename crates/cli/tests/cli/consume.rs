use super::*;

#[tokio::test]
async fn consume_into_a_closed_pipe_stops_and_acks_only_what_it_printed() {
    let server = start("closed-pipe").await;
    let client = Client::new(&server.url).unwrap();
    let settings = QueueSettingsPatch {
        retry_delay_ms: Some(60_000),
        ..QueueSettingsPatch::default()
    };
    client.queues_create("jobs", &settings).await.unwrap();
    // Bigger than a pipe's buffer: writing it blocks until the reader closes the pipe.
    let large = "x".repeat(1024 * 1024);
    for body in [json!("first"), json!(large), json!("third")] {
        client
            .publish("jobs", &NewMessage::new(body))
            .await
            .unwrap();
    }

    let mut consume = berthq(&server.url, &["consume", "jobs"]).spawn().unwrap();
    let mut stdout = BufReader::new(consume.stdout.take().unwrap());
    let mut first = String::new();
    stdout.read_line(&mut first).await.unwrap();
    assert_eq!(first, "first\n");
    drop(stdout);

    let output = tokio::time::timeout(Duration::from_secs(10), consume.wait_with_output())
        .await
        .expect("consume stops once nobody reads its output")
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("error"), "{stderr}");
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(queue.counters.acked, 1, "only the printed message is done");
    let consumer = client.consumers_get("jobs", "default").await.unwrap().stats;
    assert_eq!(
        (consumer.processing, consumer.retrying),
        (0, 2),
        "the unprinted messages are given back"
    );
    server.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn the_first_ctrl_c_finishes_the_messages_already_received() {
    let server = start("ctrl-c").await;
    let client = Client::new(&server.url).unwrap();
    let settings = QueueSettingsPatch {
        retry_delay_ms: Some(60_000),
        ..QueueSettingsPatch::default()
    };
    client.queues_create("jobs", &settings).await.unwrap();
    for n in 1..=3 {
        client.publish("jobs", &NewMessage::new(n)).await.unwrap();
    }

    let exec = "echo running >&2; sleep 0.5";
    let mut consume = berthq(&server.url, &["consume", "jobs", "--exec", exec])
        .process_group(0)
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(consume.stderr.take().unwrap()).lines();
    line_with(&mut stderr, "running").await;
    ctrl_c(&consume);
    let mut rest = Vec::new();
    while let Some(line) = stderr.next_line().await.unwrap() {
        rest.push(line);
    }
    let status = consume.wait().await.unwrap();

    assert!(!rest.iter().any(|line| line.contains("failed")), "{rest:?}");
    assert!(rest[0].contains("Ctrl-C again"), "{rest:?}");
    assert!(status.success(), "{rest:?}");
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(
        queue.counters.acked, 3,
        "the running command and the rest of the batch"
    );
    server.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_second_ctrl_c_quits_at_once_and_interrupts_the_command() {
    let server = start("ctrl-c-twice").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    for n in 1..=3 {
        client.publish("jobs", &NewMessage::new(n)).await.unwrap();
    }

    // `exec` avoids a fork: a Ctrl-C that comes during one would miss the new process.
    let exec = "echo running $$ >&2; exec sleep 5";
    let mut consume = berthq(&server.url, &["consume", "jobs", "--exec", exec])
        .process_group(0)
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(consume.stderr.take().unwrap()).lines();
    let running = line_with(&mut stderr, "running").await;
    ctrl_c(&consume);
    line_with(&mut stderr, "Ctrl-C again").await;
    ctrl_c(&consume);
    let status = tokio::time::timeout(Duration::from_secs(2), consume.wait())
        .await
        .expect("berthq quits without waiting for the command")
        .unwrap();

    assert_eq!(status.code(), Some(130));
    let group: i32 = running.trim_start_matches("running ").parse().unwrap();
    let group = rustix::process::Pid::from_raw(group).unwrap();
    let gone = async {
        while rustix::process::test_kill_process_group(group).is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), gone)
        .await
        .expect("the command was interrupted too");
    server.stop().await;
}

#[tokio::test]
async fn exec_feeds_a_large_body_while_the_command_writes_a_lot_to_stderr() {
    let server = start("exec-large").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    let body = "x".repeat(1024 * 1024);
    client
        .publish("jobs", &NewMessage::new(json!(body)))
        .await
        .unwrap();

    // More stderr than a pipe holds, all of it before the command reads its input.
    let exec = "yes 0123456789abcdefghijklmnopqrstuvwxyz | head -c 1048576 >&2; wc -c";
    let mut consume = berthq(&server.url, &["consume", "jobs", "-n", "1", "--exec", exec]);
    let output = tokio::time::timeout(Duration::from_secs(20), consume.output())
        .await
        .expect("berthq and the command don't wait on each other")
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    let last = stderr.lines().last();
    assert!(output.status.success(), "{last:?}");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "1048576");
    let queue = client.queues_get("jobs").await.unwrap().info.stats;
    assert_eq!(queue.counters.acked, 1);
    server.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn the_command_stops_when_berthq_gives_up_on_it() {
    let server = start("exec-abandoned").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    client.publish("jobs", &NewMessage::new(1)).await.unwrap();

    let exec = "echo running $$ >&2; sleep 5; echo finished";
    let args = [
        "consume",
        "jobs",
        "--visibility-timeout",
        "200ms",
        "--exec",
        exec,
    ];
    let mut consume = berthq(&server.url, &args).spawn().unwrap();
    let mut stderr = BufReader::new(consume.stderr.take().unwrap()).lines();
    let running = line_with(&mut stderr, "running").await;
    // Extending the lease of a deleted queue fails, which ends the loop with an error.
    client.queues_remove("jobs").await.unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), consume.wait())
        .await
        .expect("berthq stops once the queue is gone")
        .unwrap();

    assert_eq!(status.code(), Some(1));
    let group: i32 = running.trim_start_matches("running ").parse().unwrap();
    let group = rustix::process::Pid::from_raw(group).unwrap();
    let gone = async {
        while rustix::process::test_kill_process_group(group).is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), gone)
        .await
        .expect("the command and its children were stopped");
    server.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_second_ctrl_c_gives_the_command_its_ctrl_c_not_a_kill() {
    let server = start("ctrl-c-trap").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    client.publish("jobs", &NewMessage::new(1)).await.unwrap();
    let file = server.dir.with_extension("interrupted");
    let _ = std::fs::remove_file(&file);

    let exec = format!(
        "trap 'echo cleaned up > {}; exit 1' INT; echo running >&2; while :; do sleep 0.1; done",
        file.display()
    );
    let mut consume = berthq(&server.url, &["consume", "jobs", "--exec", &exec])
        .process_group(0)
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(consume.stderr.take().unwrap()).lines();
    line_with(&mut stderr, "running").await;
    ctrl_c(&consume);
    line_with(&mut stderr, "Ctrl-C again").await;
    ctrl_c(&consume);
    assert_eq!(consume.wait().await.unwrap().code(), Some(130));

    let cleaned_up = async {
        while std::fs::read_to_string(&file).is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), cleaned_up)
        .await
        .expect("the command ran its Ctrl-C handler");
    let _ = std::fs::remove_file(&file);
    server.stop().await;
}
