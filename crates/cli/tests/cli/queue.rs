use super::*;

#[tokio::test]
async fn delete_purge_and_remove_print_json_with_json() {
    let server = start("json-removes").await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    client.publish("jobs", &NewMessage::new(1)).await.unwrap();
    client
        .consumers_create("jobs", "billing", None)
        .await
        .unwrap();

    let purge = json_output(&server.url, &["queue", "purge", "jobs"]).await;
    let remove = json_output(&server.url, &["consumer", "remove", "jobs", "billing"]).await;
    let delete = json_output(&server.url, &["queue", "delete", "jobs"]).await;

    assert_eq!(purge, json!({ "purged": 1 }));
    assert_eq!(remove, json!({ "removed": "billing" }));
    assert_eq!(delete, json!({ "deleted": "jobs" }));
    server.stop().await;
}

async fn json_output(url: &str, args: &[&str]) -> serde_json::Value {
    let output = berthq(url, &[&["--json"], args].concat())
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_str(&stdout).unwrap_or_else(|err| panic!("{err}: {stdout}"))
}
