use super::*;

#[tokio::test]
async fn an_unreachable_broker_gets_a_hint_whatever_the_cause() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);

    for url in [refused.as_str(), "http://berthq.invalid:7331"] {
        let output = berthq(url, &["queue", "list"]).output().await.unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(stderr.contains("could not connect"), "{stderr}");
        assert!(stderr.contains("Is it running?"), "{stderr}");
    }
}

#[tokio::test]
async fn start_help_shows_only_what_start_uses() {
    let start = help(&["start", "--help"]).await;
    let client = help(&["queue", "delete", "--help"]).await;

    for flag in ["--url", "--key ", "--json"] {
        assert!(!start.contains(flag), "{flag} in:\n{start}");
        assert!(client.contains(flag), "{flag} missing in:\n{client}");
    }
    assert!(start.contains("Port to listen on"), "{start}");
    assert!(client.contains("API key"), "{client}");
    assert!(!format!("{start}{client}").contains("api key"));
}

#[tokio::test]
async fn start_refuses_an_open_network_address_before_naming_a_url() {
    let dir = std::env::temp_dir().join(format!("berthq-cli-open-{}", std::process::id()));
    let args = ["start", "--host", "0.0.0.0", "--port", "0", "--data"];
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_berthq"))
        .args(args)
        .arg(&dir)
        .env_remove("BERTHQ_API_KEYS")
        .env_remove("BERTHQ_NO_AUTH")
        .output()
        .await
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.starts_with("error: refusing to listen"), "{stderr}");
}

async fn help(args: &[&str]) -> String {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_berthq"))
        .args(args)
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(Duration::from_secs(10), output)
        .await
        .expect("--help prints help instead of starting anything")
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}
