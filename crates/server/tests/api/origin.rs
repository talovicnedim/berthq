use super::*;

fn publish<'a>() -> Call<'a> {
    Call::new(Method::POST, "/v1/queues/a/messages").json(json!({ "body": 1 }))
}

#[tokio::test]
async fn browsers_on_other_sites_are_refused_without_keys() {
    let app = app("cross-site", &[]).await;
    app.queue("a").await;
    let refused = [
        (
            "a page on another site",
            publish()
                .header("host", "127.0.0.1:7331")
                .header("content-type", "text/plain")
                .header("sec-fetch-site", "cross-site"),
        ),
        (
            "a foreign origin",
            publish()
                .header("host", "127.0.0.1:7331")
                .header("origin", "https://evil.example"),
        ),
        (
            "an opaque origin",
            publish()
                .header("host", "127.0.0.1:7331")
                .header("origin", "null"),
        ),
        (
            "dns rebinding",
            publish()
                .header("host", "rebind.evil.example:7331")
                .header("origin", "http://rebind.evil.example:7331"),
        ),
        (
            "dns rebinding reading a backup",
            Call::new(Method::GET, "/v1/backup").header("host", "rebind.evil.example:7331"),
        ),
    ];
    for (case, call) in refused {
        let (status, body) = app.call(call).await;
        assert_eq!(
            (status, body["error"]["kind"].clone()),
            (StatusCode::FORBIDDEN, json!("forbidden")),
            "{case}"
        );
    }
    let (_, consumed) = app
        .call(Call::new(Method::POST, "/v1/queues/a/consume"))
        .await;
    assert_eq!(consumed["messages"], json!([]), "nothing was published");
    app.stop().await;
}

#[tokio::test]
async fn the_dashboard_curl_and_typed_urls_work_without_keys() {
    let app = app("same-origin", &[]).await;
    app.queue("a").await;
    let allowed = [
        ("curl", publish().header("host", "127.0.0.1:7331")),
        (
            "the dashboard",
            publish()
                .header("host", "localhost:7331")
                .header("origin", "http://localhost:7331")
                .header("sec-fetch-site", "same-origin"),
        ),
        (
            "pnpm dev: Vite's proxy sends its own host",
            publish()
                .header("host", "127.0.0.1:7331")
                .header("origin", "http://localhost:5176")
                .header("sec-fetch-site", "same-origin"),
        ),
        ("ipv6 loopback", publish().header("host", "[::1]:7331")),
        (
            "another loopback address",
            publish().header("host", "127.0.0.2"),
        ),
        (
            "a url typed in the browser",
            Call::new(Method::GET, "/v1/queues")
                .header("host", "LOCALHOST:7331")
                .header("sec-fetch-site", "none"),
        ),
    ];
    for (case, call) in allowed {
        let (status, body) = app.call(call).await;
        assert!(status.is_success(), "{case}: {status} {body}");
    }
    app.stop().await;
}

#[tokio::test]
async fn with_api_keys_the_key_decides_not_the_browser_headers() {
    let app = app("origin-keys", &["adminadminadmin1"]).await;
    let create = Call::new(Method::POST, "/v1/queues")
        .json(json!({ "name": "a" }))
        .header("host", "berthq.example.com")
        .header("origin", "https://app.example.com")
        .header("sec-fetch-site", "cross-site")
        .key("adminadminadmin1");
    assert_eq!(app.call(create).await.0, StatusCode::CREATED);
    app.stop().await;
}

#[tokio::test]
async fn an_open_api_on_a_network_address_accepts_any_host_name() {
    let app = app_on_network("origin-network").await;
    app.queue("a").await;
    let service = publish().header("host", "berthq.internal:7331");
    assert_eq!(app.call(service).await.0, StatusCode::CREATED);
    let other_site = publish()
        .header("host", "berthq.internal:7331")
        .header("origin", "https://evil.example");
    assert_eq!(
        app.call(other_site).await.0,
        StatusCode::FORBIDDEN,
        "other sites are still refused"
    );
    app.stop().await;
}
