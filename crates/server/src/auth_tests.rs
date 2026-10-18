use super::*;

#[test]
fn scopes_follow_the_route() {
    let cases = [
        (Method::GET, "/v1/queues/emails", Scope::Read),
        (Method::GET, "/metrics", Scope::Read),
        (Method::GET, "/v1/backup", Scope::Admin),
        (Method::HEAD, "/v1/queues", Scope::Read),
        (Method::HEAD, "/v1/backup", Scope::Admin),
        (Method::POST, "/v1/queues/emails/messages", Scope::Publish),
        (Method::POST, "/v1/queues/emails/batch", Scope::Publish),
        (Method::POST, "/v1/queues/emails/consume", Scope::Consume),
        (Method::POST, "/v1/queues/emails/ack", Scope::Consume),
        (Method::POST, "/v1/queues/emails/purge", Scope::Admin),
        (Method::POST, "/v1/queues/emails/consumers", Scope::Admin),
        (
            Method::POST,
            "/v1/queues/emails/consumers/consume/replay",
            Scope::Admin,
        ),
        (Method::PATCH, "/v1/queues/emails", Scope::Admin),
        (
            Method::DELETE,
            "/v1/queues/emails/messages/01J",
            Scope::Admin,
        ),
        (Method::POST, "/v1/queues", Scope::Admin),
    ];
    for (method, path, scope) in cases {
        assert_eq!(required_scope(&method, path), scope, "{method} {path}");
    }
}

#[test]
fn query_keys_are_percent_decoded() {
    assert_eq!(percent_decode("a%2Fb%3Dc"), "a/b=c");
    assert_eq!(percent_decode("plain"), "plain");
    assert_eq!(percent_decode("bad%zz"), "bad%zz");
    assert_eq!(percent_decode("a+b%20c"), "a+b c", "+ is a literal plus");
}

#[test]
fn keys_come_only_as_bearer_tokens_or_on_events() {
    let request = |uri: &str, header: Option<(&str, &str)>| {
        let mut builder = Request::builder().method(Method::GET).uri(uri);
        if let Some((name, value)) = header {
            builder = builder.header(name, value);
        }
        builder.body(axum::body::Body::empty()).unwrap()
    };
    let bearer = request("/v1/queues", Some(("authorization", "Bearer secret")));
    assert_eq!(presented_key(&bearer).as_deref(), Some("secret"));
    for scheme in ["bearer", "BEARER"] {
        let header = format!("{scheme} secret");
        let request = request("/v1/queues", Some(("authorization", &header)));
        assert_eq!(
            presented_key(&request).as_deref(),
            Some("secret"),
            "{scheme}"
        );
    }
    let other_header = request("/v1/queues", Some(("x-api-key", "secret")));
    assert_eq!(presented_key(&other_header), None);
    let basic = request("/v1/queues", Some(("authorization", "Basic c2VjcmV0")));
    assert_eq!(presented_key(&basic), None);
    let events = request("/v1/events?apiKey=secret", None);
    assert_eq!(presented_key(&events).as_deref(), Some("secret"));
    let elsewhere = request("/v1/queues?apiKey=secret", None);
    assert_eq!(presented_key(&elsewhere), None);
    let queue_named_events = request("/v1/queues/events?apiKey=secret", None);
    assert_eq!(presented_key(&queue_named_events), None);
    let mut posted = request("/v1/events?apiKey=secret", None);
    *posted.method_mut() = Method::POST;
    assert_eq!(presented_key(&posted), None);
}

#[test]
fn parses_scoped_and_plain_keys() {
    let keys = ApiKeys::parse(&[
        "publish:pppppppppppppppp".to_string(),
        "publish+consume:wwwwwwwwwwwwwwww".to_string(),
        "aaaaaaaaaaaaaaaa".to_string(),
    ])
    .unwrap();
    assert_eq!(
        keys.scopes_of("pppppppppppppppp"),
        Some(&[Scope::Publish][..])
    );
    assert_eq!(
        keys.scopes_of("wwwwwwwwwwwwwwww"),
        Some(&[Scope::Publish, Scope::Consume][..])
    );
    assert_eq!(
        keys.scopes_of("aaaaaaaaaaaaaaaa"),
        Some(&[Scope::Admin][..])
    );
    assert_eq!(keys.scopes_of("nope"), None);
}

#[test]
fn rejects_short_keys_unknown_scopes_and_bare_scopes() {
    assert!(ApiKeys::parse(&["short".to_string()]).is_err());
    assert!(ApiKeys::parse(&["root:aaaaaaaaaaaaaaaa".to_string()]).is_err());
    let bare = ApiKeys::parse(&["publish".to_string()]).unwrap_err();
    assert!(bare.contains("publish+consume:KEY"), "{bare}");
    let commas = ApiKeys::parse(&["publish,consume:wwwwwwwwwwwwwwww".to_string()]).unwrap_err();
    assert!(commas.contains('+'), "{commas}");
}

#[test]
fn parse_errors_never_echo_the_key() {
    for entry in ["leaked:aaaaaaaaaaaaaaaa", "leaked,more:aaaaaaaaaaaaaaaa"] {
        let err = ApiKeys::parse(&[entry.to_string()]).unwrap_err();
        assert!(!err.contains("leaked"), "{err}");
    }
}
