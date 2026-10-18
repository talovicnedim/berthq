//! Without API keys, any web page the user opens could call the API through their browser.
//! Browser requests from other sites are refused, and on a loopback address so are host names
//! other than localhost, which a DNS rebinding attack would send.

use std::net::IpAddr;

use axum::extract::{Request, State};
use axum::http::uri::Authority;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;

use crate::auth::denied;

/// Only for an open API: a page can't send a key, so with keys there is nothing to check.
/// `loopback` says the server listens on a loopback address.
pub async fn require_same_origin(
    State(loopback): State<bool>,
    request: Request,
    next: Next,
) -> Response {
    let host = host(&request);
    if from_other_site(&request, host) {
        return denied(
            StatusCode::FORBIDDEN,
            "forbidden",
            "browser requests from other sites need an api key (start berthq with --api-key)",
        );
    }
    if loopback && host.is_some_and(|host| !is_loopback(host)) {
        return denied(
            StatusCode::FORBIDDEN,
            "forbidden",
            "without an api key, use localhost or 127.0.0.1; other host names need one (start \
             berthq with --api-key)",
        );
    }
    next.run(request).await
}

/// HTTP/2 sends the host as `:authority`, which ends up in the URI.
fn host(request: &Request) -> Option<&str> {
    match request.headers().get(header::HOST) {
        Some(host) => host.to_str().ok(),
        None => request.uri().authority().map(Authority::as_str),
    }
}

/// `Sec-Fetch-Site` decides when a browser sends it, because proxies like Vite's (`pnpm dev`)
/// change `Host` and so break the `Origin` check. Older browsers only send `Origin`.
fn from_other_site(request: &Request, host: Option<&str>) -> bool {
    let headers = request.headers();
    if let Some(site) = headers.get("sec-fetch-site") {
        return !matches!(site.as_bytes(), b"same-origin" | b"none");
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return false;
    };
    let origin_host = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .map(|(_, origin_host)| origin_host);
    match (origin_host, host) {
        (Some(origin_host), Some(host)) => !origin_host.eq_ignore_ascii_case(host),
        _ => true,
    }
}

fn is_loopback(host: &str) -> bool {
    let Ok(authority) = host.parse::<Authority>() else {
        return false;
    };
    let name = authority.host();
    name.eq_ignore_ascii_case("localhost")
        || name
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}
