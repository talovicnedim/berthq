//! API keys. Without keys the API is open (only allowed on a loopback address unless the user
//! says the network is private); with keys every API request needs one, sent as
//! `Authorization: Bearer <key>`, or as `?apiKey=<key>` on `GET /v1/events` only (`EventSource`
//! can't set headers).
//!
//! Keys have scopes: `admin` (everything), `publish`, `consume` (consume, ack, nack, extend) and
//! `read` (every GET and HEAD but backups). One key can hold several: `publish+consume:KEY`.

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

const MIN_KEY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Admin,
    Publish,
    Consume,
    Read,
}

#[derive(Debug, Clone, Default)]
pub struct ApiKeys {
    keys: Vec<(Vec<Scope>, String)>,
}

impl ApiKeys {
    /// Parses `scopes:key` entries (`publish:KEY`, `publish+consume:KEY`); a key without scopes
    /// is an admin key.
    pub fn parse(entries: &[String]) -> Result<ApiKeys, String> {
        let mut keys = Vec::new();
        for entry in entries {
            let (scopes, key) = match entry.split_once(':') {
                Some((scopes, key)) => (parse_scopes(scopes)?, key),
                None if parse_scope(entry).is_ok() => {
                    return Err(format!(
                        "`{entry}` is a scope without a key: write it as {entry}:KEY, and join \
                         several scopes with + (publish+consume:KEY)"
                    ));
                }
                None => (vec![Scope::Admin], entry.as_str()),
            };
            if key.len() < MIN_KEY_LEN {
                return Err(format!(
                    "api keys must be at least {MIN_KEY_LEN} characters"
                ));
            }
            keys.push((scopes, key.to_string()));
        }
        Ok(ApiKeys { keys })
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn scopes_of(&self, presented: &str) -> Option<&[Scope]> {
        self.keys
            .iter()
            .find(|(_, key)| constant_time_eq(key.as_bytes(), presented.as_bytes()))
            .map(|(scopes, _)| scopes.as_slice())
    }
}

fn parse_scopes(scopes: &str) -> Result<Vec<Scope>, String> {
    if scopes.contains(',') {
        return Err(
            "an api key has a comma in its scopes: join several scopes with + (publish+consume:KEY)"
                .to_string(),
        );
    }
    scopes.split('+').map(parse_scope).collect()
}

fn parse_scope(scope: &str) -> Result<Scope, String> {
    match scope {
        "admin" => Ok(Scope::Admin),
        "publish" => Ok(Scope::Publish),
        "consume" => Ok(Scope::Consume),
        "read" => Ok(Scope::Read),
        // Never echo the text: it may be the start of a key that contains a `:`.
        _ => Err(
            "an api key has an unknown scope before its first `:`: use admin, publish, \
             consume or read (a key without a scope can't contain `:`)"
                .to_string(),
        ),
    }
}

pub async fn require_key(State(keys): State<ApiKeys>, request: Request, next: Next) -> Response {
    if keys.is_empty() {
        return next.run(request).await;
    }
    let Some(presented) = presented_key(&request) else {
        return denied(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "an api key is required",
        );
    };
    let Some(scopes) = keys.scopes_of(&presented) else {
        return denied(StatusCode::UNAUTHORIZED, "unauthorized", "unknown api key");
    };
    let needed = required_scope(request.method(), request.uri().path());
    if !scopes.contains(&Scope::Admin) && !scopes.contains(&needed) {
        return denied(
            StatusCode::FORBIDDEN,
            "forbidden",
            "this api key may not do that",
        );
    }
    next.run(request).await
}

/// The scope a request needs, besides `admin`.
fn required_scope(method: &Method, path: &str) -> Scope {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if method == Method::GET || method == Method::HEAD {
        return match segments.as_slice() {
            ["v1", "backup"] => Scope::Admin,
            _ => Scope::Read,
        };
    }
    if method != Method::POST {
        return Scope::Admin;
    }
    match segments.as_slice() {
        ["v1", "queues", _, "messages" | "batch"] => Scope::Publish,
        ["v1", "queues", _, "consume" | "ack" | "nack" | "extend"] => Scope::Consume,
        _ => Scope::Admin,
    }
}

fn presented_key(request: &Request) -> Option<String> {
    let header = request.headers().get("authorization");
    if let Some((_, key)) = header
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
    {
        return Some(key.trim().to_string());
    }
    if request.method() != Method::GET || request.uri().path() != "/v1/events" {
        return None;
    }
    request
        .uri()
        .query()?
        .split('&')
        .find_map(|pair| pair.strip_prefix("apiKey="))
        .map(percent_decode)
}

/// Decodes `%XX` escapes, so keys with reserved characters work when clients encode them. A `+`
/// stays a plus: keys often contain one (base64), and `encodeURIComponent` sends a space as `%20`.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(crate) fn denied(status: StatusCode, kind: &str, message: &str) -> Response {
    let body = json!({ "error": { "kind": kind, "message": message } });
    (status, axum::Json(body)).into_response()
}

/// Compares without returning early, so timing doesn't reveal how much of a key matched.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
