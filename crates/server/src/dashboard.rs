//! Serves the dashboard (a static SvelteKit build) from inside the binary. In debug builds the
//! files are read from `apps/dashboard/build` at runtime, so a rebuilt dashboard shows up
//! without recompiling.

use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::RustEmbed;

use crate::error::ApiError;

#[derive(RustEmbed)]
#[folder = "../../apps/dashboard/build"]
#[allow_missing = true]
struct Assets;

const NOT_BUILT: &str = "<!doctype html><title>Berthq</title>\
<p>The dashboard is not built. Run <code>pnpm build</code> and restart the broker. \
The API is at <a href=\"/v1/stats\">/v1/stats</a>.</p>";

pub async fn serve(uri: Uri) -> Response {
    if is_api_typo(uri.path()) {
        return ApiError::unknown_route().into_response();
    }
    let mut response = page(uri.path().trim_start_matches('/'));
    let headers = response.headers_mut();
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// Paths no dashboard page uses, only a mistyped API or monitoring URL: they get the API's 404,
/// so a typo in a probe or scrape config doesn't look healthy.
fn is_api_typo(path: &str) -> bool {
    if path.starts_with("//") {
        return true;
    }
    let first = path.trim_start_matches('/').split('/').next().unwrap_or("");
    let version = first
        .strip_prefix('v')
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
    version || first == "metrics" || first == "healthz"
}

fn page(path: &str) -> Response {
    if let Some(response) = file(path) {
        return response;
    }
    // Client-side routes like /queues/emails all load the app shell.
    if !path.starts_with("_app/")
        && let Some(response) = file("index.html")
    {
        return response;
    }
    if path.is_empty() || path == "index.html" {
        return Html(NOT_BUILT).into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}

fn file(path: &str) -> Option<Response> {
    let path = if path.is_empty() { "index.html" } else { path };
    let asset = Assets::get(path)?;
    // Hashed build output never changes; everything else must be revalidated.
    let cache = if path.starts_with("_app/immutable/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let headers = [
        (header::CONTENT_TYPE, asset.metadata.mimetype().to_string()),
        (header::CACHE_CONTROL, cache.to_string()),
    ];
    Some((headers, asset.data).into_response())
}
