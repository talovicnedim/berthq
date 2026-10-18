//! Time is paused and jumps to the next timer whenever every task waits, so the 30 second
//! timeouts take no real time. On macOS loopback data arrives a moment after the write, so the
//! tests wait in small steps: one long sleep would let time jump past data still on its way.

use axum::routing::{get, post};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

use super::*;

const HEAD: &str = "GET / HTTP/1.1\r\nhost: localhost\r\n";
const STEP: Duration = Duration::from_millis(100);

#[test]
fn an_open_api_needs_loopback_or_no_auth() {
    let local: SocketAddr = "127.0.0.1:7331".parse().unwrap();
    let network: SocketAddr = "0.0.0.0:7331".parse().unwrap();
    assert!(check_open(local, true, false).is_ok());
    assert!(check_open(network, false, false).is_ok(), "keys are set");
    assert!(check_open(network, true, true).is_ok());
    let refused = check_open(network, true, false).unwrap_err();
    assert!(refused.contains("--no-auth"), "{refused}");
}

#[tokio::test(start_paused = true)]
async fn a_request_head_sent_one_line_a_second_is_cut_off() {
    let (address, handle) = start();
    let started = Instant::now();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(HEAD.as_bytes()).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    tokio::spawn(async move {
        loop {
            wait(Duration::from_secs(1)).await;
            if writer.write_all(b"x-slow: 1\r\n").await.is_err() {
                return;
            }
        }
    });
    let mut response = Vec::new();
    let closed = tokio::time::timeout(4 * CLIENT_TIMEOUT, reader.read_to_end(&mut response)).await;
    assert!(closed.is_ok(), "still open after {:?}", started.elapsed());
    assert!(
        started.elapsed() >= CLIENT_TIMEOUT,
        "{:?}",
        started.elapsed()
    );
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_connection_that_sends_nothing_is_closed() {
    let (address, handle) = start();
    let started = Instant::now();
    let stream = TcpStream::connect(address).await.unwrap();
    assert_closed_after_timeout(stream, started).await;
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_client_that_stops_inside_the_http2_preface_is_closed() {
    let (address, handle) = start();
    let started = Instant::now();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"PRI * HTTP").await.unwrap();
    assert_closed_after_timeout(stream, started).await;
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn an_http2_client_that_answers_no_ping_is_closed() {
    let (address, handle) = start();
    let started = Instant::now();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(H2_PREFACE).await.unwrap();
    assert_closed_after_timeout(stream, started).await;
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn an_idle_keep_alive_connection_is_closed() {
    let (address, handle) = start();
    let started = Instant::now();
    let mut stream = TcpStream::connect(address).await.unwrap();
    let answer = exchange(&mut stream, &format!("{HEAD}\r\n")).await;
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert_closed_after_timeout(stream, started).await;
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_slow_upload_is_not_cut_off() {
    let (address, handle) = start();
    let mut stream = TcpStream::connect(address).await.unwrap();
    let upload = "POST /echo HTTP/1.1\r\nhost: localhost\r\ncontent-length: 4\r\n\r\nab";
    stream.write_all(upload.as_bytes()).await.unwrap();
    wait(2 * CLIENT_TIMEOUT).await;
    let answer = exchange(&mut stream, "cd").await;
    assert!(answer.ends_with("abcd"), "{answer}");
    handle.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_slow_answer_is_not_cut_off() {
    let (address, handle) = start();
    let mut stream = TcpStream::connect(address).await.unwrap();
    let answer = exchange(&mut stream, "GET /slow HTTP/1.1\r\nhost: localhost\r\n\r\n").await;
    assert!(answer.ends_with("done"), "{answer}");
    handle.shutdown();
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn start() -> (SocketAddr, ServerHandle) {
    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .route("/echo", post(|body: String| async move { body }))
        .route(
            "/slow",
            get(|| async {
                wait(2 * CLIENT_TIMEOUT).await;
                "done"
            }),
        );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = ServerHandle::new();
    serve(listener, app, None, handle.clone()).unwrap();
    (address, handle)
}

async fn wait(duration: Duration) {
    let started = Instant::now();
    while started.elapsed() < duration {
        tokio::time::sleep(STEP).await;
    }
}

/// Sends `request` and returns the first answer.
async fn exchange(stream: &mut TcpStream, request: &str) -> String {
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut answer = vec![0; 1024];
    loop {
        if let Ok(read) = tokio::time::timeout(STEP, stream.read(&mut answer)).await {
            return String::from_utf8_lossy(&answer[..read.unwrap()]).into_owned();
        }
    }
}

/// The server closes the connection at least the timeout after `started`, and within four
/// times the timeout.
async fn assert_closed_after_timeout(mut stream: TcpStream, started: Instant) {
    let mut buf = [0; 1024];
    while started.elapsed() < 4 * CLIENT_TIMEOUT {
        if let Ok(Ok(0) | Err(_)) = tokio::time::timeout(STEP, stream.read(&mut buf)).await {
            let elapsed = started.elapsed();
            assert!(elapsed >= CLIENT_TIMEOUT, "closed after {elapsed:?}");
            return;
        }
    }
    panic!("still open after {:?}", started.elapsed());
}
