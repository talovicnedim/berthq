//! The `berthq` binary against a real broker server running in this process.

// Helpers outside `#[test]` functions aren't covered by clippy's allow-*-in-tests settings.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use berthq_client::{Client, NewMessage, QueueSettingsPatch};
use berthq_core::Broker;
use berthq_server::ApiKeys;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, Lines};

mod consume;
mod help;
mod publish;
mod queue;

struct TestServer {
    url: String,
    broker: Broker,
    dir: PathBuf,
}

async fn start(name: &str) -> TestServer {
    let dir = std::env::temp_dir().join(format!("berthq-cli-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Broker::open(&dir, 0).unwrap();
    let app = berthq_server::router(broker.clone(), ApiKeys::parse(&[]).unwrap(), true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    TestServer { url, broker, dir }
}

impl TestServer {
    async fn stop(self) {
        self.broker.shutdown(Duration::ZERO).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `berthq --url <server> <args>` with its output piped; killed if the test gives up on it.
fn berthq(url: &str, args: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_berthq"));
    command
        .arg("--url")
        .arg(url)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// Reads lines until one contains `text` and returns it.
async fn line_with(lines: &mut Lines<BufReader<impl AsyncRead + Unpin>>, text: &str) -> String {
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.contains(text) {
                return line;
            }
        }
        panic!("the output ended without {text:?}")
    });
    found
        .await
        .unwrap_or_else(|_| panic!("no {text:?} in the output after 10 s"))
}

/// What a terminal does on Ctrl-C: SIGINT to every process in the foreground process group.
/// Spawn the child with `process_group(0)` so that group is the child's own.
#[cfg(unix)]
fn ctrl_c(child: &tokio::process::Child) {
    use rustix::process::{Pid, Signal, kill_process_group};

    let pid = Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap()).unwrap();
    kill_process_group(pid, Signal::INT).unwrap();
}

/// Runs `berthq <args>` with `input` on stdin, which stays open afterwards: the command has to
/// stop on its own, not because its input ended.
async fn with_open_stdin(url: &str, args: &[&str], input: String) -> std::process::Output {
    let mut command = berthq(url, args);
    let mut child = command.stdin(Stdio::piped()).spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    tokio::spawn(async move {
        let _ = stdin.write_all(input.as_bytes()).await;
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("berthq stops without waiting for the end of stdin")
        .unwrap()
}
