//! The SDK against a real broker server running in this process.

// Helpers outside `#[test]` functions aren't covered by clippy's allow-unwrap-in-tests.
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::time::Duration;

use berthq_client::{
    Client, ConsumeOptions, ConsumerMessageState, NackOptions, NewMessage, ProcessOptions,
    PublisherOptions, QueueSettingsPatch,
};
use berthq_core::Broker;
use berthq_server::ApiKeys;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod admin;
mod consumers;
mod delivery;
mod publisher;

struct TestServer {
    url: String,
    broker: Broker,
    dir: PathBuf,
    shutdown: oneshot::Sender<()>,
    serving: JoinHandle<()>,
}

async fn start(name: &str, keys: &[&str]) -> TestServer {
    let dir = std::env::temp_dir().join(format!("berthq-sdk-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    serve(dir, listener, keys)
}

fn serve(dir: PathBuf, listener: TcpListener, keys: &[&str]) -> TestServer {
    let broker = Broker::open(&dir, 0).unwrap();
    let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let app = berthq_server::router(broker.clone(), ApiKeys::parse(&keys).unwrap(), true);
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        let stopped = async {
            let _ = stopped.await;
        };
        axum::serve(listener, app)
            .with_graceful_shutdown(stopped)
            .await
            .unwrap();
    });
    TestServer {
        url,
        broker,
        dir,
        shutdown,
        serving,
    }
}

impl TestServer {
    async fn stop(self) {
        self.broker.shutdown(Duration::ZERO).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// Stops the broker and opens it again on the same address and data. While it is down, the
    /// first client to reach the address has its connection dropped, so that request fails.
    async fn restart(self) -> TestServer {
        let TestServer {
            url,
            broker,
            dir,
            shutdown,
            serving,
        } = self;
        let _ = shutdown.send(());
        serving.await.unwrap();
        broker.shutdown(Duration::ZERO).await;
        drop(broker);
        let listener = TcpListener::bind(url.trim_start_matches("http://"))
            .await
            .unwrap();
        drop(listener.accept().await.unwrap());
        serve(dir, listener, &[])
    }
}

fn settings(max_attempts: u32) -> QueueSettingsPatch {
    QueueSettingsPatch {
        max_attempts: Some(max_attempts),
        retry_delay_ms: Some(0),
        ..QueueSettingsPatch::default()
    }
}
