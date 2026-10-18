//! `berthq`: runs the broker (`berthq start`) and talks to a running one (`berthq publish`,
//! `berthq consume`, `berthq queue …`, `berthq consumer …`).

mod args;
mod consume;
mod consumer;
mod duration;
mod exec;
mod inspect;
mod output;
mod publish;
mod queue;
mod settings;
mod size;

use std::process::ExitCode;

use berthq_client::Client;
use clap::Parser;

use crate::args::{Cli, Command};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Start(args) => {
            log_to_stderr("info");
            start(args).await
        }
        command => {
            // The client logs only what it recovers from, like `consume` riding out a restart.
            log_to_stderr("warn");
            client_command(&cli.url, cli.api_key.as_deref(), cli.json, command).await
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            output::note(&format!("error: {err}"));
            ExitCode::FAILURE
        }
    }
}

/// Logs at `level` and up, or as `BERTHQ_LOG` says.
fn log_to_stderr(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("BERTHQ_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn start(args: args::StartArgs) -> Result<(), String> {
    let api_keys = berthq_server::ApiKeys::parse(&args.api_keys)?;
    berthq_server::run(berthq_server::ServerOptions {
        host: args.host,
        port: args.port,
        data_dir: args.data,
        min_free_disk: args.min_free_disk,
        shutdown_timeout: args.shutdown_timeout,
        api_keys,
        no_auth: args.no_auth,
        tls: args.tls_cert.zip(args.tls_key),
    })
    .await
}

async fn client_command(
    url: &str,
    api_key: Option<&str>,
    json: bool,
    command: Command,
) -> Result<(), String> {
    let mut client = Client::new(url).map_err(|err| err.to_string())?;
    if let Some(key) = api_key {
        client = client.with_api_key(key);
    }
    let result = match command {
        Command::Start(_) => Ok(()),
        Command::Queue(command) => queue::run(&client, json, command).await,
        Command::Publish(args) => publish::run(&client, json, args).await,
        Command::Consume(args) => consume::run(&client, json, args).await,
        Command::Consumer(command) => consumer::run(&client, json, command).await,
        Command::Messages(args) => inspect::messages(&client, json, args).await,
        Command::Backup { file } => backup(&client, &file).await,
        Command::Stats => inspect::stats(&client, json).await,
    };
    result.map_err(|err| match err {
        berthq_client::Error::Unreachable(message) => {
            format!("{message}\nIs it running? Start it with `berthq start`, or point --url / BERTHQ_URL at it.")
        }
        err => err.to_string(),
    })
}

async fn backup(client: &Client, file: &std::path::Path) -> berthq_client::Result<()> {
    let written = client.backup(file).await?;
    let size = output::number(written / 1024);
    output::line(&format!("Saved a backup to {} ({size} KB)", file.display()));
    output::note("To restore: stop Berthq and put the file in the data directory as berthq.redb");
    Ok(())
}
