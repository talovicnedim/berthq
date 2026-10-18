use std::path::PathBuf;
use std::time::Duration;

use clap::{Arg, ArgAction, Args, Parser, Subcommand};

use crate::consume::ConsumeArgs;
use crate::consumer::ConsumerCommand;
use crate::duration;
use crate::inspect::MessagesArgs;
use crate::publish::PublishArgs;
use crate::queue::QueueCommand;
use crate::size;

#[derive(Parser)]
#[command(
    name = "berthq",
    version,
    about = "A message broker in one binary: persistent queues, consumers with replay, retries, dead letters and a dashboard",
    after_help = "Get started:\n  berthq start\n  berthq queue create emails\n  berthq publish emails '{\"user\":123}'\n  berthq consume emails"
)]
pub struct Cli {
    /// Broker to talk to (client commands)
    #[arg(
        long,
        global = true,
        env = "BERTHQ_URL",
        default_value = "http://127.0.0.1:7331"
    )]
    pub url: String,

    /// API key for client commands
    #[arg(
        long = "key",
        global = true,
        env = "BERTHQ_API_KEY",
        hide_env_values = true
    )]
    pub api_key: Option<String>,

    /// Print JSON instead of tables
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start the broker
    Start(StartArgs),
    /// Create, inspect and manage queues
    #[command(subcommand)]
    Queue(QueueCommand),
    /// Publish a message; reads one message per line from stdin when the body is `-` or missing
    /// (a line the broker refuses is reported with its number and skipped)
    Publish(PublishArgs),
    /// Receive messages for a consumer: print and ack them, or run a command for each one
    Consume(ConsumeArgs),
    /// Manage consumers: list them, replay, pause, remove
    #[command(subcommand)]
    Consumer(ConsumerCommand),
    /// Look at messages without consuming them
    Messages(MessagesArgs),
    /// Save a consistent copy of the broker's database
    Backup {
        /// File to write; it must not exist yet
        file: PathBuf,
    },
    /// Show throughput and queue sizes
    Stats,
}

#[derive(Args)]
// Hidden stand-ins keep the global client flags out of `start --help`: clap doesn't copy a global
// flag into a subcommand that already has one with its id.
#[command(
    arg = Arg::new("url").long("url").hide(true),
    arg = Arg::new("api_key").long("key").hide(true),
    arg = Arg::new("json").long("json").action(ArgAction::SetTrue).hide(true)
)]
pub struct StartArgs {
    /// Address to listen on; use 0.0.0.0 to accept connections from other machines (then set
    /// --api-key, or --no-auth on a private network)
    #[arg(long, env = "BERTHQ_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to listen on
    #[arg(long, short, env = "BERTHQ_PORT", default_value_t = 7331)]
    pub port: u16,

    /// Directory for the data file
    #[arg(long, env = "BERTHQ_DATA", default_value = "./data")]
    pub data: PathBuf,

    /// Stop accepting publishes while the disk has less than this free, e.g. 1GB (0 = off);
    /// acks and deletes keep working, so the backlog can drain
    #[arg(long, env = "BERTHQ_MIN_FREE_DISK", default_value = "1GB", value_parser = size::parse)]
    pub min_free_disk: u64,

    /// How long shutdown waits for in-flight messages to be acked
    #[arg(long, env = "BERTHQ_SHUTDOWN_TIMEOUT", default_value = "10s", value_parser = duration::parse)]
    pub shutdown_timeout: Duration,

    /// Require this API key (repeatable). Prefix scopes to limit it: publish:KEY,
    /// publish+consume:KEY, read:KEY; plain keys are admin keys. BERTHQ_API_KEYS takes a
    /// comma-separated list
    #[arg(
        long = "api-key",
        env = "BERTHQ_API_KEYS",
        value_delimiter = ',',
        hide_env_values = true
    )]
    pub api_keys: Vec<String>,

    /// Allow an open API (no API keys) on a non-local address, for private networks
    #[arg(long, env = "BERTHQ_NO_AUTH")]
    pub no_auth: bool,

    /// TLS certificate chain (PEM); serves HTTPS together with --tls-key
    #[arg(long, env = "BERTHQ_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// TLS private key (PEM)
    #[arg(long, env = "BERTHQ_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
}
