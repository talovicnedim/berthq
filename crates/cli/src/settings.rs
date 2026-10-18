//! Queue settings as command-line flags (`berthq queue create|update`).

use std::time::Duration;

use berthq_client::QueueSettingsPatch;
use clap::Args;

use crate::duration::{self, millis};

#[derive(Args)]
pub struct SettingsArgs {
    /// How long messages stay after they become visible, for replay, e.g. 24h (0 = delete once
    /// every consumer acked them)
    #[arg(long, value_parser = duration::parse)]
    pub retention: Option<Duration>,
    /// How long a worker has to ack before the message is retried, e.g. 30s
    #[arg(long, value_parser = duration::parse)]
    pub visibility_timeout: Option<Duration>,
    /// Deliveries before a message is dead-lettered (0 = retry forever)
    #[arg(long)]
    pub max_attempts: Option<u32>,
    /// First retry delay, doubled for every further attempt up to 5 minutes, e.g. 1s
    #[arg(long, value_parser = duration::parse)]
    pub retry_delay: Option<Duration>,
    /// Most messages waiting to be processed; publishing to a full queue fails (0 = no limit)
    #[arg(long)]
    pub max_messages: Option<u32>,
}

pub fn patch(args: &SettingsArgs) -> QueueSettingsPatch {
    QueueSettingsPatch {
        retention_ms: args.retention.map(millis),
        visibility_timeout_ms: args.visibility_timeout.map(millis),
        max_attempts: args.max_attempts,
        retry_delay_ms: args.retry_delay.map(millis),
        max_messages: args.max_messages,
    }
}
