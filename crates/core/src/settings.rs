//! Queue names and per-queue settings, with their defaults and validation.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Suffix of the dead-letter queue that is created next to a queue on demand.
pub const DEAD_LETTER_SUFFIX: &str = ".dlq";

const MAX_NAME_LEN: usize = 128;
const MAX_TIMEOUT_MS: u64 = 12 * 60 * 60 * 1000;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_RETENTION_MS: u64 = 365 * DAY_MS;
/// Retries back off up to this long, or up to `retryDelayMs` when that is longer.
const MAX_RETRY_DELAY_MS: u64 = 5 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSettings {
    /// How long messages stay after they become visible, so consumers can replay them.
    #[serde(default = "default_retention")]
    pub retention_ms: u64,
    /// How long a delivered message stays leased before it counts as failed and is retried.
    pub visibility_timeout_ms: u64,
    /// Deliveries before a message is dead-lettered; 0 retries forever.
    pub max_attempts: u32,
    /// Delay before the first retry, doubled for every further attempt.
    pub retry_delay_ms: u64,
    /// Most messages consumers still have to process; publishing to a full queue fails.
    pub max_messages: Option<u32>,
    pub paused: bool,
}

fn default_retention() -> u64 {
    DAY_MS
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            retention_ms: DAY_MS,
            visibility_timeout_ms: 30_000,
            max_attempts: 5,
            retry_delay_ms: 1_000,
            max_messages: None,
            paused: false,
        }
    }
}

/// Settings to change; missing fields keep their current value. `maxMessages: 0` removes the
/// limit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueSettingsPatch {
    pub retention_ms: Option<u64>,
    pub visibility_timeout_ms: Option<u64>,
    pub max_attempts: Option<u32>,
    pub retry_delay_ms: Option<u64>,
    pub max_messages: Option<u32>,
}

impl QueueSettings {
    pub fn apply(&self, patch: &QueueSettingsPatch) -> Result<QueueSettings> {
        let settings = QueueSettings {
            retention_ms: patch.retention_ms.unwrap_or(self.retention_ms),
            visibility_timeout_ms: patch
                .visibility_timeout_ms
                .unwrap_or(self.visibility_timeout_ms),
            max_attempts: patch.max_attempts.unwrap_or(self.max_attempts),
            retry_delay_ms: patch.retry_delay_ms.unwrap_or(self.retry_delay_ms),
            max_messages: match patch.max_messages {
                Some(0) => None,
                Some(max) => Some(max),
                None => self.max_messages,
            },
            paused: self.paused,
        };
        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> Result<()> {
        if self.retention_ms > MAX_RETENTION_MS {
            return Err(invalid("retentionMs must be at most 365 days"));
        }
        check_visibility_timeout(self.visibility_timeout_ms)?;
        if self.retry_delay_ms > MAX_TIMEOUT_MS {
            return Err(invalid("retryDelayMs must be at most 12 hours"));
        }
        Ok(())
    }

    /// Backoff before the next delivery of a message that has failed `attempts` times.
    pub fn retry_delay(&self, attempts: u32) -> u64 {
        let doublings = attempts.saturating_sub(1).min(32);
        self.retry_delay_ms
            .saturating_mul(1 << doublings)
            .min(MAX_RETRY_DELAY_MS.max(self.retry_delay_ms))
    }
}

/// Checks a queue or consumer name; `kind` is used in the error ("queue", "consumer").
pub fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(Error::Invalid(format!(
            "{kind} names must be 1 to 128 characters"
        )));
    }
    let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
    // Starting with a letter or digit rules out `.` and `..`, which URLs would resolve away.
    let starts_well = name.starts_with(|c: char| c.is_ascii_alphanumeric());
    if !starts_well || !name.chars().all(allowed) {
        return Err(Error::Invalid(format!(
            "invalid {kind} name {name}: start with a letter or digit, then use letters, digits, \
             '.', '_' and '-'"
        )));
    }
    Ok(())
}

/// Checks the name of a queue to create. Names ending in `.dlq` are left to the dead-letter
/// queues Berthq creates itself.
pub fn validate_queue_name(name: &str) -> Result<()> {
    validate_name("queue", name)?;
    if is_dead_letter_queue(name) {
        return Err(Error::Invalid(format!(
            "invalid queue name {name}: Berthq creates <queue>{DEAD_LETTER_SUFFIX} for dead \
             letters itself, pick a name that doesn't end in {DEAD_LETTER_SUFFIX}"
        )));
    }
    Ok(())
}

pub fn is_dead_letter_queue(name: &str) -> bool {
    name.ends_with(DEAD_LETTER_SUFFIX)
}

pub fn dead_letter_queue(name: &str) -> String {
    format!("{name}{DEAD_LETTER_SUFFIX}")
}

/// The lease length rule for queue settings, consume and extend.
pub(crate) fn check_visibility_timeout(ms: u64) -> Result<()> {
    if !(100..=MAX_TIMEOUT_MS).contains(&ms) {
        return Err(invalid(
            "visibilityTimeoutMs must be between 100 ms and 12 hours",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_doubles_up_to_five_minutes() {
        let settings = QueueSettings {
            retry_delay_ms: 60_000,
            ..QueueSettings::default()
        };
        let delays: Vec<u64> = (1..=5).map(|n| settings.retry_delay(n)).collect();
        assert_eq!(delays, [60_000, 120_000, 240_000, 300_000, 300_000]);
        assert_eq!(settings.retry_delay(u32::MAX), 300_000);
    }

    #[test]
    fn a_retry_delay_above_five_minutes_stays_as_it_is() {
        let settings = QueueSettings {
            retry_delay_ms: 600_000,
            ..QueueSettings::default()
        };
        assert_eq!(settings.retry_delay(1), 600_000);
        assert_eq!(settings.retry_delay(4), 600_000);
    }

    #[test]
    fn patch_keeps_unset_fields_and_zero_clears_max_messages() {
        let current = QueueSettings {
            max_messages: Some(10),
            ..QueueSettings::default()
        };
        let patch = QueueSettingsPatch {
            max_attempts: Some(3),
            ..QueueSettingsPatch::default()
        };
        let updated = current.apply(&patch).unwrap();
        assert_eq!(updated.max_attempts, 3);
        assert_eq!(updated.max_messages, Some(10));

        let clear = QueueSettingsPatch {
            max_messages: Some(0),
            ..QueueSettingsPatch::default()
        };
        assert_eq!(updated.apply(&clear).unwrap().max_messages, None);
    }

    #[test]
    fn patch_rejects_invalid_settings() {
        let patch = QueueSettingsPatch {
            visibility_timeout_ms: Some(5),
            ..QueueSettingsPatch::default()
        };
        assert!(QueueSettings::default().apply(&patch).is_err());
    }

    #[test]
    fn names_allow_letters_digits_dots_dashes_underscores() {
        assert!(validate_name("queue", "emails").is_ok());
        assert!(validate_name("queue", "orders.v2_high-priority").is_ok());
        for bad in [
            "",
            "..",
            ".hidden",
            "-flag",
            "with space",
            "slash/name",
            "ümlaut",
            &"x".repeat(129),
        ] {
            assert!(validate_name("queue", bad).is_err(), "{bad}");
        }
    }
}
