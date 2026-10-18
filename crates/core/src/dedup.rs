//! Deduplication: a publish with a `dedupId` seen on the queue within the last hour stores
//! nothing and returns the original message id.

use redb::ReadableTable;
use ulid::Ulid;

use crate::error::Result;
use crate::tx::Tx;

/// How long a `dedupId` is remembered: long enough for retries and a restarted outbox loop.
pub(crate) const DEDUP_WINDOW_MS: u64 = 60 * 60 * 1000;

impl Tx<'_, '_> {
    /// The message published with this dedup id within the dedup window, if any.
    pub(crate) fn seen(&self, queue: &str, dedup_id: &str) -> Result<Option<Ulid>> {
        let Some(entry) = self.t.dedup.get((queue, dedup_id))? else {
            return Ok(None);
        };
        let (id, expires_at) = entry.value();
        Ok((expires_at > self.now).then_some(Ulid(id)))
    }

    pub(crate) fn remember(
        &mut self,
        queue: &str,
        dedup_id: &str,
        id: Ulid,
        expires_at: u64,
    ) -> Result<()> {
        // An expired entry the timer hasn't removed yet: drop its expiry so the timer doesn't
        // remove the new entry with it.
        let old = self
            .t
            .dedup
            .insert((queue, dedup_id), (id.0, expires_at))?
            .map(|old| old.value().1);
        if let Some(old_expiry) = old {
            self.t
                .dedup_expiring
                .remove((queue, old_expiry, dedup_id))?;
        }
        self.t
            .dedup_expiring
            .insert((queue, expires_at, dedup_id), ())?;
        let queue = self.mem.queue_mut(queue)?;
        queue.wake_at = queue.wake_at.min(expires_at);
        Ok(())
    }

    /// Forgets up to `limit` dedup ids of `queue` that expire at or before `until`. Returns how
    /// many it forgot.
    pub(crate) fn forget_dedup(&mut self, queue: &str, until: u64, limit: usize) -> Result<usize> {
        let mut expired = Vec::new();
        let range = self
            .t
            .dedup_expiring
            .range((queue, 0, "")..=(queue, until, "\u{10FFFF}"))?;
        for entry in range.take(limit) {
            let (key, _) = entry?;
            let (_, expires_at, dedup_id) = key.value();
            expired.push((expires_at, dedup_id.to_string()));
        }
        let forgotten = expired.len();
        for (expires_at, dedup_id) in expired {
            let dedup_id = dedup_id.as_str();
            self.t
                .dedup_expiring
                .remove((queue, expires_at, dedup_id))?;
            self.t.dedup.remove((queue, dedup_id))?;
        }
        Ok(forgotten)
    }
}
