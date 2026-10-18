//! Settling deliveries inside a job: ack (done), nack (failed) and extend (more time). A lease
//! names its consumer and is unique per delivery, so a stale one never matches.

use ulid::Ulid;

use crate::delivery::Settled;
use crate::error::Result;
use crate::failure::Failure;
use crate::ids;
use crate::records::{Pending, Slot};
use crate::tx::Tx;

impl Tx<'_, '_> {
    pub(crate) fn ack(&mut self, queue: &str, leases: &[String]) -> Result<Settled> {
        self.mem.queue(queue)?;
        let mut settled = Settled::default();
        for lease in leases {
            match self.leased(queue, lease)? {
                Some((consumer, pending)) => {
                    self.finish(queue, &consumer, &pending)?;
                    self.mem.queue_mut(queue)?.counters.acked += 1;
                    settled.settled += 1;
                }
                None => settled.unknown.push(lease.clone()),
            }
        }
        Ok(settled)
    }

    pub(crate) fn nack(
        &mut self,
        queue: &str,
        leases: &[String],
        failure: &Failure,
    ) -> Result<Settled> {
        self.mem.queue(queue)?;
        let mut settled = Settled::default();
        for lease in leases {
            match self.leased(queue, lease)? {
                Some((consumer, pending)) => {
                    self.fail(queue, &consumer, pending, failure)?;
                    settled.settled += 1;
                }
                None => settled.unknown.push(lease.clone()),
            }
        }
        Ok(settled)
    }

    pub(crate) fn extend(
        &mut self,
        queue: &str,
        leases: &[String],
        visibility_timeout_ms: Option<u64>,
    ) -> Result<Settled> {
        let settings = &self.mem.queue(queue)?.settings;
        let visibility = visibility_timeout_ms.unwrap_or(settings.visibility_timeout_ms);
        let mut settled = Settled::default();
        for lease in leases {
            let Some((consumer, pending)) = self.leased(queue, lease)? else {
                settled.unknown.push(lease.clone());
                continue;
            };
            let mut extended = pending.clone();
            if let Some(lease) = &mut extended.lease {
                lease.deadline = self.now + visibility;
            }
            self.update_pending(queue, &consumer, &pending, &extended)?;
            settled.settled += 1;
        }
        Ok(settled)
    }

    /// The consumer and pending record a lease refers to, if that delivery is still leased.
    fn leased(&self, queue: &str, lease: &str) -> Result<Option<(String, Pending)>> {
        let Some((id, attempt, token, consumer)) = parse_lease(lease) else {
            return Ok(None);
        };
        if self.mem.consumer(queue, consumer).is_err() {
            return Ok(None);
        }
        let pending = self.pending(queue, consumer, id)?.filter(|pending| {
            pending.slot == Slot::Leased
                && pending.attempts == attempt
                && pending.lease.as_ref().is_some_and(|l| l.token == token)
        });
        Ok(pending.map(|pending| (consumer.to_string(), pending)))
    }
}

/// Leases are `<message id>.<attempt>.<token>.<consumer>`: the random token makes a lease from an
/// earlier delivery (or from before a replay) never match the current one.
pub(crate) fn lease_token(id: Ulid, attempt: u32, token: u64, consumer: &str) -> String {
    format!("{id}.{attempt}.{token:x}.{consumer}")
}

fn parse_lease(lease: &str) -> Option<(Ulid, u32, u64, &str)> {
    let mut parts = lease.splitn(4, '.');
    let id = ids::parse(parts.next()?)?;
    let attempt = parts.next()?.parse().ok()?;
    let token = u64::from_str_radix(parts.next()?, 16).ok()?;
    Some((id, attempt, token, parts.next()?))
}
