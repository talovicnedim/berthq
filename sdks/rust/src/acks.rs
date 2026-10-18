//! Acks that travel with the next consume call instead of a request of their own.

use crate::process::unless_temporary;
use crate::{Client, Result};

/// Most leases one request may ack.
const MAX_ACKS: usize = 1000;

/// Leases of finished messages, waiting for the next `consume` (its `ack` field).
#[derive(Debug, Default)]
pub(crate) struct PendingAcks {
    leases: Vec<String>,
}

impl PendingAcks {
    pub(crate) fn push(&mut self, lease: String) {
        self.leases.push(lease);
    }

    /// Up to 1000 leases for the next consume call; they leave the buffer.
    pub(crate) fn take(&mut self) -> Vec<String> {
        let count = self.leases.len().min(MAX_ACKS);
        self.leases.drain(..count).collect()
    }

    /// Puts back leases whose request failed, so they are sent again. Acking a lease twice is
    /// harmless: the second time it is just unknown.
    pub(crate) fn restore(&mut self, leases: Vec<String>) {
        self.leases.extend(leases);
    }

    /// Sends every waiting ack with `ack` requests.
    pub(crate) async fn flush(&mut self, client: &Client, queue: &str) -> Result<()> {
        while !self.leases.is_empty() {
            let leases = self.take();
            let failed = format!("could not ack {} messages on {queue}", leases.len());
            unless_temporary(client.ack(queue, &leases).await, &failed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_empties_the_buffer_in_chunks_of_1000() {
        let mut acks = PendingAcks::default();
        for n in 0..1500 {
            acks.push(n.to_string());
        }
        assert_eq!(acks.take().len(), 1000);
        assert_eq!(acks.take().len(), 500);
        assert!(acks.take().is_empty());
    }

    #[test]
    fn restored_acks_are_sent_again() {
        let mut acks = PendingAcks::default();
        acks.restore(vec!["a".to_string()]);
        assert_eq!(acks.take(), vec!["a".to_string()]);
    }
}
