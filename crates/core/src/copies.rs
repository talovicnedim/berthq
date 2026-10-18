//! Requeued dead letters: a private copy of a message for the one consumer that failed it, so
//! other consumers don't get it again. It keeps the original id. A consumer only holds a copy of
//! a message it won't get again from the log, or it would receive the message twice.

use redb::ReadableTable;
use ulid::Ulid;

use crate::error::Result;
use crate::records::Message;
use crate::tx::Tx;

impl Tx<'_, '_> {
    pub fn insert_copy(
        &mut self,
        queue: &str,
        consumer: &str,
        message: &Message,
        body: &[u8],
    ) -> Result<()> {
        let key = (queue, consumer, message.id.0);
        let record = serde_json::to_vec(message)?;
        self.t.copies.insert(key, record.as_slice())?;
        self.t.copy_bodies.insert(key, body)?;
        let counts = &mut self.mem.queue_mut(queue)?.counts;
        counts.copies += 1;
        counts.bytes += u64::from(message.size);
        self.queue_changed(queue);
        Ok(())
    }

    pub fn copy(&self, queue: &str, consumer: &str, id: Ulid) -> Result<Option<Message>> {
        let Some(record) = self.t.copies.get((queue, consumer, id.0))? else {
            return Ok(None);
        };
        let mut message: Message = serde_json::from_slice(record.value())?;
        message.id = id;
        Ok(Some(message))
    }

    pub fn copy_body(&self, queue: &str, consumer: &str, id: Ulid) -> Result<Option<Vec<u8>>> {
        let body = self.t.copy_bodies.get((queue, consumer, id.0))?;
        Ok(body.map(|body| body.value().to_vec()))
    }

    pub fn remove_copy(&mut self, queue: &str, consumer: &str, id: Ulid) -> Result<()> {
        let Some(message) = self.copy(queue, consumer, id)? else {
            return Ok(());
        };
        self.t.copies.remove((queue, consumer, id.0))?;
        self.t.copy_bodies.remove((queue, consumer, id.0))?;
        let counts = &mut self.mem.queue_mut(queue)?.counts;
        counts.copies = counts.copies.saturating_sub(1);
        counts.bytes = counts.bytes.saturating_sub(u64::from(message.size));
        self.queue_changed(queue);
        Ok(())
    }
}
