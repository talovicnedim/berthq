//! Storing and deleting messages: visible messages go to the log of their priority lane, delayed
//! ones wait aside until they are due. Every change goes through these functions, which keep the
//! tables and the queue's counts in step.

use redb::ReadableTable;
use ulid::Ulid;

use crate::error::Result;
use crate::records::{Message, lane};
use crate::tables::LogEntry;
use crate::tx::Tx;

impl Tx<'_, '_> {
    pub fn message(&self, id: Ulid) -> Result<Option<Message>> {
        let Some(record) = self.t.messages.get(id.0)? else {
            return Ok(None);
        };
        let mut message: Message = serde_json::from_slice(record.value())?;
        message.id = id;
        Ok(Some(message))
    }

    pub fn body(&self, id: Ulid) -> Result<Option<Vec<u8>>> {
        Ok(self.t.bodies.get(id.0)?.map(|body| body.value().to_vec()))
    }

    /// Stores a new message: in the log when it is visible now, otherwise with the delayed ones.
    pub fn store_message(&mut self, mut message: Message, body: &[u8]) -> Result<()> {
        let (queue, id) = (message.queue.clone(), message.id.0);
        self.t.bodies.insert(id, body)?;
        match message.deliver_at {
            Some(at) if at > self.now => {
                self.t.delayed.insert((queue.as_str(), at, id), ())?;
                self.mem.queue_mut(&queue)?.counts.delayed += 1;
                self.wake_queue_at(&queue, at)?;
            }
            _ => {
                message.deliver_at = None;
                self.append(&mut message)?;
            }
        }
        self.put_message(&message)?;
        let counts = &mut self.mem.queue_mut(&queue)?.counts;
        counts.bytes += u64::from(message.size);
        self.queue_changed(&queue);
        Ok(())
    }

    /// Makes a delayed message visible: it joins the end of its lane.
    pub fn release(&mut self, queue: &str, deliver_at: u64, id: Ulid) -> Result<()> {
        self.t.delayed.remove((queue, deliver_at, id.0))?;
        let Some(mut message) = self.message(id)? else {
            return Ok(());
        };
        let counts = &mut self.mem.queue_mut(queue)?.counts;
        counts.delayed = counts.delayed.saturating_sub(1);
        message.deliver_at = None;
        self.append(&mut message)?;
        self.put_message(&message)
    }

    /// Deletes a message for every consumer. With `pending_too`, consumers that are processing
    /// or retrying it drop it too (their leases become unknown).
    pub fn remove_message(&mut self, message: &Message, pending_too: bool) -> Result<()> {
        let (queue, id) = (message.queue.as_str(), message.id.0);
        self.t.messages.remove(id)?;
        self.t.bodies.remove(id)?;
        let counts = &mut self.mem.queue_mut(queue)?.counts;
        counts.bytes = counts.bytes.saturating_sub(u64::from(message.size));
        if let Some(seq) = message.seq {
            self.t.log.remove((queue, lane(message.priority), seq))?;
            let counts = &mut self.mem.queue_mut(queue)?.counts;
            counts.log = counts.log.saturating_sub(1);
        } else if let Some(at) = message.deliver_at {
            self.t.delayed.remove((queue, at, id))?;
            let counts = &mut self.mem.queue_mut(queue)?.counts;
            counts.delayed = counts.delayed.saturating_sub(1);
        }
        self.queue_changed(queue);
        if pending_too {
            self.drop_pending_everywhere(queue, message.id)?;
        }
        Ok(())
    }

    /// Deletes a log entry found by retention or purge, without reading the message first.
    /// Consumers' pending records are left to the caller.
    pub fn remove_entry(&mut self, queue: &str, lane: u8, seq: u64, entry: LogEntry) -> Result<()> {
        let (id, _, size) = entry;
        self.t.log.remove((queue, lane, seq))?;
        self.t.messages.remove(id)?;
        self.t.bodies.remove(id)?;
        let counts = &mut self.mem.queue_mut(queue)?.counts;
        counts.log = counts.log.saturating_sub(1);
        counts.bytes = counts.bytes.saturating_sub(u64::from(size));
        self.queue_changed(queue);
        Ok(())
    }

    fn append(&mut self, message: &mut Message) -> Result<()> {
        let now = self.now;
        let queue = self.mem.queue_mut(&message.queue)?;
        let lane = lane(message.priority);
        let seq = queue.counts.heads[usize::from(lane)] + 1;
        queue.counts.heads[usize::from(lane)] = seq;
        queue.counts.log += 1;
        queue.retention_due = queue
            .retention_due
            .min(now.saturating_add(queue.settings.retention_ms));
        queue.wake_at = queue.wake_at.min(queue.retention_due);
        message.seq = Some(seq);
        message.visible_at = Some(now);
        let entry = (message.id.0, now, message.size);
        self.t
            .log
            .insert((message.queue.as_str(), lane, seq), entry)?;
        self.wake_workers(&message.queue);
        Ok(())
    }

    fn put_message(&mut self, message: &Message) -> Result<()> {
        let record = serde_json::to_vec(message)?;
        self.t.messages.insert(message.id.0, record.as_slice())?;
        Ok(())
    }

    /// Consumers processing or retrying a log message drop it; their private copies stay.
    fn drop_pending_everywhere(&mut self, queue: &str, id: Ulid) -> Result<()> {
        let consumers: Vec<String> = self.mem.queue(queue)?.consumers.keys().cloned().collect();
        for consumer in consumers {
            if let Some(pending) = self.pending(queue, &consumer, id)?
                && !pending.is_copy()
            {
                self.delete_pending(queue, &consumer, &pending)?;
                self.floors_moved(queue);
            }
        }
        Ok(())
    }

    pub fn wake_queue_at(&mut self, queue: &str, at: u64) -> Result<()> {
        let queue = self.mem.queue_mut(queue)?;
        queue.wake_at = queue.wake_at.min(at);
        Ok(())
    }
}
