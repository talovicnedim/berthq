//! Prometheus metrics in the text exposition format.

use std::fmt::Write;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use berthq_core::{Broker, ConsumerInfo, DiskStats, QueueInfo};

pub async fn metrics(State(broker): State<Broker>) -> impl IntoResponse {
    let queues = broker.queues_list();
    let consumers: Vec<ConsumerInfo> = queues
        .iter()
        .filter_map(|queue| broker.consumers_list(&queue.name).ok())
        .flatten()
        .collect();
    let disk = broker.overview().disk;
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        render(&queues, &consumers, disk),
    )
}

/// Metric name, help text, and how to read the value.
type Metric<T> = (&'static str, &'static str, fn(&T) -> u64);

const QUEUE_GAUGES: [Metric<QueueInfo>; 8] = [
    (
        "berthq_queue_messages",
        "Messages stored, including ones kept for replay",
        |q| q.stats.messages,
    ),
    (
        "berthq_queue_backlog",
        "Messages some consumer still has to process",
        |q| q.stats.backlog,
    ),
    (
        "berthq_queue_delayed",
        "Messages waiting for their deliverAt",
        |q| q.stats.delayed,
    ),
    (
        "berthq_queue_processing",
        "Messages leased to workers",
        |q| q.stats.processing,
    ),
    (
        "berthq_queue_bytes",
        "Total body size of the stored messages",
        |q| q.stats.bytes,
    ),
    ("berthq_queue_consumers", "Consumers of the queue", |q| {
        q.stats.consumers
    }),
    (
        "berthq_queue_workers",
        "Workers seen in the last minute",
        |q| q.stats.workers,
    ),
    ("berthq_queue_paused", "1 when the queue is paused", |q| {
        q.settings.paused.into()
    }),
];

const QUEUE_COUNTERS: [Metric<QueueInfo>; 6] = [
    (
        "berthq_messages_published_total",
        "Messages stored by publishes",
        |q| q.stats.counters.published,
    ),
    (
        "berthq_messages_delivered_total",
        "Deliveries to workers",
        |q| q.stats.counters.delivered,
    ),
    ("berthq_messages_acked_total", "Messages acked", |q| {
        q.stats.counters.acked
    }),
    (
        "berthq_messages_retried_total",
        "Failed deliveries scheduled for another attempt",
        |q| q.stats.counters.retried,
    ),
    (
        "berthq_messages_dead_lettered_total",
        "Messages copied to a dead-letter queue",
        |q| q.stats.counters.dead_lettered,
    ),
    (
        "berthq_messages_dropped_total",
        "Messages that ran out of attempts in a dead-letter queue",
        |q| q.stats.counters.dropped,
    ),
];

const CONSUMER_GAUGES: [Metric<ConsumerInfo>; 3] = [
    (
        "berthq_consumer_waiting",
        "Messages after the consumer's position",
        |c| c.stats.waiting,
    ),
    (
        "berthq_consumer_processing",
        "The consumer's messages leased right now",
        |c| c.stats.processing,
    ),
    (
        "berthq_consumer_retrying",
        "The consumer's messages waiting for another attempt",
        |c| c.stats.retrying,
    ),
];

const DISK_GAUGES: [Metric<DiskStats>; 2] = [
    (
        "berthq_disk_free_bytes",
        "Bytes free on the disk that holds the data directory",
        |d| d.free,
    ),
    (
        "berthq_disk_full",
        "1 while publishing is stopped because the disk is almost full",
        |d| d.full.into(),
    ),
];

fn render(queues: &[QueueInfo], consumers: &[ConsumerInfo], disk: DiskStats) -> String {
    let mut out = String::new();
    for (name, help, value) in DISK_GAUGES {
        family(
            &mut out,
            (name, help, "gauge"),
            &[disk],
            |_| String::new(),
            value,
        );
    }
    let queue_labels = |q: &QueueInfo| format!("queue=\"{}\"", q.name);
    for (name, help, value) in QUEUE_GAUGES {
        family(&mut out, (name, help, "gauge"), queues, queue_labels, value);
    }
    for (name, help, value) in QUEUE_COUNTERS {
        family(
            &mut out,
            (name, help, "counter"),
            queues,
            queue_labels,
            value,
        );
    }
    let consumer_labels =
        |c: &ConsumerInfo| format!("queue=\"{}\",consumer=\"{}\"", c.queue, c.name);
    for (name, help, value) in CONSUMER_GAUGES {
        family(
            &mut out,
            (name, help, "gauge"),
            consumers,
            consumer_labels,
            value,
        );
    }
    out
}

fn family<T>(
    out: &mut String,
    (name, help, kind): (&str, &str, &str),
    items: &[T],
    labels: impl Fn(&T) -> String,
    value: fn(&T) -> u64,
) {
    // Writing to a String can't fail.
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
    for item in items {
        let labels = labels(item);
        let labels = if labels.is_empty() {
            labels
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(out, "{name}{labels} {}", value(item));
    }
}
