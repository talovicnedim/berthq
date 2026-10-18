use serde_json::json;

use crate::failure::Failure;
use crate::positions::Start;
use crate::publish::NewMessage;
use crate::settings::QueueSettings;
use crate::testing::{
    TestDb, ack, bodies, body, failure, id, leases, nack, publish, publish_bodies, replay, request,
    take, take_with,
};

#[test]
fn delivers_by_priority_then_age() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 1000).unwrap();
    let urgent = NewMessage {
        priority: Some(5),
        ..body(2)
    };
    publish(&mut db, urgent, 1000).unwrap();
    publish(&mut db, body(3), 1000).unwrap();
    let deliveries = take(&mut db, "default", 10, 1000);
    assert_eq!(bodies(&deliveries), [json!(2), json!(1), json!(3)]);
    assert_eq!(db.consumer("default").processing, 3);
}

#[test]
fn ack_finishes_and_unknown_leases_are_reported() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 1000).unwrap();
    let deliveries = take(&mut db, "default", 1, 1000);
    assert_eq!(ack(&mut db, &deliveries, 1000).settled, 1);
    assert_eq!(db.consumer("default").processing, 0);
    assert!(take(&mut db, "default", 1, 1000).is_empty());

    let settled = ack(&mut db, &deliveries, 1000);
    assert_eq!(settled.unknown.len(), 1);
}

#[test]
fn every_consumer_gets_every_message_and_workers_share_them() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish_bodies(&mut db, [1, 2], 0);
    let first_worker = take(&mut db, "billing", 1, 0);
    let second_worker = take(&mut db, "billing", 1, 0);
    assert_eq!(bodies(&first_worker), [json!(1)]);
    assert_eq!(bodies(&second_worker), [json!(2)]);
    assert_eq!(bodies(&take(&mut db, "audit", 10, 0)), [json!(1), json!(2)]);
}

#[test]
fn nack_retries_with_exponential_backoff_then_dead_letters() {
    let settings = QueueSettings {
        max_attempts: 3,
        retry_delay_ms: 1000,
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();

    let mut now = 0;
    for delay in [1000, 2000] {
        let delivery = take(&mut db, "default", 1, now).remove(0);
        nack(&mut db, &delivery, &failure("boom"), now);
        db.tick(now + delay - 1);
        assert!(take(&mut db, "default", 1, now + delay - 1).is_empty());
        db.tick(now + delay);
        now += delay;
    }
    let delivery = take(&mut db, "default", 1, now).remove(0);
    assert_eq!(delivery.attempt, 3);
    assert_eq!(delivery.last_error.as_deref(), Some("boom"));
    nack(&mut db, &delivery, &failure("boom"), now);

    assert_eq!(db.consumer("default").retrying(), 0);
    assert_eq!(db.counts("jobs.dlq").log, 1);
    assert_eq!(
        db.counts("jobs").log,
        1,
        "the message stays for other consumers"
    );
}

#[test]
fn nack_can_dead_letter_right_away_or_pick_the_delay() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish_bodies(&mut db, [1, 2], 0);
    let deliveries = take(&mut db, "default", 2, 0);

    let reject = Failure {
        dead_letter: true,
        ..failure("bad")
    };
    nack(&mut db, &deliveries[0], &reject, 0);
    assert_eq!(db.counts("jobs.dlq").log, 1);

    let later = Failure {
        delay_ms: Some(50),
        ..failure("later")
    };
    nack(&mut db, &deliveries[1], &later, 0);
    assert_eq!(db.consumer("default").scheduled, 1);
    db.tick(49);
    assert!(take(&mut db, "default", 1, 49).is_empty());
    db.tick(50);
    assert_eq!(bodies(&take(&mut db, "default", 1, 50)), [json!(2)]);
}

#[test]
fn retries_come_before_newer_messages_of_the_same_priority() {
    let settings = QueueSettings {
        retry_delay_ms: 0,
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();
    let first = take(&mut db, "default", 1, 0).remove(0);
    publish(&mut db, body(2), 0).unwrap();
    nack(&mut db, &first, &failure("again"), 0);
    let next = take(&mut db, "default", 10, 0);
    assert_eq!(bodies(&next), [json!(1), json!(2)]);
    assert_eq!(next[0].attempt, 2);
}

#[test]
fn expired_leases_count_as_failed_attempts() {
    let settings = QueueSettings {
        visibility_timeout_ms: 1000,
        retry_delay_ms: 0,
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();
    let first = take(&mut db, "default", 1, 0);
    db.tick(999);
    assert!(take(&mut db, "default", 1, 999).is_empty());

    db.tick(1000);
    let again = take(&mut db, "default", 1, 1000);
    assert_eq!(again[0].id, first[0].id);
    assert_eq!(again[0].attempt, 2);
    assert_eq!(db.mem.queue("jobs").unwrap().counters.retried, 1);
    assert_eq!(
        ack(&mut db, &first, 1000).settled,
        0,
        "a stale lease must not ack the redelivery"
    );
}

#[test]
fn extend_pushes_the_lease_deadline() {
    let settings = QueueSettings {
        visibility_timeout_ms: 1000,
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();
    let delivery = take(&mut db, "default", 1, 0);
    let leases = crate::testing::leases(&delivery);
    db.run(900, |tx| tx.extend("jobs", &leases, Some(5000)))
        .unwrap();
    db.tick(2000);
    assert_eq!(db.consumer("default").processing, 1);
    db.tick(5900);
    assert_eq!(db.consumer("default").processing, 0);
}

#[test]
fn paused_queues_and_consumers_deliver_nothing() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 0).unwrap();
    db.settings().paused = true;
    assert!(take(&mut db, "default", 1, 0).is_empty());
    db.settings().paused = false;

    db.mem
        .consumer_mut("jobs", "default")
        .unwrap()
        .record
        .paused = true;
    assert!(take(&mut db, "default", 1, 0).is_empty());
    assert_eq!(take(&mut db, "other", 1, 0).len(), 1);
}

#[test]
fn failures_inside_a_dead_letter_queue_are_dropped() {
    let settings = QueueSettings {
        max_attempts: 1,
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();
    let delivery = take(&mut db, "default", 1, 0).remove(0);
    nack(&mut db, &delivery, &failure("boom"), 0);

    let dead = db
        .run(0, |tx| tx.take("jobs.dlq", &request("default", 1)))
        .unwrap();
    let rejected = Failure {
        dead_letter: true,
        ..failure("still broken")
    };
    db.run(0, |tx| tx.nack("jobs.dlq", &leases(&dead), &rejected))
        .unwrap();
    assert_eq!(db.mem.queue("jobs.dlq").unwrap().counters.dropped, 1);
    assert!(
        db.mem.queue("jobs.dlq.dlq").is_err(),
        "a dead-letter queue has no dead-letter queue of its own"
    );
}

#[test]
fn auto_ack_moves_the_position_without_leases() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish_bodies(&mut db, [1, 2], 0);
    let auto = crate::take::Take {
        auto_ack: true,
        ..request("stream", 10)
    };
    let deliveries = take_with(&mut db, &auto, 0);
    assert_eq!(bodies(&deliveries), [json!(1), json!(2)]);
    assert!(
        deliveries
            .iter()
            .all(|d| d.lease.is_none() && d.lease_expires_at.is_none())
    );
    assert_eq!(db.consumer("stream").processing, 0);
    assert!(take_with(&mut db, &auto, 0).is_empty());
}

#[test]
fn leases_from_before_a_replay_never_match_new_deliveries() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 0).unwrap();
    let before = take(&mut db, "default", 1, 0);
    replay(&mut db, "default", Start::Oldest, 0);
    let after = take(&mut db, "default", 1, 0);
    assert_eq!(
        (after[0].id.clone(), after[0].attempt),
        (before[0].id.clone(), 1)
    );
    assert_eq!(ack(&mut db, &before, 0).settled, 0);
    assert_eq!(ack(&mut db, &after, 0).settled, 1);
}

#[test]
fn a_stored_body_that_is_not_json_is_delivered_as_null() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let published = publish(&mut db, body(1), 0).unwrap();
    db.run(0, |tx| {
        tx.t.bodies.insert(id(&published).0, b"{\"cut".as_slice())?;
        Ok(())
    })
    .unwrap();
    assert_eq!(bodies(&take(&mut db, "default", 1, 0)), [json!(null)]);
}
