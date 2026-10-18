use redb::{ReadableDatabase, ReadableTable};
use serde_json::json;

use super::MAX_CONSUMERS;
use crate::error::Error;
use crate::positions::Start;
use crate::publish::NewMessage;
use crate::settings::QueueSettings;
use crate::testing::{
    TestDb, ack, bodies, body, id, publish, publish_bodies, replay, request, take, take_with,
};

fn drain(db: &mut TestDb, consumer: &str, now: u64) -> Vec<serde_json::Value> {
    let deliveries = take(db, consumer, 100, now);
    ack(db, &deliveries, now);
    bodies(&deliveries)
}

#[test]
fn new_consumers_start_at_the_oldest_message_or_at_the_end() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish_bodies(&mut db, [1, 2], 0);
    let from_end = crate::take::Take {
        from: Start::Newest,
        ..request("emails", 10)
    };
    assert!(take_with(&mut db, &from_end, 0).is_empty());
    publish(&mut db, body(3), 0).unwrap();
    assert_eq!(bodies(&take_with(&mut db, &from_end, 0)), [json!(3)]);
    assert_eq!(drain(&mut db, "audit", 0), [json!(1), json!(2), json!(3)]);
}

#[test]
fn replay_moves_to_start_end_time_or_message() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 1000).unwrap();
    let second = publish(&mut db, body(2), 2000).unwrap();
    publish(&mut db, body(3), 3000).unwrap();
    assert_eq!(drain(&mut db, "billing", 3000).len(), 3);

    replay(&mut db, "billing", Start::Time(2000), 4000);
    assert_eq!(drain(&mut db, "billing", 4000), [json!(2), json!(3)]);
    replay(&mut db, "billing", Start::Message(id(&second)), 4000);
    assert_eq!(drain(&mut db, "billing", 4000), [json!(2), json!(3)]);
    replay(&mut db, "billing", Start::Oldest, 4000);
    assert_eq!(
        drain(&mut db, "billing", 4000),
        [json!(1), json!(2), json!(3)]
    );
    replay(&mut db, "billing", Start::Newest, 4000);
    assert!(drain(&mut db, "billing", 4000).is_empty());
}

#[test]
fn replay_by_time_covers_every_priority() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 1000).unwrap();
    let urgent = |n| NewMessage {
        priority: Some(9),
        ..body(n)
    };
    publish(&mut db, urgent(2), 1000).unwrap();
    publish(&mut db, body(3), 2000).unwrap();
    publish(&mut db, urgent(4), 2000).unwrap();
    drain(&mut db, "billing", 2000);
    replay(&mut db, "billing", Start::Time(2000), 3000);
    assert_eq!(drain(&mut db, "billing", 3000), [json!(4), json!(3)]);
}

#[test]
fn replay_drops_what_the_consumer_was_processing() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish_bodies(&mut db, [1, 2], 0);
    let leased = take(&mut db, "billing", 2, 0);
    assert_eq!(db.consumer("billing").processing, 2);
    replay(&mut db, "billing", Start::Newest, 0);
    assert_eq!(db.consumer("billing").processing, 0);
    assert_eq!(ack(&mut db, &leased, 0).settled, 0);
}

#[test]
fn removing_a_consumer_drops_its_record_and_pending_messages() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    publish(&mut db, body(1), 0).unwrap();
    take(&mut db, "billing", 1, 0);
    db.run(0, |tx| tx.remove_consumer("jobs", "billing"))
        .unwrap();
    assert!(db.mem.consumer("jobs", "billing").is_err());
    let txn = db.db.begin_read().unwrap();
    let records = txn.open_table(crate::tables::CONSUMERS).unwrap();
    assert!(records.get(("jobs", "billing")).unwrap().is_none());
    let pending = txn.open_table(crate::tables::PENDING).unwrap();
    assert_eq!(pending.iter().unwrap().count(), 0);
}

#[test]
fn consumer_names_and_counts_are_checked() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let bad = db.run(0, |tx| {
        tx.create_consumer("jobs", "no spaces", Start::Oldest)
    });
    assert!(matches!(bad, Err(Error::Invalid(_))));
    for n in 0..MAX_CONSUMERS {
        let name = format!("c{n}");
        db.run(0, |tx| tx.create_consumer("jobs", &name, Start::Oldest))
            .unwrap();
    }
    let one_more = db.run(0, |tx| tx.create_consumer("jobs", "extra", Start::Oldest));
    assert!(matches!(one_more, Err(Error::Invalid(_))));
}

#[test]
fn a_consumer_without_workers_that_holds_messages_turns_idle_after_an_hour() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    db.run(0, |tx| tx.create_consumer("jobs", "billing", Start::Oldest))
        .unwrap();
    let idle = |db: &TestDb, now| db.mem.consumer_info("jobs", "billing", now).unwrap().idle;
    assert!(!idle(&db, 3_600_000), "it holds nothing yet");
    publish(&mut db, body(1), 0).unwrap();
    assert!(!idle(&db, 3_599_999));
    assert!(
        idle(&db, 3_600_000),
        "counted from its creation when never seen"
    );
    let deliveries = take(&mut db, "billing", 1, 3_600_000);
    assert!(!idle(&db, 3_600_001), "a worker was just seen");
    ack(&mut db, &deliveries, 3_600_001);
    assert!(!idle(&db, 99_000_000), "nothing left to hold");
}

#[test]
fn positions_skip_over_deleted_messages() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let published = publish(&mut db, body(1), 0).unwrap();
    db.run(0, |tx| tx.create_consumer("jobs", "billing", Start::Oldest))
        .unwrap();
    db.run(0, |tx| {
        let message = tx.message(id(&published))?.unwrap();
        tx.remove_message(&message, true)
    })
    .unwrap();
    assert!(take(&mut db, "billing", 10, 0).is_empty());
    assert!(!db.mem.queue("jobs").unwrap().has_work_for("billing"));
}
