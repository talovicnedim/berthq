use serde_json::json;

use crate::ids;
use crate::positions::Start;
use crate::settings::QueueSettings;
use crate::testing::{TestDb, ack, bodies, publish_bodies, take};

fn queue(retention_ms: u64) -> TestDb {
    TestDb::with_queue(QueueSettings {
        retention_ms,
        ..QueueSettings::default()
    })
}

fn create(db: &mut TestDb, consumer: &str, now: u64) {
    db.run(now, |tx| {
        tx.create_consumer("jobs", consumer, Start::Oldest)
    })
    .unwrap();
}

fn finish_all(db: &mut TestDb, consumer: &str, now: u64) {
    let deliveries = take(db, consumer, 100, now);
    ack(db, &deliveries, now);
}

#[test]
fn messages_stay_until_every_consumer_has_acked_them() {
    let mut db = queue(1000);
    create(&mut db, "billing", 0);
    create(&mut db, "audit", 0);
    publish_bodies(&mut db, [1, 2], 0);
    finish_all(&mut db, "billing", 0);
    db.tick(2000);
    assert_eq!(db.counts("jobs").log, 2, "audit still needs them");

    finish_all(&mut db, "audit", 2000);
    db.tick(2000);
    assert_eq!(db.counts("jobs").log, 0);
    assert_eq!(db.counts("jobs").bytes, 0);
}

#[test]
fn a_queue_without_consumers_keeps_its_messages() {
    let mut db = queue(0);
    publish_bodies(&mut db, [1, 2], 0);
    db.tick(1_000_000);
    assert_eq!(db.counts("jobs").log, 2);
}

#[test]
fn retention_zero_deletes_right_after_the_ack() {
    let mut db = queue(0);
    publish_bodies(&mut db, [1, 2], 0);
    let first = take(&mut db, "default", 1, 0);
    ack(&mut db, &first, 0);
    db.tick(0);
    assert_eq!(db.counts("jobs").log, 1, "only the acked message goes");
}

#[test]
fn deleting_the_message_that_holds_retention_back_lets_it_go_on() {
    let mut db = queue(0);
    publish_bodies(&mut db, [1, 2], 0);
    let first = take(&mut db, "default", 1, 0);
    let second = take(&mut db, "default", 1, 0);
    ack(&mut db, &second, 0);
    db.tick(10);
    assert_eq!(
        db.counts("jobs").log,
        2,
        "the leased first message holds both"
    );

    let id = ids::parse(&first[0].id).unwrap();
    db.run(10, |tx| {
        let message = tx.message(id)?.unwrap();
        tx.remove_message(&message, true)
    })
    .unwrap();
    db.tick(20);
    assert_eq!(
        db.counts("jobs").log,
        0,
        "the acked second message goes too"
    );
}

#[test]
fn acked_messages_stay_for_replay_until_retention_ends() {
    let mut db = queue(1000);
    publish_bodies(&mut db, [1, 2], 0);
    finish_all(&mut db, "default", 0);
    db.tick(500);
    let late = take(&mut db, "late", 10, 500);
    assert_eq!(
        bodies(&late),
        [json!(1), json!(2)],
        "a new consumer can still read them"
    );
    ack(&mut db, &late, 500);
    db.tick(999);
    assert_eq!(db.counts("jobs").log, 2);
    db.tick(1000);
    assert_eq!(db.counts("jobs").log, 0);
}

#[test]
fn backlog_counts_what_some_consumer_still_has_to_process() {
    let mut db = queue(60_000);
    publish_bodies(&mut db, [1, 2, 3], 0);
    assert_eq!(db.backlog(0), 3, "without consumers every message counts");

    finish_all(&mut db, "billing", 0);
    assert_eq!(db.backlog(0), 0, "kept only for replay");
    create(&mut db, "audit", 0);
    assert_eq!(db.backlog(0), 3);
    take(&mut db, "audit", 1, 0);
    assert_eq!(db.backlog(0), 3, "being processed still counts");
}
