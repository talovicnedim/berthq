use redb::ReadableTable;
use serde_json::json;
use ulid::Ulid;

use super::Requeued;
use crate::error::Error;
use crate::positions::Start;
use crate::settings::QueueSettings;
use crate::testing::{
    TestDb, ack, bodies, body, failure, leases, message_ids, nack, publish, replay, request, take,
};

fn one_attempt() -> TestDb {
    TestDb::with_queue(QueueSettings {
        max_attempts: 1,
        ..QueueSettings::default()
    })
}

/// Publishes one message and lets `billing` fail it into the dead-letter queue.
fn dead_letter_for_billing(db: &mut TestDb) -> String {
    let published = publish(db, body(1), 0).unwrap();
    let delivery = take(db, "billing", 1, 0).remove(0);
    nack(db, &delivery, &failure("boom"), 0);
    published.id
}

fn dead_letter_ids(db: &mut TestDb) -> Vec<Option<Ulid>> {
    db.run(0, |tx| {
        let entries =
            tx.t.log
                .range(("jobs.dlq", 0, 0)..=("jobs.dlq", u8::MAX, u64::MAX))?;
        let mut found = Vec::new();
        for entry in entries {
            found.push(Some(Ulid(entry?.1.value().0)));
        }
        Ok(found)
    })
    .unwrap()
}

fn requeue_all(db: &mut TestDb, now: u64) -> Requeued {
    let ids = dead_letter_ids(db);
    db.run(now, |tx| tx.requeue("jobs.dlq", ids)).unwrap()
}

#[test]
fn dead_letters_name_the_consumer_and_leave_others_alone() {
    let mut db = one_attempt();
    db.run(0, |tx| tx.create_consumer("jobs", "audit", Start::Oldest))
        .unwrap();
    let original = dead_letter_for_billing(&mut db);
    let dead_id = dead_letter_ids(&mut db).remove(0).unwrap();
    let dead = db.run(0, |tx| tx.message(dead_id)).unwrap().unwrap();
    let info = dead.dead_letter.unwrap();
    assert_eq!(
        (info.queue.as_str(), info.consumer.as_str()),
        ("jobs", "billing")
    );
    assert_eq!(info.message_id, original);
    assert_eq!(info.last_error.as_deref(), Some("boom"));
    assert_ne!(
        dead_id.to_string(),
        original,
        "a dead letter has its own id"
    );
    assert_eq!(bodies(&take(&mut db, "audit", 10, 0)), [json!(1)]);
}

#[test]
fn requeued_dead_letters_go_back_to_their_consumer_under_the_original_id() {
    let mut db = one_attempt();
    let original = dead_letter_for_billing(&mut db);
    let audit = take(&mut db, "audit", 10, 0);
    ack(&mut db, &audit, 0);

    let ids = dead_letter_ids(&mut db);
    let requeued = db.run(10, |tx| tx.requeue("jobs.dlq", ids)).unwrap();
    assert_eq!((requeued.requeued, requeued.skipped), (1, 0));
    assert_eq!(db.counts("jobs.dlq").log, 0);

    let again = take(&mut db, "billing", 10, 10);
    assert_eq!(again.len(), 1);
    assert_eq!(
        again[0].id, original,
        "workers recognise it by its original id"
    );
    assert_eq!(again[0].attempt, 1);
    assert_eq!(again[0].last_error.as_deref(), Some("boom"));
    assert!(
        take(&mut db, "audit", 10, 10).is_empty(),
        "other consumers don't get it again"
    );

    ack(&mut db, &again, 10);
    assert_eq!(db.counts("jobs").copies, 0);
}

#[test]
fn requeue_skips_what_it_cannot_give_back() {
    let mut db = one_attempt();
    dead_letter_for_billing(&mut db);
    db.run(0, |tx| tx.remove_consumer("jobs", "billing"))
        .unwrap();
    let mut ids = dead_letter_ids(&mut db);
    ids.push(None);
    let requeued = db.run(0, |tx| tx.requeue("jobs.dlq", ids)).unwrap();
    assert_eq!((requeued.requeued, requeued.skipped), (0, 2));
    assert_eq!(db.counts("jobs.dlq").log, 1);
}

#[test]
fn only_dead_letter_queues_can_be_requeued() {
    let mut db = one_attempt();
    let result = db.run(0, |tx| tx.check_dead_letter_queue("jobs"));
    assert!(matches!(result, Err(Error::Invalid(_))));
}

#[test]
fn a_dead_letter_requeued_after_a_replay_is_delivered_once() {
    let mut db = one_attempt();
    let original = dead_letter_for_billing(&mut db);
    let bytes = db.counts("jobs").bytes;
    replay(&mut db, "billing", Start::Oldest, 0);
    let requeued = requeue_all(&mut db, 0);
    assert_eq!((requeued.requeued, requeued.skipped), (1, 0));
    assert_eq!(db.counts("jobs.dlq").log, 0);

    let again = take(&mut db, "billing", 10, 0);
    assert_eq!(message_ids(&again), [original], "the replay brings it back");
    ack(&mut db, &again, 0);
    assert_eq!(db.consumer("billing").processing, 0);
    assert_eq!(
        (db.counts("jobs").copies, db.counts("jobs").bytes),
        (0, bytes)
    );
}

#[test]
fn a_replay_drops_requeued_dead_letters_the_queue_delivers_again() {
    let mut db = one_attempt();
    let original = dead_letter_for_billing(&mut db);
    requeue_all(&mut db, 0);
    replay(&mut db, "billing", Start::Oldest, 0);

    let again = take(&mut db, "billing", 10, 0);
    assert_eq!(message_ids(&again), [original]);
    ack(&mut db, &again, 0);
    assert_eq!(db.counts("jobs").copies, 0);
}

#[test]
fn a_replay_keeps_requeued_dead_letters_whose_message_is_gone() {
    let mut db = one_attempt();
    db.settings().retention_ms = 1000;
    let original = dead_letter_for_billing(&mut db);
    db.tick(1000);
    assert_eq!(db.counts("jobs").log, 0, "retention deleted the original");
    requeue_all(&mut db, 1000);
    replay(&mut db, "billing", Start::Oldest, 1000);

    let again = take(&mut db, "billing", 10, 1000);
    assert_eq!(message_ids(&again), [original]);
    assert_eq!(again[0].last_error.as_deref(), Some("boom"));
}

#[test]
fn a_replay_to_the_end_keeps_requeued_dead_letters_as_if_just_requeued() {
    let mut db = one_attempt();
    let original = dead_letter_for_billing(&mut db);
    requeue_all(&mut db, 0);
    let before = take(&mut db, "billing", 1, 0);
    replay(&mut db, "billing", Start::Newest, 0);
    assert_eq!(
        ack(&mut db, &before, 0).settled,
        0,
        "leases from before are unknown"
    );

    let again = take(&mut db, "billing", 10, 0);
    assert_eq!(message_ids(&again), [original]);
    assert_eq!(again[0].attempt, 1);
    ack(&mut db, &again, 0);
    assert_eq!(db.counts("jobs").copies, 0);
}

#[test]
fn a_replay_in_chunks_moves_past_the_dead_letters_it_keeps() {
    let mut db = one_attempt();
    let originals = [
        dead_letter_for_billing(&mut db),
        dead_letter_for_billing(&mut db),
    ];
    requeue_all(&mut db, 0);
    let end = db
        .run(0, |tx| tx.positions_for("jobs", Start::Newest))
        .unwrap();
    let mut after = None;
    for _ in 0..=originals.len() {
        after = db
            .run(0, |tx| tx.replay_pending("jobs", "billing", &end, after, 1))
            .unwrap();
        if after.is_none() {
            break;
        }
    }
    assert_eq!(after, None, "the chunks get past the copies they keep");

    db.run(0, |tx| tx.move_consumer("jobs", "billing", end))
        .unwrap();
    assert_eq!(message_ids(&take(&mut db, "billing", 10, 0)), originals);
}

#[test]
fn requeueing_the_dead_letter_that_holds_retention_back_lets_it_go_on() {
    let mut db = one_attempt();
    db.run(0, |tx| tx.ensure_dead_letter_queue("jobs")).unwrap();
    db.mem.queue_mut("jobs.dlq").unwrap().settings.retention_ms = 0;
    dead_letter_for_billing(&mut db);
    dead_letter_for_billing(&mut db);
    let take_dead = |db: &mut TestDb| {
        db.run(0, |tx| tx.take("jobs.dlq", &request("inspector", 1)))
            .unwrap()
    };
    let first = take_dead(&mut db);
    let second = take_dead(&mut db);
    db.run(0, |tx| tx.ack("jobs.dlq", &leases(&second)))
        .unwrap();
    db.tick(10);
    assert_eq!(
        db.counts("jobs.dlq").log,
        2,
        "the leased first one holds both"
    );

    let first_id = crate::ids::parse(&first[0].id);
    db.run(10, |tx| tx.requeue("jobs.dlq", vec![first_id]))
        .unwrap();
    db.tick(20);
    assert_eq!(
        db.counts("jobs.dlq").log,
        0,
        "the acked second one goes too"
    );
}
