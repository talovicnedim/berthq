use super::*;
use crate::dedup::DEDUP_WINDOW_MS;
use crate::settings::QueueSettings;
use crate::testing::{TestDb, ack, body, id, publish, take};
use crate::time;

#[test]
fn duplicate_dedup_ids_return_the_original() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let with_id = || NewMessage {
        dedup_id: Some("order-1".to_string()),
        ..body(1)
    };
    let first = publish(&mut db, with_id(), 1000).unwrap();
    let second = publish(&mut db, with_id(), 2000).unwrap();
    assert!(!first.duplicate && second.duplicate);
    assert_eq!(first.id, second.id);
    assert_eq!(db.counts("jobs").log, 1);

    let in_window = publish(&mut db, with_id(), 1000 + DEDUP_WINDOW_MS - 1).unwrap();
    assert!(in_window.duplicate);
    let after_window = publish(&mut db, with_id(), 1000 + DEDUP_WINDOW_MS).unwrap();
    assert!(!after_window.duplicate);
}

#[test]
fn duplicates_within_one_batch_are_stored_once() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let with_id = || {
        let message = NewMessage {
            dedup_id: Some("same".to_string()),
            ..body(1)
        };
        check(message, 1000).unwrap()
    };
    let published = db
        .run(1000, |tx| tx.publish("jobs", vec![with_id(), with_id()]))
        .unwrap();
    assert!(!published[0].duplicate && published[1].duplicate);
    assert_eq!(published[0].id, published[1].id);
    assert_eq!(db.counts("jobs").log, 1);
}

#[test]
fn a_full_backlog_rejects_new_messages() {
    let settings = QueueSettings {
        max_messages: Some(2),
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    let first = publish(&mut db, body(1), 1000).unwrap();
    publish(&mut db, body(2), 1000).unwrap();
    assert!(matches!(
        publish(&mut db, body(3), 1000),
        Err(Error::QueueFull(_))
    ));
    assert_eq!(db.counts("jobs").log, 2);
    assert!(db.run(1000, |tx| tx.message(id(&first))).unwrap().is_some());
}

#[test]
fn messages_kept_only_for_replay_do_not_fill_the_queue() {
    let settings = QueueSettings {
        max_messages: Some(2),
        ..QueueSettings::default()
    };
    let mut db = TestDb::with_queue(settings);
    publish(&mut db, body(1), 0).unwrap();
    publish(&mut db, body(2), 0).unwrap();
    let deliveries = take(&mut db, "default", 2, 0);
    ack(&mut db, &deliveries, 0);
    publish(&mut db, body(3), 0).unwrap();
    publish(&mut db, body(4), 0).unwrap();
    assert_eq!(db.counts("jobs").log, 4);
}

#[test]
fn delayed_messages_wait_aside_until_they_are_due() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let message = NewMessage {
        delay_ms: Some(5_000),
        ..body(1)
    };
    let published = publish(&mut db, message, 1000).unwrap();
    assert_eq!((db.counts("jobs").delayed, db.counts("jobs").log), (1, 0));
    let stored = db
        .run(1000, |tx| tx.message(id(&published)))
        .unwrap()
        .unwrap();
    assert_eq!(stored.deliver_at, Some(6_000));

    db.tick(5_999);
    assert!(take(&mut db, "default", 1, 5_999).is_empty());
    db.tick(6_000);
    assert_eq!((db.counts("jobs").delayed, db.counts("jobs").log), (0, 1));
    assert_eq!(take(&mut db, "default", 1, 6_000).len(), 1);
}

#[test]
fn messages_can_be_delayed_by_at_most_a_year() {
    let mut db = TestDb::with_queue(QueueSettings::default());
    let now = 1000;
    let delayed = |ms| NewMessage {
        delay_ms: Some(ms),
        ..body(1)
    };
    let at = |ms| NewMessage {
        deliver_at: Some(time::format(ms)),
        ..body(1)
    };
    for too_late in [
        delayed(MAX_DELAY_MS + 1),
        delayed(u64::MAX),
        at(now + MAX_DELAY_MS + 1),
    ] {
        let err = publish(&mut db, too_late, now).unwrap_err();
        assert_eq!(err.kind(), "invalid", "{err}");
    }
    let published = publish(&mut db, delayed(MAX_DELAY_MS), now).unwrap();
    let stored = db.run(now, |tx| tx.message(id(&published))).unwrap();
    assert_eq!(stored.unwrap().deliver_at, Some(now + MAX_DELAY_MS));
    publish(&mut db, at(now + MAX_DELAY_MS), now).unwrap();
    assert_eq!(db.counts("jobs").delayed, 2);
}

#[test]
fn rejects_invalid_messages() {
    let bad = [
        NewMessage {
            priority: Some(10),
            ..body(1)
        },
        NewMessage {
            deliver_at: Some("soon".to_string()),
            ..body(1)
        },
        NewMessage {
            dedup_id: Some(String::new()),
            ..body(1)
        },
    ];
    for message in bad {
        assert!(matches!(check(message, 0), Err(Error::Invalid(_))));
    }
    let mut db = TestDb::with_queue(QueueSettings::default());
    let checked = vec![check(body(1), 0).unwrap()];
    assert!(matches!(
        db.run(0, |tx| tx.publish("missing", checked)),
        Err(Error::NotFound(_))
    ));
}
