use super::*;
use crate::settings::QueueSettings;
use std::collections::HashMap;

#[test]
fn sample_records_deltas_per_second() {
    let mut state = Memory::new();
    state.queues.insert(
        "jobs".to_string(),
        QueueMemory::new("jobs", QueueSettings::default(), 0),
    );
    let mut previous = HashMap::new();

    state.queue_mut("jobs").unwrap().counters.published = 5;
    state.sample(1000, &mut previous);
    state.queue_mut("jobs").unwrap().counters.published = 12;
    state.queue_mut("jobs").unwrap().counters.retried = 1;
    state.sample(2000, &mut previous);

    let queue = state.queue("jobs").unwrap();
    let published: Vec<u64> = queue.history.iter().map(|p| p.published).collect();
    assert_eq!(published, [5, 7]);
    assert_eq!(queue.history[1].failed, 1);
    assert_eq!(state.history.len(), 2);
    assert_eq!(queue.stats().publish_rate, 7);
}

#[test]
fn history_keeps_the_last_minute() {
    let mut state = Memory::new();
    let mut previous = HashMap::new();
    for second in 0..90 {
        state.sample(second * 1000, &mut previous);
    }
    assert_eq!(state.history.len(), HISTORY_LEN);
}
