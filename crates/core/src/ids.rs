//! Message ids: ULIDs, so ids sort by creation time. Plus random lease tokens.

use ulid::Ulid;

/// Hands out strictly increasing ULIDs, so messages published in the same millisecond still keep
/// their publish order.
pub struct IdGenerator {
    last: Ulid,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self { last: Ulid::nil() }
    }

    pub fn next(&mut self, now_ms: u64) -> Ulid {
        let candidate = Ulid::from_parts(now_ms, random_bits());
        self.last = if candidate > self.last {
            candidate
        } else {
            self.last.increment().unwrap_or(candidate)
        };
        self.last
    }
}

/// Not cryptographic: ids only need to be unique and unpredictable enough to avoid collisions
/// between restarts, which the timestamp part already mostly guarantees.
fn random_bits() -> u128 {
    use std::hash::{BuildHasher, Hasher};
    let state = std::collections::hash_map::RandomState::new();
    let mut high = state.build_hasher();
    high.write_u8(1);
    let mut low = state.build_hasher();
    low.write_u8(2);
    (u128::from(high.finish()) << 64 | u128::from(low.finish())) & ((1 << 80) - 1)
}

/// Random enough to tell two deliveries of the same message apart; not for secrets.
pub fn random_u64() -> u64 {
    (random_bits() >> 16) as u64
}

pub fn parse(text: &str) -> Option<Ulid> {
    Ulid::from_string(text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_increase_within_one_millisecond() {
        let mut ids = IdGenerator::new();
        let first = ids.next(1000);
        let second = ids.next(1000);
        let earlier_clock = ids.next(999);
        assert!(first < second && second < earlier_clock);
    }

    #[test]
    fn ids_round_trip_as_text() {
        let id = IdGenerator::new().next(1_791_726_062_123);
        assert_eq!(parse(&id.to_string()), Some(id));
        assert_eq!(id.timestamp_ms(), 1_791_726_062_123);
        assert_eq!(parse("not-an-id"), None);
    }
}
