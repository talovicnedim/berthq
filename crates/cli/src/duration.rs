//! Durations like `500ms`, `30s`, `5m`, `1h` and `2d`.

use std::time::Duration;

pub fn parse(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("invalid duration {text}: use e.g. 500ms, 30s, 5m, 1h or 2d"))?;
    let millis = match unit.trim() {
        "ms" => 1.0,
        "s" | "" => 1_000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => {
            return Err(format!(
                "invalid duration {text}: use e.g. 500ms, 30s, 5m, 1h or 2d"
            ));
        }
    };
    Duration::try_from_secs_f64(number * millis / 1000.0)
        .map_err(|_| format!("duration {text} is too long"))
}

/// Whole milliseconds, as the API expects.
pub fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The shortest readable form, e.g. `30s`, `5m`, `1h30m`.
pub fn format(ms: u64) -> String {
    if ms == 0 {
        return "0s".to_string();
    }
    if ms < 1000 || !ms.is_multiple_of(1000) {
        return format!("{ms}ms");
    }
    let mut seconds = ms / 1000;
    let mut parts = String::new();
    for (unit, size) in [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)] {
        if seconds >= size {
            parts.push_str(&format!("{}{unit}", seconds / size));
            seconds %= size;
        }
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units_and_bare_seconds() {
        assert_eq!(parse("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse("1.5m"), Ok(Duration::from_secs(90)));
        assert_eq!(parse("2d"), Ok(Duration::from_secs(172_800)));
        assert_eq!(parse("15"), Ok(Duration::from_secs(15)));
        assert!(parse("soon").is_err());
        assert!(parse("5 weeks").is_err());
    }

    #[test]
    fn refuses_durations_too_long_to_hold() {
        let err = parse("99999999999999999999999d").unwrap_err();
        assert!(err.contains("too long"), "{err}");
    }

    #[test]
    fn formats_compactly() {
        assert_eq!(format(30_000), "30s");
        assert_eq!(format(5_400_000), "1h30m");
        assert_eq!(format(1_500), "1500ms");
        assert_eq!(format(0), "0s");
    }
}
