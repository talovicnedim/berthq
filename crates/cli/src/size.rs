//! Sizes on the command line: `1GB`, `500MB`, `0`. A KB is 1024 bytes.

/// Longest suffix first: `GB` also ends with `B`.
const UNITS: [(&str, u64); 5] = [
    ("TB", 1 << 40),
    ("GB", 1 << 30),
    ("MB", 1 << 20),
    ("KB", 1 << 10),
    ("B", 1),
];

pub fn parse(text: &str) -> Result<u64, String> {
    let upper = text.trim().to_ascii_uppercase();
    let (number, unit) = UNITS
        .iter()
        .find_map(|(suffix, unit)| upper.strip_suffix(suffix).map(|number| (number, *unit)))
        .unwrap_or((upper.as_str(), 1));
    let number: u64 = number.trim().parse().map_err(|_| {
        format!("invalid size {text}: use a number with KB, MB, GB or TB, e.g. 1GB")
    })?;
    number
        .checked_mul(unit)
        .ok_or_else(|| format!("size {text} is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_use_units_of_1024() {
        assert_eq!(parse("0"), Ok(0));
        assert_eq!(parse("1GB"), Ok(1 << 30));
        assert_eq!(parse("500mb"), Ok(500 << 20));
        assert_eq!(parse("2 TB"), Ok(2 << 40));
        assert_eq!(parse("2048"), Ok(2048));
        assert!(parse("1 gigabyte").is_err());
        assert!(parse("-1GB").is_err());
    }
}
