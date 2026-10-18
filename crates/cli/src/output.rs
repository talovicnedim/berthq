//! Writing results to the terminal: aligned tables, numbers and JSON.

use std::io::Write;

use serde::Serialize;

pub fn line(text: &str) {
    // A closed pipe (e.g. `| head`) isn't worth failing over.
    let _ = write_line(text);
}

/// Like `line`, but says when nobody reads the output any more (a closed pipe).
pub fn write_line(text: &str) -> std::io::Result<()> {
    writeln!(std::io::stdout().lock(), "{text}")
}

pub fn note(text: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{text}");
}

pub fn json(value: &impl Serialize) {
    match serde_json::to_string_pretty(value) {
        Ok(text) => line(&text),
        Err(err) => note(&format!("error: failed to format json: {err}")),
    }
}

/// `value` as JSON with `--json`, otherwise `message`.
pub fn done(json: bool, value: &impl Serialize, message: &str) {
    if json {
        self::json(value);
    } else {
        line(message);
    }
}

/// One value per line, for output that other tools read line by line.
pub fn json_line(value: &impl Serialize) {
    match serde_json::to_string(value) {
        Ok(text) => line(&text),
        Err(err) => note(&format!("error: failed to format json: {err}")),
    }
}

/// Columns padded to their widest cell; numbers (and headers over them) are right-aligned.
pub fn table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let numeric: Vec<bool> = (0..headers.len())
        .map(|i| {
            !rows.is_empty()
                && rows
                    .iter()
                    .all(|row| row.get(i).is_some_and(|c| is_number(c)))
        })
        .collect();
    let format_row = |cells: Vec<&str>| {
        cells
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let width = widths[i];
                if numeric[i] {
                    format!("{cell:>width$}")
                } else {
                    format!("{cell:<width$}")
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    line(&format_row(headers.to_vec()));
    for row in rows {
        line(&format_row(row.iter().map(String::as_str).collect()));
    }
}

fn is_number(cell: &str) -> bool {
    !cell.is_empty()
        && cell
            .chars()
            .all(|c| c.is_ascii_digit() || c == ',' || c == '-')
}

/// `82140` → `82,140`.
pub fn number(n: impl Into<u64>) -> String {
    let digits = n.into().to_string();
    let mut out = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// `1 worker`, `3 workers`.
pub fn count(n: usize, noun: &str) -> String {
    let plural = if n == 1 { "" } else { "s" };
    format!("{} {noun}{plural}", number(n as u64))
}

pub fn or_dash(value: Option<&str>) -> String {
    value.unwrap_or("-").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_get_thousands_separators() {
        assert_eq!(number(0u32), "0");
        assert_eq!(number(999u32), "999");
        assert_eq!(number(82_140u32), "82,140");
        assert_eq!(number(1_234_567u64), "1,234,567");
    }
}
