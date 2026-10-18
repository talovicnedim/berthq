//! Message bodies are kept as the JSON text that was sent; this checks that every client can
//! read that text back.

use std::fmt;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use crate::error::{Error, Result};

/// Refuses what JSON parsers refuse although the syntax is right (numbers out of range, lone
/// surrogates, nesting deeper than 128), so every client can read the body back. It walks the
/// text once and keeps nothing.
pub(crate) fn check_readable(body: &RawValue) -> Result<()> {
    serde_json::from_str::<Readable>(body.get())
        .map(|_| ())
        .map_err(|err| Error::Invalid(format!("body can't be read as JSON: {err}")))
}

struct Readable;

impl<'de> Deserialize<'de> for Readable {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(Readable)
    }
}

impl<'de> Visitor<'de> for Readable {
    type Value = Readable;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_bool<E>(self, _: bool) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_i64<E>(self, _: i64) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_u64<E>(self, _: u64) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_f64<E>(self, _: f64) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_str<E>(self, _: &str) -> std::result::Result<Readable, E> {
        Ok(Readable)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Readable, A::Error> {
        while seq.next_element::<Readable>()?.is_some() {}
        Ok(Readable)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Readable, A::Error> {
        while map.next_entry::<Readable, Readable>()?.is_some() {}
        Ok(Readable)
    }
}
