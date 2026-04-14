//! Step 8c — `parse_timestamp_to_utc` parity with `archive_cli.features.parse_timestamp_to_utc`
//! (dateutil `isoparse` + naive → `PPA_DEFAULT_TIMEZONE` + UTC).

use std::sync::OnceLock;

use chrono::{DateTime, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use jiff::{Timestamp, Zoned};
use pyo3::prelude::*;

static DEFAULT_TZ_NAME: OnceLock<String> = OnceLock::new();

fn default_tz_name() -> &'static str {
    DEFAULT_TZ_NAME
        .get_or_init(|| std::env::var("PPA_DEFAULT_TIMEZONE").unwrap_or_else(|_| "UTC".to_string()))
        .as_str()
}

fn chrono_from_jiff_timestamp(ts: Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(ts.as_second(), ts.subsec_nanosecond() as u32)
}

/// Parse ISO-like timestamps; naive datetimes use `PPA_DEFAULT_TIMEZONE` (default `UTC`).
pub fn parse_timestamp_to_utc_rust(value: &str) -> Option<DateTime<Utc>> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(z) = s.parse::<Zoned>() {
        let ts = z.timestamp();
        return chrono_from_jiff_timestamp(ts);
    }
    if let Ok(ts) = s.parse::<Timestamp>() {
        return chrono_from_jiff_timestamp(ts);
    }
    // chrono fallbacks (RFC3339 / common ISO)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return naive_local_to_utc(naive);
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return naive_local_to_utc(d.and_hms_opt(0, 0, 0)?);
    }
    None
}

fn naive_local_to_utc(naive: NaiveDateTime) -> Option<DateTime<Utc>> {
    let tz_name = default_tz_name();
    let tz: Tz = tz_name.parse().ok()?;
    match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}

/// Convert UTC `DateTime` to a timezone-aware Python `datetime` (UTC).
pub fn utc_datetime_to_py(py: Python<'_>, dt: &DateTime<Utc>) -> PyResult<PyObject> {
    let datetime_mod = py.import_bound("datetime")?;
    let tz_utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
    let dt_cls = datetime_mod.getattr("datetime")?;
    let secs = dt.timestamp() as f64 + (dt.timestamp_subsec_nanos() as f64) / 1e9_f64;
    dt_cls.call_method1("fromtimestamp", (secs, tz_utc))?.extract()
}

/// `Option<DateTime<Utc>>` → Python `datetime` or `None`.
pub fn optional_utc_to_py(
    py: Python<'_>,
    dt: Option<DateTime<Utc>>,
) -> PyResult<Option<PyObject>> {
    match dt {
        Some(d) => Ok(Some(utc_datetime_to_py(py, &d)?)),
        None => Ok(None),
    }
}
