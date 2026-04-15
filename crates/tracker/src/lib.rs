pub mod analytics;
pub mod classification;
pub mod db;
pub mod jaccard;
pub mod replay;
pub mod resolver;

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Cross-receiver deduplication constants
// ---------------------------------------------------------------------------

/// Two sightings of the same vehicle fingerprint from different receivers
/// within this window (ms) are treated as the same physical event.
pub const CROSS_RECEIVER_WINDOW_MS: u64 = 5_000;

// ---------------------------------------------------------------------------
// TX-interval tuning constants
// ---------------------------------------------------------------------------

/// Ring buffer depth for inter-packet intervals.
pub const TX_INTERVAL_WINDOW: usize = 8;

/// Minimum number of recorded intervals before the median is used in matching.
pub const TX_INTERVAL_MIN_SAMPLES: usize = 3;

/// Intervals longer than this (ms) are discarded (e.g. expiry gaps, silence).
pub const TX_INTERVAL_MAX_MS: u32 = 120_000;

/// Match tolerance (ms) for comparing two interval medians.  ±8 s is
/// deliberately wide for the initial implementation — sensor crystals drift
/// with temperature and the tracker's interval measurement includes SDR jitter.
pub const TX_INTERVAL_TOLERANCE_MS: u32 = 8_000;

/// Raw packet emitted by `tpms-sniffer` on stdout (one JSON object per line).
/// Only the fields the tracker needs are declared; unknown fields are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct TpmsPacket {
    pub timestamp: String,
    pub protocol: String,
    pub rtl433_id: u16,
    pub sensor_id: String, // hex string e.g. "0x1A2B3C4D"
    pub pressure_kpa: f32,
    pub temp_c: Option<f32>,
    pub battery_ok: Option<bool>,
    pub alarm: Option<bool>,
    pub confidence: u8,
    /// `false` when the decoder tagged the pressure reading as a protocol
    /// artifact (e.g. AVE-TPMS half-range low-pressure frame). Missing in
    /// older JSON captures — defaults to `true`.
    #[serde(default = "default_pressure_reliable")]
    pub pressure_kpa_reliable: bool,
    /// Identifier for the receiver node that captured this packet.  Populated
    /// from `--receiver-id` on the sending node; defaults to `"default"` when
    /// absent in the JSON stream.
    #[serde(default = "default_receiver_id")]
    pub receiver_id: String,
}

fn default_receiver_id() -> String {
    "default".to_string()
}

fn default_pressure_reliable() -> bool {
    true
}

impl TpmsPacket {
    /// Parse the hex sensor_id string to a u32.
    pub fn sensor_id_u32(&self) -> Option<u32> {
        parse_sensor_id(&self.sensor_id)
    }

    /// Parse the timestamp string to `DateTime<Utc>`.
    /// The sniffer emits local time without a timezone offset; we treat it as UTC.
    pub fn parsed_ts(&self) -> Option<DateTime<Utc>> {
        NaiveDateTime::parse_from_str(&self.timestamp, "%Y-%m-%d %H:%M:%S%.3f")
            .ok()
            .map(|ndt| ndt.and_utc())
    }
}

/// A single normalised TPMS sighting ready for persistence.
#[derive(Debug, Clone)]
pub struct Sighting {
    pub ts: DateTime<Utc>,
    pub protocol: String,
    pub rtl433_id: u16,
    pub sensor_id: u32,
    pub pressure_kpa: f32,
    /// `None` when the raw value is a sentinel (≥ 200 °C).
    pub temp_c: Option<f32>,
    pub alarm: bool,
    pub battery_ok: bool,
    /// `false` when the decoded pressure value is a protocol-level artifact
    /// rather than a genuine reading (e.g. AVE-TPMS dual-range half-pressure
    /// frames, see `AVE_MIN_RELIABLE_KPA` in the sniffer decoder).  The
    /// tracker skips pressure-fingerprint updates for unreliable readings so
    /// they do not drift the vehicle's stored average.
    pub pressure_reliable: bool,
    /// Interval hint (ms) computed from the gap since the last packet of the
    /// same protocol in the input stream.  `None` for the very first packet of
    /// a protocol or when the gap exceeds `TX_INTERVAL_MAX_MS`.
    pub tx_interval_hint_ms: Option<u32>,
    /// Identifier for the receiver node that captured this packet.
    pub receiver_id: String,
}

/// Long-lived record for a vehicle inferred from repeated sightings.
#[derive(Debug, Clone)]
pub struct VehicleTrack {
    pub vehicle_id: Uuid,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub sighting_count: u32,
    pub protocol: String,
    /// Canonical rtl_433 decoder ID. Paired with `fixed_sensor_id` to form the
    /// fixed-ID map key so that two sensors from different protocols that
    /// happen to share a `sensor_id` value cannot be merged into the same
    /// vehicle UUID.
    pub rtl433_id: u16,
    /// Stable for pre-2018 fixed-ID sensors; `None` for rolling-ID protocols.
    pub fixed_sensor_id: Option<u32>,
    /// Exponential-moving-average pressure per wheel slot (kPa).
    /// For fixed-ID sensors only slot 0 is used.
    pub pressure_signature: [f32; 4],
    pub make_model_hint: Option<String>,
    /// Most recent battery status reported by the sensor.  `true` means the
    /// battery is OK; `false` means the sensor has flagged low battery.
    /// Defaults to `true` when no battery field is present in the packet.
    pub battery_ok: bool,
    /// Ring buffer of recent inter-packet intervals (ms), capped at
    /// `TX_INTERVAL_WINDOW`.
    pub tx_intervals_ms: VecDeque<u32>,
    /// Median of `tx_intervals_ms`; `None` until at least
    /// `TX_INTERVAL_MIN_SAMPLES` intervals have been collected.
    pub tx_interval_median_ms: Option<u32>,
    /// ID of the car group this vehicle belongs to (set by Jaccard grouping).
    /// `None` until the co-occurrence grouper has enough data to assign it.
    pub car_id: Option<Uuid>,
    /// Per-receiver sighting timestamps.  Keys are receiver IDs; values are
    /// chronologically ordered timestamps of sightings from that receiver.
    /// Used for direction-of-travel inference and cross-receiver metadata.
    pub receiver_sightings: HashMap<String, Vec<DateTime<Utc>>>,
    /// Inferred wheel position (FL/FR/RL/RR) based on trailing-byte analysis.
    /// `None` when inference is not possible or not applicable.
    pub wheel_position: Option<jaccard::WheelPosition>,
    /// Inferred vehicle class based on pressure range and sensor count.
    pub vehicle_class: classification::VehicleClass,
}

/// Return a human-readable make/model hint for a given rtl_433 protocol ID.
pub fn make_model_hint(rtl433_id: u16) -> Option<&'static str> {
    match rtl433_id {
        59 => Some("Schrader EG53MA4"),
        82 => Some("Citroën / Peugeot / Fiat / Mitsubishi"),
        88 => Some("Toyota Auris / Corolla / Lexus"),
        89 => Some("Ford Fiesta / Focus / Kuga / Transit"),
        90 => Some("Renault / Dacia"),
        95 => Some("Saab / Opel / Vauxhall / Chevrolet"),
        140 => Some("Hyundai Elantra 2012"),
        208 => Some("AVE (rolling-ID)"),
        241 => Some("EezTire"),
        252 => Some("BMW / Audi / VW (Gen4/5)"),
        257 => Some("BMW (Gen2/3)"),
        298 => Some("TRW-OOK"),
        _ => None,
    }
}

/// Parse "0x1A2B3C4D" (with or without prefix) or plain decimal into a u32.
pub fn parse_sensor_id(s: &str) -> Option<u32> {
    let t = s.trim();
    if let Some(hex) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        u32::from_str_radix(hex, 16).ok()
    } else {
        t.parse().ok()
    }
}

/// Compute the median of a `VecDeque<u32>`.  Returns `None` when the deque has
/// fewer than `TX_INTERVAL_MIN_SAMPLES` elements.
pub fn compute_median(buf: &VecDeque<u32>) -> Option<u32> {
    if buf.len() < TX_INTERVAL_MIN_SAMPLES {
        return None;
    }
    let mut sorted: Vec<u32> = buf.iter().copied().collect();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        Some((sorted[mid - 1] + sorted[mid]) / 2)
    } else {
        Some(sorted[mid])
    }
}
