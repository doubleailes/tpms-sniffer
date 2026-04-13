pub mod db;
pub mod resolver;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

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
