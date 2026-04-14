use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use std::collections::VecDeque;
use uuid::Uuid;

use crate::db::Database;
use crate::{
    Sighting, TpmsPacket, VehicleTrack, compute_median, make_model_hint, TX_INTERVAL_MAX_MS,
    TX_INTERVAL_TOLERANCE_MS, TX_INTERVAL_WINDOW,
};

/// rtl_433 protocol IDs that transmit rolling (non-stable) sensor IDs.
/// For these we cluster packets into time-window bursts and fingerprint by
/// the resulting pressure tuple.
const ROLLING_ID_PROTOCOLS: &[u16] = &[
    208, // AVE
];

/// rtl_433 protocol IDs whose sensor ID field is unreliable because individual
/// bits flip between transmissions (so every packet looks like a different
/// sensor).  For these we ignore the sensor ID entirely and correlate each
/// packet against active vehicles by protocol + pressure fingerprint.
const BIT_FLIP_ID_PROTOCOLS: &[u16] = &[
    241, // EezTire
];

/// Rejects sensor IDs that are decode artifacts rather than real identifiers.
///
/// When a protocol decoder cannot extract all bits of the sensor ID field
/// reliably, unresolved bits default to `1`, producing values close to
/// `0xFFFFFFFF` (e.g. `0xFFFFFFFD`, `0xFFFFFFF7`, `0xFFFFFFFB`, `0xFFFFFFBF`).
/// These near-sentinel IDs appear across multiple protocols and, if treated
/// as legitimate fixed IDs, cause cross-protocol vehicle merging in the
/// fixed-ID map.
///
/// The check rejects all-zeros and any ID with fewer than 2 bits cleared from
/// the all-ones state. A `>= 2` threshold (rather than the `>= 3` originally
/// proposed) preserves the confirmed real TRW sensor `0xFEFFFFFD`, which has
/// exactly 2 bits cleared. Raising the threshold further would regress that
/// known-good fixed-ID track.
fn is_valid_sensor_id(id: u32) -> bool {
    // Reject 0x00000000 and any ID with ≤ 1 bit cleared from 0xFFFFFFFF.
    // IDs with 0–1 bits cleared are decode artifacts (unresolved bit fields).
    id != 0x00000000 && id.count_zeros() >= 2
}

/// Maximum gap (ms) between consecutive packets belonging to the same burst.
const BURST_GAP_MS: i64 = 200;

/// Maximum wheel slots per burst.
const BURST_MAX_WHEELS: usize = 4;

/// L1 distance threshold (kPa, per-wheel average) for matching a new burst
/// against a known vehicle's pressure signature.
const PRESSURE_MATCH_TOLERANCE_KPA: f32 = 5.0;

/// How long a vehicle remains eligible for pressure-fingerprint correlation
/// after its most recent sighting.  After this gap we assume the sensor has
/// gone out of range and any new match is a different vehicle.
///
/// Some protocols transmit infrequently when the vehicle is stationary and
/// need a longer expiry than the default.  See `crates/tracker/README.md`
/// for the documented per-protocol values and rationale.
fn vehicle_expiry_for(rtl433_id: u16) -> Duration {
    match rtl433_id {
        208 => Duration::seconds(600), // AVE-TPMS: low TX rate when parked
        241 => Duration::seconds(480), // EezTire: may be slow when low battery
        298 => Duration::seconds(480), // TRW-OOK: similar
        _ => Duration::seconds(300),   // default: 5 minutes
    }
}

/// Compute the effective expiry for a vehicle, extending the base per-protocol
/// window when the sensor has reported low battery.  A low-battery sensor may
/// transmit less frequently but is still physically present; the extension
/// avoids spurious track restarts.
fn effective_expiry(vehicle: &VehicleTrack) -> Duration {
    let base = vehicle_expiry_for(vehicle.rtl433_id);
    if !vehicle.battery_ok {
        base + Duration::seconds(300) // extend by 5 min for low-battery sensors
    } else {
        base
    }
}

/// EMA weight for updating the pressure signature (higher = faster adaptation).
const EMA_ALPHA: f32 = 0.2;

// ---------------------------------------------------------------------------
// Internal accumulator for rolling-ID bursts
// ---------------------------------------------------------------------------

struct BurstAccumulator {
    protocol: String,
    rtl433_id: u16,
    pressures: Vec<f32>,
    last_ts: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

pub struct Resolver {
    /// `(sensor_id, rtl433_id) → vehicle_id` for fixed-ID protocols.
    ///
    /// The decoder ID is part of the key so that two sensors from different
    /// protocols that happen to share a `sensor_id` value cannot be absorbed
    /// into the same vehicle UUID. See the issue "Fixed-ID lookup does not
    /// verify protocol, allows near-sentinel cross-protocol absorption" for
    /// background.
    fixed_map: HashMap<(u32, u16), Uuid>,
    /// All tracked vehicles keyed by vehicle_id.
    vehicles: HashMap<Uuid, VehicleTrack>,
    /// Accumulator for an in-progress rolling-ID burst.
    pending_burst: Option<BurstAccumulator>,
    /// Tracks the timestamp of the last packet seen per `rtl433_id`, used to
    /// compute `tx_interval_hint_ms` on incoming sightings.
    last_seen_by_protocol: HashMap<u16, DateTime<Utc>>,
    db: Database,
}

impl Resolver {
    /// Open (or create) the database and restore in-memory state from it.
    pub fn new(db: Database) -> Result<Self> {
        let mut r = Self {
            fixed_map: HashMap::new(),
            vehicles: HashMap::new(),
            pending_burst: None,
            last_seen_by_protocol: HashMap::new(),
            db,
        };
        r.load_from_db()?;
        Ok(r)
    }

    fn load_from_db(&mut self) -> Result<()> {
        for vehicle in self.db.all_vehicles()? {
            if let Some(sid) = vehicle.fixed_sensor_id {
                self.fixed_map
                    .insert((sid, vehicle.rtl433_id), vehicle.vehicle_id);
            }
            self.vehicles.insert(vehicle.vehicle_id, vehicle);
        }
        Ok(())
    }

    /// Process one decoded TPMS packet.
    ///
    /// Returns the vehicle UUID if it could be resolved immediately.  For
    /// rolling-ID protocols the UUID is returned only when a complete burst
    /// has been correlated; returns `None` while the burst is still building.
    pub fn process(&mut self, packet: &TpmsPacket) -> Result<Option<Uuid>> {
        let ts = packet.parsed_ts().unwrap_or_else(Utc::now);
        let Some(sensor_id) = packet.sensor_id_u32() else {
            return Ok(None);
        };

        // Discard temperature sentinel values (≥ 200 °C means "not available").
        let temp_c = packet.temp_c.filter(|&t| t < 200.0);

        // Compute tx_interval_hint from the last packet of the same protocol.
        let hint = self
            .last_seen_by_protocol
            .get(&packet.rtl433_id)
            .map(|&prev| (ts - prev).num_milliseconds() as u32)
            .filter(|&ms| ms < TX_INTERVAL_MAX_MS);
        self.last_seen_by_protocol.insert(packet.rtl433_id, ts);

        let sighting = Sighting {
            ts,
            protocol: packet.protocol.clone(),
            rtl433_id: packet.rtl433_id,
            sensor_id,
            pressure_kpa: packet.pressure_kpa,
            temp_c,
            alarm: packet.alarm.unwrap_or(false),
            battery_ok: packet.battery_ok.unwrap_or(true),
            pressure_reliable: packet.pressure_kpa_reliable,
            tx_interval_hint_ms: hint,
        };

        if BIT_FLIP_ID_PROTOCOLS.contains(&packet.rtl433_id) {
            self.process_fingerprint(sighting)
        } else if ROLLING_ID_PROTOCOLS.contains(&packet.rtl433_id) || !is_valid_sensor_id(sensor_id)
        {
            self.process_rolling(sighting)
        } else {
            self.process_fixed(sighting, packet.rtl433_id)
        }
    }

    /// Flush any pending rolling-ID burst.  Call this when the input stream ends.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(burst) = self.pending_burst.take() {
            if burst.pressures.len() >= 2 {
                self.resolve_burst(&burst)?;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Fixed-ID path
    // -----------------------------------------------------------------------

    fn process_fixed(
        &mut self,
        sighting: Sighting,
        rtl433_id: u16,
    ) -> Result<Option<Uuid>> {
        let sensor_id = sighting.sensor_id;
        // Key the fixed-ID map on `(sensor_id, rtl433_id)` so that two
        // sensors from different decoders sharing a `sensor_id` value produce
        // distinct vehicle UUIDs instead of merging.
        let key = (sensor_id, rtl433_id);

        // Look up or create the vehicle.
        let vehicle_id = if let Some(&vid) = self.fixed_map.get(&key) {
            vid
        } else if rtl433_id != 0 {
            // Legacy backward-compat bridge: a migrated row may have
            // `rtl433_id=0` because the column did not exist at insert time.
            // Check whether `(sensor_id, 0)` already maps to a vehicle whose
            // protocol matches, and if so adopt it rather than creating a
            // duplicate.
            let legacy_key = (sensor_id, 0u16);
            if let Some(&legacy_vid) = self.fixed_map.get(&legacy_key) {
                let protocol_matches = self
                    .vehicles
                    .get(&legacy_vid)
                    .map_or(false, |v| v.protocol == sighting.protocol);
                if protocol_matches {
                    // Migrate the in-memory key from (sensor_id, 0) to the
                    // real (sensor_id, rtl433_id) and correct the stored value.
                    self.fixed_map.remove(&legacy_key);
                    self.fixed_map.insert(key, legacy_vid);
                    if let Some(v) = self.vehicles.get_mut(&legacy_vid) {
                        v.rtl433_id = rtl433_id;
                    }
                    self.db.update_rtl433_id(legacy_vid, rtl433_id)?;
                    legacy_vid
                } else {
                    // Different protocol — create a new vehicle as normal.
                    let vid = Uuid::new_v4();
                    let vehicle = VehicleTrack {
                        vehicle_id: vid,
                        first_seen: sighting.ts,
                        last_seen: sighting.ts,
                        sighting_count: 0,
                        protocol: sighting.protocol.clone(),
                        rtl433_id,
                        fixed_sensor_id: Some(sensor_id),
                        pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                        make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                        battery_ok: sighting.battery_ok,
                        tx_intervals_ms: VecDeque::new(),
                        tx_interval_median_ms: None,
                    };
                    self.fixed_map.insert(key, vid);
                    self.vehicles.insert(vid, vehicle);
                    vid
                }
            } else {
                let vid = Uuid::new_v4();
                let vehicle = VehicleTrack {
                    vehicle_id: vid,
                    first_seen: sighting.ts,
                    last_seen: sighting.ts,
                    sighting_count: 0,
                    protocol: sighting.protocol.clone(),
                    rtl433_id,
                    fixed_sensor_id: Some(sensor_id),
                    pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                    make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                    battery_ok: sighting.battery_ok,
                    tx_intervals_ms: VecDeque::new(),
                    tx_interval_median_ms: None,
                };
                self.fixed_map.insert(key, vid);
                self.vehicles.insert(vid, vehicle);
                vid
            }
        } else {
            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: sighting.ts,
                last_seen: sighting.ts,
                sighting_count: 0,
                protocol: sighting.protocol.clone(),
                rtl433_id,
                fixed_sensor_id: Some(sensor_id),
                pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                battery_ok: sighting.battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
            };
            self.fixed_map.insert(key, vid);
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        // Accumulate inter-packet interval before updating last_seen.
        let interval_ms = (sighting.ts - vehicle.last_seen).num_milliseconds() as u32;
        if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
            vehicle.tx_intervals_ms.push_back(interval_ms);
            if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                vehicle.tx_intervals_ms.pop_front();
            }
            vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
        }
        vehicle.last_seen = sighting.ts;
        vehicle.sighting_count += 1;
        vehicle.battery_ok = sighting.battery_ok;
        if sighting.pressure_reliable {
            ema_update(&mut vehicle.pressure_signature[0], sighting.pressure_kpa);
        }

        // Persist.
        self.db.upsert_vehicle(vehicle)?;
        self.db.insert_sighting(&sighting, vehicle_id)?;

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Fingerprint path (single-packet pressure correlation)
    // -----------------------------------------------------------------------

    /// Correlate a single packet against active vehicles of the same protocol
    /// using the pressure fingerprint.  Used for protocols whose sensor ID
    /// field is unreliable because bits flip between transmissions (e.g.
    /// EezTire), which would otherwise cause every packet to create a new
    /// vehicle via the fixed-ID path.
    fn process_fingerprint(
        &mut self,
        sighting: Sighting,
    ) -> Result<Option<Uuid>> {
        let now = sighting.ts;
        let pressure = sighting.pressure_kpa;
        let protocol = sighting.protocol.clone();
        let rtl433_id = sighting.rtl433_id;
        let hint = sighting.tx_interval_hint_ms;

        // Find an active vehicle of the same protocol whose pressure
        // signature is within tolerance.  Use `rtl433_id` (not the display
        // name string) so the filter applies unconditionally to all callers,
        // including sentinel-rejected fixed-ID packets that fall through to
        // this path.  Copy the Uuid so we drop the shared borrow before
        // taking a mutable one below.
        //
        // The interval check is applied only when both the candidate vehicle
        // and the sighting carry enough samples.  When either side lacks data,
        // pressure match alone is sufficient.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.rtl433_id == rtl433_id && v.fixed_sensor_id.is_none())
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .find(|v| {
                let pressure_ok =
                    (v.pressure_signature[0] - pressure).abs() <= PRESSURE_MATCH_TOLERANCE_KPA;
                if !pressure_ok {
                    return false;
                }
                // Interval guard: reject if both sides have data and intervals differ.
                if let (Some(v_interval), Some(s_interval)) =
                    (v.tx_interval_median_ms, hint)
                {
                    let delta = (v_interval as i64 - s_interval as i64).unsigned_abs() as u32;
                    if delta > TX_INTERVAL_TOLERANCE_MS {
                        return false;
                    }
                }
                true
            })
            .map(|v| v.vehicle_id);

        let vehicle_id = if let Some(vid) = matched_vid {
            vid
        } else {
            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: now,
                last_seen: now,
                sighting_count: 0,
                protocol: protocol.clone(),
                rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: [pressure, 0.0, 0.0, 0.0],
                make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                battery_ok: sighting.battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
            };
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        // Accumulate inter-packet interval before updating last_seen.
        let interval_ms = (now - vehicle.last_seen).num_milliseconds() as u32;
        if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
            vehicle.tx_intervals_ms.push_back(interval_ms);
            if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                vehicle.tx_intervals_ms.pop_front();
            }
            vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
        }
        vehicle.last_seen = now;
        vehicle.sighting_count += 1;
        vehicle.battery_ok = sighting.battery_ok;
        if sighting.pressure_reliable {
            ema_update(&mut vehicle.pressure_signature[0], pressure);
        }

        // Persist.
        self.db.upsert_vehicle(vehicle)?;
        self.db.insert_sighting(&sighting, vehicle_id)?;

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Rolling-ID path (burst accumulator)
    // -----------------------------------------------------------------------

    fn process_rolling(
        &mut self,
        sighting: Sighting,
    ) -> Result<Option<Uuid>> {
        // Unreliable readings (e.g. AVE half-range low-pressure frames) would
        // poison the pressure fingerprint if fed through the burst accumulator.
        // Drop them entirely: do not extend the pending burst, do not close it,
        // do not start a new one.  A sighting cannot be persisted here because
        // no vehicle UUID is known until a complete burst resolves — recording a
        // sighting requires a vehicle to attach it to.  The track for the
        // underlying vehicle keeps its last known good signature and is still
        // matched when the next normal packet arrives.
        if !sighting.pressure_reliable {
            return Ok(None);
        }

        let ts = sighting.ts;
        let protocol = sighting.protocol.clone();

        // Decide whether this packet extends the current burst or starts a new one.
        let start_new = match &self.pending_burst {
            Some(b) if b.protocol == protocol => {
                let gap_ms = (ts - b.last_ts).num_milliseconds();
                gap_ms > BURST_GAP_MS || b.pressures.len() >= BURST_MAX_WHEELS
            }
            _ => true,
        };

        let completed_vid = if start_new {
            // Close and resolve the completed burst (if any).
            let resolved = if let Some(old) = self.pending_burst.take() {
                if old.pressures.len() >= 2 {
                    self.resolve_burst(&old)?
                } else {
                    None
                }
            } else {
                None
            };

            // Open a new burst for this packet.
            self.pending_burst = Some(BurstAccumulator {
                protocol,
                rtl433_id: sighting.rtl433_id,
                pressures: vec![sighting.pressure_kpa],
                last_ts: ts,
            });

            resolved
        } else {
            // Extend the current burst.
            if let Some(burst) = &mut self.pending_burst {
                burst.pressures.push(sighting.pressure_kpa);
                burst.last_ts = ts;
            }
            None
        };

        Ok(completed_vid)
    }

    fn resolve_burst(&mut self, burst: &BurstAccumulator) -> Result<Option<Uuid>> {
        let now = burst.last_ts;
        let sig = burst_to_signature(&burst.pressures);

        // Find an existing rolling-ID vehicle whose pressure signature is close
        // enough and that was seen recently enough to still be active.  We copy
        // the Uuid (it's Copy) so we drop the shared borrow before taking the
        // mutable one below.
        //
        // When a candidate vehicle has an established interval median, also
        // check that the gap since the vehicle's last sighting is compatible
        // with its typical TX interval.  This prevents merging two sensors at
        // the same pressure when they transmit at different rates.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.fixed_sensor_id.is_none() && v.rtl433_id == burst.rtl433_id)
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .find(|v| {
                let pressure_ok =
                    l1_per_wheel(&v.pressure_signature, &sig) < PRESSURE_MATCH_TOLERANCE_KPA;
                if !pressure_ok {
                    return false;
                }
                // Interval guard: if the vehicle has an established median,
                // verify the gap since its last sighting is compatible.
                if let Some(v_median) = v.tx_interval_median_ms {
                    let gap_ms = now.signed_duration_since(v.last_seen).num_milliseconds();
                    if gap_ms > 0 && (gap_ms as u32) < TX_INTERVAL_MAX_MS {
                        let delta =
                            (v_median as i64 - gap_ms).unsigned_abs() as u32;
                        if delta > TX_INTERVAL_TOLERANCE_MS {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|v| v.vehicle_id);

        let vehicle_id = if let Some(vid) = matched_vid {
            vid
        } else {
            // First sighting of this vehicle — create a new record.
            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: now,
                last_seen: now,
                sighting_count: 0,
                protocol: burst.protocol.clone(),
                rtl433_id: burst.rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: sig,
                make_model_hint: make_model_hint(burst.rtl433_id).map(str::to_owned),
                battery_ok: true,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
            };
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        // Accumulate inter-burst interval before updating last_seen.
        let interval_ms = (now - vehicle.last_seen).num_milliseconds() as u32;
        if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
            vehicle.tx_intervals_ms.push_back(interval_ms);
            if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                vehicle.tx_intervals_ms.pop_front();
            }
            vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
        }
        vehicle.last_seen = now;
        vehicle.sighting_count += 1;
        for (i, &p) in burst.pressures.iter().enumerate().take(4) {
            ema_update(&mut vehicle.pressure_signature[i], p);
        }

        // Persist.
        self.db.upsert_vehicle(vehicle)?;

        Ok(Some(vehicle_id))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a 4-slot pressure signature from a burst (zero-pads unused slots).
fn burst_to_signature(pressures: &[f32]) -> [f32; 4] {
    let mut sig = [0.0f32; 4];
    for (i, &p) in pressures.iter().enumerate().take(4) {
        sig[i] = p;
    }
    sig
}

/// Mean per-wheel L1 distance between two pressure signatures.
/// Slots that are zero in either vector are excluded from the average so that
/// a vehicle seen with fewer than 4 wheels can still match.
fn l1_per_wheel(a: &[f32; 4], b: &[f32; 4]) -> f32 {
    let (sum, count) = a
        .iter()
        .zip(b.iter())
        .fold((0.0f32, 0u32), |(s, n), (&x, &y)| {
            if x == 0.0 || y == 0.0 {
                (s, n)
            } else {
                (s + (x - y).abs(), n + 1)
            }
        });
    if count == 0 {
        f32::MAX
    } else {
        sum / count as f32
    }
}

/// Exponential moving average update (in-place).
fn ema_update(slot: &mut f32, new_val: f32) {
    if *slot == 0.0 {
        *slot = new_val;
    } else {
        *slot = *slot * (1.0 - EMA_ALPHA) + new_val * EMA_ALPHA;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    fn make_packet(
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
    ) -> TpmsPacket {
        make_packet_at(
            "2025-06-01 12:00:00.000",
            sensor_id,
            protocol,
            rtl433_id,
            pressure_kpa,
        )
    }

    fn make_packet_at(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
    ) -> TpmsPacket {
        make_packet_at_reliable(timestamp, sensor_id, protocol, rtl433_id, pressure_kpa, true)
    }

    fn make_packet_at_reliable(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
        pressure_kpa_reliable: bool,
    ) -> TpmsPacket {
        TpmsPacket {
            timestamp: timestamp.to_string(),
            protocol: protocol.to_string(),
            rtl433_id,
            sensor_id: sensor_id.to_string(),
            pressure_kpa,
            temp_c: Some(25.0),
            battery_ok: Some(true),
            alarm: Some(false),
            confidence: 90,
            pressure_kpa_reliable,
        }
    }

    fn make_packet_at_battery(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
        battery_ok: bool,
    ) -> TpmsPacket {
        TpmsPacket {
            timestamp: timestamp.to_string(),
            protocol: protocol.to_string(),
            rtl433_id,
            sensor_id: sensor_id.to_string(),
            pressure_kpa,
            temp_c: Some(25.0),
            battery_ok: Some(battery_ok),
            alarm: Some(false),
            confidence: 90,
            pressure_kpa_reliable: true,
        }
    }

    fn in_memory_resolver() -> Resolver {
        let db = Database::open(":memory:").unwrap();
        Resolver::new(db).unwrap()
    }

    #[test]
    fn sentinel_0xffffffff_not_created_via_fixed_id_path() {
        let mut resolver = in_memory_resolver();

        // Send two packets with 0xFFFFFFFF from EezTire (fixed-ID protocol 241).
        let p1 = make_packet("0xFFFFFFFF", "EezTire", 241, 51.1);
        let p2 = make_packet("0xFFFFFFFF", "EezTire", 241, 51.2);
        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.flush().unwrap();

        // No vehicle should have a fixed_sensor_id of 0xFFFFFFFF.
        for v in resolver.vehicles.values() {
            assert_ne!(
                v.fixed_sensor_id,
                Some(0xFFFFFFFF),
                "sentinel 0xFFFFFFFF must not be stored as a fixed sensor ID"
            );
        }
        assert!(
            !resolver.fixed_map.keys().any(|(sid, _)| *sid == 0xFFFFFFFF),
            "sentinel 0xFFFFFFFF must not appear in fixed_map"
        );
    }

    #[test]
    fn sentinel_0x00000000_not_created_via_fixed_id_path() {
        let mut resolver = in_memory_resolver();

        let p1 = make_packet("0x00000000", "TRW-OOK", 298, 63.0);
        let p2 = make_packet("0x00000000", "TRW-OOK", 298, 63.1);
        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.flush().unwrap();

        for v in resolver.vehicles.values() {
            assert_ne!(
                v.fixed_sensor_id,
                Some(0x00000000),
                "sentinel 0x00000000 must not be stored as a fixed sensor ID"
            );
        }
        assert!(
            !resolver.fixed_map.keys().any(|(sid, _)| *sid == 0x00000000),
            "sentinel 0x00000000 must not appear in fixed_map"
        );
    }

    #[test]
    fn sentinel_different_protocols_do_not_merge() {
        let mut resolver = in_memory_resolver();

        // Two bursts with 0xFFFFFFFF from different protocols, with very
        // different pressures so the fingerprint correlator cannot merge them.
        let p1a = make_packet("0xFFFFFFFF", "EezTire", 241, 51.0);
        let p1b = make_packet("0xFFFFFFFF", "EezTire", 241, 52.0);
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        let p2a = make_packet("0xFFFFFFFF", "Hyundai-Elantra", 140, 255.0);
        let p2b = make_packet("0xFFFFFFFF", "Hyundai-Elantra", 140, 254.0);
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        // Collect all vehicle IDs — different protocols must not share a UUID.
        let eez_vids: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .map(|v| v.vehicle_id)
            .collect();
        let hyu_vids: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "Hyundai-Elantra")
            .map(|v| v.vehicle_id)
            .collect();

        for eid in &eez_vids {
            assert!(
                !hyu_vids.contains(eid),
                "EezTire and Hyundai-Elantra with sentinel ID must not share vehicle UUID"
            );
        }
    }

    #[test]
    fn valid_fixed_id_still_works() {
        let mut resolver = in_memory_resolver();

        // TRW sensor 0xFEFFFFFD — a valid fixed ID.
        for _ in 0..5 {
            let p = make_packet("0xFEFFFFFD", "TRW-OOK", 298, 63.8);
            resolver.process(&p).unwrap();
        }

        // All 5 sightings should map to the same vehicle via the fixed-ID path.
        assert!(
            resolver.fixed_map.contains_key(&(0xFEFFFFFD, 298)),
            "valid ID 0xFEFFFFFD must be in the fixed_map"
        );
        let vid = resolver.fixed_map[&(0xFEFFFFFD, 298)];
        let vehicle = &resolver.vehicles[&vid];
        assert_eq!(vehicle.sighting_count, 5);
        assert_eq!(vehicle.fixed_sensor_id, Some(0xFEFFFFFD));
    }

    #[test]
    fn eeztire_rolling_id_burst_resolves_to_single_vehicle() {
        // Regression for the 6-packet burst listed in the tracker issue:
        // EezTire (protocol 241) stable ~51.1 kPa with every packet reporting
        // a different bit-flipped sensor ID.  Prior behaviour was to create
        // one vehicle per packet; all six must now collapse to a single UUID.
        let mut resolver = in_memory_resolver();
        let bursts = [
            ("2025-06-01 12:00:00.000", "0xF7FFFFFF"),
            ("2025-06-01 12:00:30.000", "0xFFDFFFFF"),
            ("2025-06-01 12:01:00.000", "0xBFFFFFFF"),
            ("2025-06-01 12:01:30.000", "0xEFFFF5FF"),
            ("2025-06-01 12:02:00.000", "0x7FFFFF7F"),
            ("2025-06-01 12:02:30.000", "0x7FFEFFFE"),
        ];

        let mut vids = Vec::new();
        for (ts, sid) in bursts {
            let p = make_packet_at(ts, sid, "EezTire", 241, 51.1);
            let vid = resolver.process(&p).unwrap().expect("vid resolved");
            vids.push(vid);
        }

        let first = vids[0];
        for (i, v) in vids.iter().enumerate() {
            assert_eq!(
                *v, first,
                "packet {i} expected to resolve to the same vehicle as the first"
            );
        }

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(
            eez_count, 1,
            "all six EezTire packets must collapse into a single vehicle"
        );
        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .unwrap();
        assert_eq!(v.sighting_count, 6);
        assert!(v.fixed_sensor_id.is_none());
    }

    #[test]
    fn eeztire_packet_after_expiry_creates_new_vehicle() {
        // After EezTire VEHICLE_EXPIRY (8 minutes / 480 s) of silence a new
        // EezTire packet at the same pressure should resolve to a *different*
        // vehicle UUID.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            51.1,
        );
        let p2 = make_packet_at(
            "2025-06-01 12:00:10.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            51.1,
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(vid1, vid2, "packets inside the expiry window must merge");

        // > 8 minutes after last sighting, a new sighting should not merge
        // with the earlier vehicle. Last sighting was at 12:00:10.
        let p3 = make_packet_at(
            "2025-06-01 12:08:11.000",
            "0x7FFFFF7F",
            "EezTire",
            241,
            51.1,
        );
        let vid3 = resolver.process(&p3).unwrap().unwrap();
        assert_ne!(
            vid1, vid3,
            "a packet arriving after VEHICLE_EXPIRY silence must create a new vehicle"
        );
    }

    #[test]
    fn eeztire_two_sensors_at_different_pressures_do_not_merge() {
        // Two EezTire sensors with materially different pressures (51 kPa and
        // 25 kPa, both present in the real capture) must resolve to distinct
        // vehicles.
        let mut resolver = in_memory_resolver();

        let hi1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            51.1,
        );
        let lo1 = make_packet_at(
            "2025-06-01 12:00:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            25.0,
        );
        let hi2 = make_packet_at(
            "2025-06-01 12:00:02.000",
            "0x7FFEFFFE",
            "EezTire",
            241,
            51.2,
        );
        let lo2 = make_packet_at(
            "2025-06-01 12:00:03.000",
            "0xEFFFF5FF",
            "EezTire",
            241,
            24.9,
        );

        let vhi1 = resolver.process(&hi1).unwrap().unwrap();
        let vlo1 = resolver.process(&lo1).unwrap().unwrap();
        let vhi2 = resolver.process(&hi2).unwrap().unwrap();
        let vlo2 = resolver.process(&lo2).unwrap().unwrap();

        assert_ne!(vhi1, vlo1, "51 kPa and 25 kPa sensors must not merge");
        assert_eq!(
            vhi1, vhi2,
            "both 51 kPa packets must resolve to same vehicle"
        );
        assert_eq!(
            vlo1, vlo2,
            "both 25 kPa packets must resolve to same vehicle"
        );

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(eez_count, 2);
    }

    #[test]
    fn fixed_id_map_keyed_on_protocol_does_not_merge_across_decoders() {
        // Two sightings with identical sensor_id but different rtl433_id
        // values must resolve to distinct vehicle UUIDs via the fixed-ID
        // path. This is the defence-in-depth guard against cross-protocol
        // absorption — if two decoders happen to emit the same sensor_id,
        // keying the map on sensor_id alone would merge them. Keying on
        // (sensor_id, rtl433_id) must keep them separate.
        let mut resolver = in_memory_resolver();

        // Pick a sensor_id that passes is_valid_sensor_id (plenty of bits
        // cleared, not near the all-ones sentinel).
        let shared_id = "0x1A2B3C4D";

        let p_trw = make_packet(shared_id, "TRW-OOK", 298, 63.8);
        let p_hyu = make_packet(shared_id, "Hyundai-Elantra", 140, 255.0);

        let vid_trw = resolver.process(&p_trw).unwrap().expect("vid");
        let vid_hyu = resolver.process(&p_hyu).unwrap().expect("vid");

        assert_ne!(
            vid_trw, vid_hyu,
            "same sensor_id from two different decoders must produce distinct vehicle UUIDs"
        );

        // Both entries must be present in the fixed_map under their
        // (sensor_id, rtl433_id) keys.
        assert!(resolver.fixed_map.contains_key(&(0x1A2B3C4D, 298)));
        assert!(resolver.fixed_map.contains_key(&(0x1A2B3C4D, 140)));
        assert_eq!(resolver.vehicles.len(), 2);

        // A subsequent TRW packet with the shared ID must rejoin the TRW
        // vehicle, not flip to the Hyundai one.
        let p_trw2 = make_packet(shared_id, "TRW-OOK", 298, 63.9);
        let vid_trw2 = resolver.process(&p_trw2).unwrap().expect("vid");
        assert_eq!(vid_trw, vid_trw2);
    }

    #[test]
    fn ave_half_range_frame_does_not_split_vehicle() {
        // Regression for the AVE-TPMS dual-range encoding issue: a single
        // physical sensor occasionally emits a frame whose pressure field
        // decodes to roughly half the real pressure (~190 kPa vs ~382 kPa).
        // The sniffer decoder flags these as `pressure_kpa_reliable = false`;
        // the tracker must drop them so they do not create a distinct vehicle
        // or drift the pressure signature of the real vehicle.
        let mut resolver = in_memory_resolver();

        // Three AVE packets with rolling sensor IDs, tight timestamps so they
        // all fall inside one burst window (BURST_GAP_MS = 200ms).
        let p1 = make_packet_at(
            "2025-06-01 19:17:57.000",
            "0x12345678",
            "AVE-TPMS",
            208,
            382.5,
        );
        let p2 = make_packet_at_reliable(
            "2025-06-01 19:17:57.050",
            "0x87654321",
            "AVE-TPMS",
            208,
            190.5,
            false, // half-range artifact, flagged by the decoder
        );
        let p3 = make_packet_at(
            "2025-06-01 19:17:57.100",
            "0xDEADBEEF",
            "AVE-TPMS",
            208,
            382.5,
        );

        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.process(&p3).unwrap();
        resolver.flush().unwrap();

        // Exactly one AVE vehicle, whose signature has not been contaminated
        // by the 190.5 kPa half-range reading.
        let ave: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave.len(),
            1,
            "the half-range 190.5 kPa frame must not spawn a second vehicle"
        );
        let sig0 = ave[0].pressure_signature[0];
        assert!(
            (sig0 - 382.5).abs() < 1.0,
            "pressure signature drifted from 382.5 kPa to {sig0}"
        );
    }

    #[test]
    fn ave_sequence_382_190_produces_one_vehicle_and_stable_average() {
        // Acceptance-criteria test: the sequence [382.5, 190.5, 382.5, 190.5, 382.5]
        // must produce exactly one vehicle UUID and the final stored pressure
        // average must be within 5.0 kPa of 382.5.  The 190.5 kPa packets are
        // AVE half-range artifacts (pressure_kpa_reliable = false) and must not
        // corrupt the running average.
        let mut resolver = in_memory_resolver();

        let pressures: &[(f32, bool)] = &[
            (382.5, true),
            (190.5, false),
            (382.5, true),
            (190.5, false),
            (382.5, true),
        ];

        // Use timestamps spaced > BURST_GAP_MS apart so each reliable packet
        // forms its own burst and resolves immediately.  Unreliable packets are
        // dropped before entering the burst accumulator.
        let base_ts = "2025-06-01 21:00:0";

        for (i, &(kpa, reliable)) in pressures.iter().enumerate() {
            // Construct two packets per reliable reading so each burst has the
            // minimum 2 slots required by resolve_burst.
            if reliable {
                let ts1 = format!("{}{}.000", base_ts, i);
                let ts2 = format!("{}{}.100", base_ts, i);
                let p1 = make_packet_at_reliable(
                    &ts1,
                    &format!("0x{:08X}", 0xAA000000 + i as u32 * 2),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                let p2 = make_packet_at_reliable(
                    &ts2,
                    &format!("0x{:08X}", 0xAA000001 + i as u32 * 2),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                resolver.process(&p1).unwrap();
                resolver.process(&p2).unwrap();
                resolver.flush().unwrap();
            } else {
                let ts = format!("{}{}.000", base_ts, i);
                let p = make_packet_at_reliable(
                    &ts,
                    &format!("0x{:08X}", 0xBB000000 + i as u32),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                resolver.process(&p).unwrap();
            }
        }

        // Collect all AVE vehicles.
        let ave: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave.len(),
            1,
            "AVE sequence [382.5, 190.5, 382.5, 190.5, 382.5] must produce exactly one vehicle"
        );

        let sig0 = ave[0].pressure_signature[0];
        assert!(
            (sig0 - 382.5).abs() < 5.0,
            "final stored pressure average ({sig0}) must be within 5.0 kPa of 382.5"
        );
    }

    #[test]
    fn per_protocol_vehicle_expiry() {
        // Documented per-protocol expiry values.
        assert_eq!(vehicle_expiry_for(208), Duration::seconds(600)); // AVE-TPMS
        assert_eq!(vehicle_expiry_for(241), Duration::seconds(480)); // EezTire
        assert_eq!(vehicle_expiry_for(298), Duration::seconds(480)); // TRW-OOK
        assert_eq!(vehicle_expiry_for(0), Duration::seconds(300));   // default
    }

    #[test]
    fn ave_burst_after_expiry_creates_new_vehicle() {
        // AVE-TPMS (protocol 208) has a 600 s expiry.  Two bursts of the same
        // pressure separated by more than 600 s must resolve to *distinct*
        // vehicle UUIDs.  This verifies that the expiry filter in resolve_burst
        // is actually evaluated (not just the function return value).
        let mut resolver = in_memory_resolver();

        // First burst: two packets within the 200 ms burst window.
        let p1a = make_packet_at(
            "2025-06-01 10:00:00.000",
            "0x11111111",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p1b = make_packet_at(
            "2025-06-01 10:00:00.100",
            "0x22222222",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        let vid1 = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("first burst must create a vehicle");

        // Second burst: exactly the same pressure, but more than 600 s later.
        // resolve_burst must reject the existing vehicle as stale and create a
        // new UUID instead of re-using vid1.
        let p2a = make_packet_at(
            "2025-06-01 10:10:01.000",
            "0x33333333",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p2b = make_packet_at(
            "2025-06-01 10:10:01.100",
            "0x44444444",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        let ave_vehicles: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave_vehicles.len(),
            2,
            "burst after AVE expiry (600 s) must create a second vehicle"
        );
        let vid2 = ave_vehicles
            .iter()
            .find(|v| v.vehicle_id != vid1)
            .map(|v| v.vehicle_id)
            .expect("second vehicle must exist");
        assert_ne!(
            vid1, vid2,
            "the two bursts separated by >600 s must not share a vehicle UUID"
        );
    }

    #[test]
    fn ave_burst_within_expiry_reuses_vehicle() {
        // Complement of the above: two AVE bursts separated by less than 600 s
        // at matching pressure must still collapse into a single vehicle UUID.
        let mut resolver = in_memory_resolver();

        let p1a = make_packet_at(
            "2025-06-01 10:00:00.000",
            "0xAAAAAAAA",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p1b = make_packet_at(
            "2025-06-01 10:00:00.100",
            "0xBBBBBBBB",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        // Second burst 599 s later — still within the 600 s AVE window.
        let p2a = make_packet_at(
            "2025-06-01 10:09:59.000",
            "0xCCCCCCCC",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p2b = make_packet_at(
            "2025-06-01 10:09:59.100",
            "0xDDDDDDDD",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        let ave_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .count();
        assert_eq!(
            ave_count, 1,
            "two AVE bursts within the 600 s window must collapse to a single vehicle"
        );
    }

    #[test]
    fn is_valid_sensor_id_unit() {
        // Exact sentinels.
        assert!(!is_valid_sensor_id(0xFFFFFFFF));
        assert!(!is_valid_sensor_id(0x00000000));

        // Near-sentinels with a single bit cleared (decode artifacts).
        assert!(!is_valid_sensor_id(0xFFFFFFFD));
        assert!(!is_valid_sensor_id(0xFFFFFFF7));
        assert!(!is_valid_sensor_id(0xFFFFFFFB));
        assert!(!is_valid_sensor_id(0xFFFFFFBF));

        // Confirmed real TRW sensor with exactly 2 bits cleared — must not
        // regress.
        assert!(is_valid_sensor_id(0xFEFFFFFD));

        // Arbitrary real IDs.
        assert!(is_valid_sensor_id(0x1A2B3C4D));
        assert!(is_valid_sensor_id(1));
    }

    /// Popcount boundary coverage required by the acceptance criteria:
    /// IDs with 0, 1, 2, 3, and 4 bits cleared must return the correct
    /// valid/invalid result. The boundary sits between 1 and 2 bits cleared.
    #[test]
    fn is_valid_sensor_id_popcount_boundary() {
        // 0 bits cleared → invalid.
        assert_eq!(0xFFFFFFFFu32.count_zeros(), 0);
        assert!(!is_valid_sensor_id(0xFFFFFFFF));

        // 1 bit cleared → invalid.
        assert_eq!(0xFFFFFFFEu32.count_zeros(), 1);
        assert!(!is_valid_sensor_id(0xFFFFFFFE));

        // 2 bits cleared → valid.
        assert_eq!(0xFFFFFFFCu32.count_zeros(), 2);
        assert!(is_valid_sensor_id(0xFFFFFFFC));

        // 3 bits cleared → valid.
        assert_eq!(0xFFFFFFF8u32.count_zeros(), 3);
        assert!(is_valid_sensor_id(0xFFFFFFF8));

        // 4 bits cleared → valid.
        assert_eq!(0xFFFFFFF0u32.count_zeros(), 4);
        assert!(is_valid_sensor_id(0xFFFFFFF0));
    }

    #[test]
    fn sentinel_trw_ook_never_resolves_to_ave_vehicle() {
        // A sentinel-rejected TRW-OOK packet (sensor_id = 0xFFFFFFFF) must
        // never be absorbed into an AVE-TPMS vehicle, regardless of pressure.
        // This is the unit test for AC #3: sentinel-rejected packet from
        // protocol A cannot match an active vehicle from protocol B.
        let mut resolver = in_memory_resolver();

        // Establish an AVE-TPMS vehicle via a rolling-ID burst at 63.8 kPa.
        let ave1 = make_packet_at(
            "2025-06-01 21:58:00.000",
            "0xA1A1A1A1",
            "AVE-TPMS",
            208,
            63.8,
        );
        let ave2 = make_packet_at(
            "2025-06-01 21:58:00.100",
            "0xB2B2B2B2",
            "AVE-TPMS",
            208,
            63.8,
        );
        resolver.process(&ave1).unwrap();
        resolver.process(&ave2).unwrap();
        resolver.flush().unwrap();

        let ave_vid = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("AVE vehicle must exist");

        // Send sentinel TRW-OOK packets at exactly the same pressure.
        // Because the sensor ID is invalid, these fall through to the
        // rolling-ID path.  With the rtl433_id filter in place, they must
        // never match the AVE vehicle.
        let trw_sentinel = make_packet_at(
            "2025-06-01 21:58:43.000",
            "0xFFFFFFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let result = resolver.process(&trw_sentinel).unwrap();

        // If it resolved to something, it must not be the AVE vehicle.
        if let Some(vid) = result {
            assert_ne!(
                vid, ave_vid,
                "sentinel TRW-OOK packet must not be absorbed into AVE-TPMS vehicle"
            );
        }

        // Also verify no AVE vehicle has been touched by the TRW packet.
        let ave_vehicle = &resolver.vehicles[&ave_vid];
        assert_eq!(
            ave_vehicle.protocol, "AVE-TPMS",
            "AVE vehicle protocol must remain AVE-TPMS"
        );
        assert_eq!(
            ave_vehicle.rtl433_id, 208,
            "AVE vehicle rtl433_id must remain 208"
        );
    }

    #[test]
    fn three_packet_trw_burst_regression() {
        // Regression test for the three-packet 21:58:42–21:58:45 burst from
        // the issue: all three TRW-OOK packets must resolve to the same
        // TRW-OOK vehicle, even when an AVE-TPMS vehicle exists at the same
        // pressure.  The sentinel packet (0xFFFFFFFF) must never resolve to
        // the AVE vehicle UUID.
        let mut resolver = in_memory_resolver();

        // Pre-seed an AVE-TPMS vehicle at 63.8 kPa.
        let ave1 = make_packet_at(
            "2025-06-01 21:57:50.000",
            "0xA1A1A1A1",
            "AVE-TPMS",
            208,
            63.8,
        );
        let ave2 = make_packet_at(
            "2025-06-01 21:57:50.100",
            "0xB2B2B2B2",
            "AVE-TPMS",
            208,
            63.8,
        );
        resolver.process(&ave1).unwrap();
        resolver.process(&ave2).unwrap();
        resolver.flush().unwrap();

        let ave_vid = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("AVE vehicle must exist");

        // Packet 1: TRW-OOK, valid fixed ID 0xFEFFFFFD.
        let p1 = make_packet_at(
            "2025-06-01 21:58:42.000",
            "0xFEFFFFFD",
            "TRW-OOK",
            298,
            63.8,
        );
        let vid1 = resolver.process(&p1).unwrap().expect("packet 1 must resolve");
        assert_ne!(vid1, ave_vid, "packet 1 must not resolve to AVE vehicle");

        // Packet 2: TRW-OOK, sentinel 0xFFFFFFFF.
        let p2 = make_packet_at(
            "2025-06-01 21:58:43.000",
            "0xFFFFFFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let result2 = resolver.process(&p2).unwrap();
        if let Some(vid2) = result2 {
            assert_ne!(
                vid2, ave_vid,
                "sentinel packet must not resolve to AVE vehicle"
            );
        }

        // Packet 3: TRW-OOK, valid fixed ID 0xFEFFFFFD (same as packet 1).
        let p3 = make_packet_at(
            "2025-06-01 21:58:45.000",
            "0xFEFFFFFD",
            "TRW-OOK",
            298,
            63.8,
        );
        let vid3 = resolver.process(&p3).unwrap().expect("packet 3 must resolve");
        assert_ne!(vid3, ave_vid, "packet 3 must not resolve to AVE vehicle");

        // Packets 1 and 3 share the same valid fixed ID — they must resolve
        // to the same TRW-OOK vehicle UUID.
        assert_eq!(
            vid1, vid3,
            "packets 1 and 3 (same fixed sensor ID) must share a vehicle UUID"
        );

        // Verify the TRW vehicle is actually a TRW-OOK vehicle.
        let trw_vehicle = &resolver.vehicles[&vid1];
        assert_eq!(trw_vehicle.protocol, "TRW-OOK");
        assert_eq!(trw_vehicle.rtl433_id, 298);
    }

    #[test]
    fn eeztire_low_battery_survives_9_minute_gap() {
        // EezTire (241) base expiry is 480 s.  With battery_ok=false, the
        // effective expiry extends by 300 s to 780 s (13 min).  A 9-minute
        // gap (540 s) must NOT create a new vehicle.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_battery(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            51.1,
            false, // low battery
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();

        // 9 minutes later — within the extended 780 s window.
        let p2 = make_packet_at_battery(
            "2025-06-01 12:09:00.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            51.1,
            false,
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(
            vid1, vid2,
            "EezTire with battery_ok=false must survive a 9-minute gap"
        );
    }

    #[test]
    fn eeztire_good_battery_expires_after_8_minutes() {
        // EezTire (241) base expiry is 480 s.  With battery_ok=true, there is
        // no extension.  A gap of 481 s must create a new vehicle.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_battery(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            51.1,
            true, // good battery
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();

        // 8 minutes + 1 second later — just past the 480 s window.
        let p2 = make_packet_at_battery(
            "2025-06-01 12:08:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            51.1,
            true,
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_ne!(
            vid1, vid2,
            "EezTire with battery_ok=true must expire after 8 minutes"
        );
    }

    #[test]
    fn effective_expiry_battery_extension() {
        // Verify the effective_expiry function applies the 300 s battery
        // extension correctly for various protocols.
        let make_track = |rtl433_id: u16, battery_ok: bool| -> VehicleTrack {
            VehicleTrack {
                vehicle_id: Uuid::new_v4(),
                first_seen: Utc::now(),
                last_seen: Utc::now(),
                sighting_count: 1,
                protocol: String::new(),
                rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: [0.0; 4],
                make_model_hint: None,
                battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
            }
        };

        // EezTire, good battery: 480 s
        assert_eq!(
            effective_expiry(&make_track(241, true)),
            Duration::seconds(480)
        );
        // EezTire, low battery: 480 + 300 = 780 s
        assert_eq!(
            effective_expiry(&make_track(241, false)),
            Duration::seconds(780)
        );
        // AVE-TPMS, good battery: 600 s
        assert_eq!(
            effective_expiry(&make_track(208, true)),
            Duration::seconds(600)
        );
        // AVE-TPMS, low battery: 600 + 300 = 900 s
        assert_eq!(
            effective_expiry(&make_track(208, false)),
            Duration::seconds(900)
        );
        // Default protocol, good battery: 300 s
        assert_eq!(
            effective_expiry(&make_track(0, true)),
            Duration::seconds(300)
        );
        // Default protocol, low battery: 300 + 300 = 600 s
        assert_eq!(
            effective_expiry(&make_track(0, false)),
            Duration::seconds(600)
        );
    }

    // -----------------------------------------------------------------------
    // TX-interval fingerprint tests
    // -----------------------------------------------------------------------

    #[test]
    fn compute_median_basic() {
        use crate::compute_median;

        let mut buf = VecDeque::new();
        // Not enough samples → None.
        buf.push_back(1000);
        buf.push_back(2000);
        assert_eq!(compute_median(&buf), None);

        // 3 samples → median is the middle value.
        buf.push_back(3000);
        assert_eq!(compute_median(&buf), Some(2000));

        // 4 samples → average of the two middle values.
        buf.push_back(4000);
        assert_eq!(compute_median(&buf), Some(2500));

        // 5 samples (odd).
        buf.push_back(5000);
        assert_eq!(compute_median(&buf), Some(3000));
    }

    #[test]
    fn tx_interval_accumulated_on_fingerprint_path() {
        // Feed EezTire (fingerprint path) packets at 30 s intervals and
        // verify the vehicle accumulates intervals and computes a median.
        let mut resolver = in_memory_resolver();

        let timestamps = [
            "2025-06-01 12:00:00.000",
            "2025-06-01 12:00:30.000",
            "2025-06-01 12:01:00.000",
            "2025-06-01 12:01:30.000",
            "2025-06-01 12:02:00.000",
        ];

        for ts in &timestamps {
            let p = make_packet_at(ts, "0xF7FFFFFF", "EezTire", 241, 51.1);
            resolver.process(&p).unwrap();
        }

        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");

        // 5 packets → 4 intervals of 30_000 ms each.
        assert_eq!(v.tx_intervals_ms.len(), 4);
        assert_eq!(v.tx_interval_median_ms, Some(30_000));
    }

    #[test]
    fn two_ave_same_pressure_different_intervals_separate() {
        // Two simulated AVE-TPMS vehicles at identical pressure (382.5 kPa)
        // but different TX intervals (45 s vs 71 s) must resolve to separate
        // vehicle UUIDs after enough bursts to establish interval medians.
        //
        // Process bursts in chronological order, interleaving the two vehicles.
        let mut resolver = in_memory_resolver();

        // Chronological sequence of bursts (each burst = 2 packets 100 ms apart):
        // A@0s, B@5s, A@45s, B@76s, A@90s, B@147s, A@135s, B@218s
        let bursts: &[(&str, &str, &str)] = &[
            // (ts1, ts2, vehicle_label)
            ("2025-06-01 10:00:00.000", "2025-06-01 10:00:00.100", "A"),
            ("2025-06-01 10:00:05.000", "2025-06-01 10:00:05.100", "B"),
            ("2025-06-01 10:00:45.000", "2025-06-01 10:00:45.100", "A"),
            ("2025-06-01 10:01:16.000", "2025-06-01 10:01:16.100", "B"),
            ("2025-06-01 10:01:30.000", "2025-06-01 10:01:30.100", "A"),
            ("2025-06-01 10:02:27.000", "2025-06-01 10:02:27.100", "B"),
            ("2025-06-01 10:02:15.000", "2025-06-01 10:02:15.100", "A"),
            ("2025-06-01 10:03:38.000", "2025-06-01 10:03:38.100", "B"),
        ];

        // Sort by first timestamp to ensure chronological processing.
        let mut sorted: Vec<_> = bursts.to_vec();
        sorted.sort_by_key(|(ts1, _, _)| ts1.to_string());

        for (ts1, ts2, _label) in &sorted {
            let p1 = make_packet_at(ts1, "0xA1000001", "AVE-TPMS", 208, 382.5);
            let p2 = make_packet_at(ts2, "0xA1000002", "AVE-TPMS", 208, 382.5);
            resolver.process(&p1).unwrap();
            resolver.process(&p2).unwrap();
            resolver.flush().unwrap();
        }

        let ave_vehicles: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert!(
            ave_vehicles.len() >= 2,
            "two AVE-TPMS sensors at identical pressure but different TX intervals \
             must resolve to separate vehicle UUIDs (got {} vehicles)",
            ave_vehicles.len()
        );
    }

    #[test]
    fn single_vehicle_noisy_interval_does_not_split() {
        // A single EezTire vehicle with jittery interval (30 s ± 5 s) must
        // remain a single vehicle UUID.
        let mut resolver = in_memory_resolver();

        let timestamps = [
            "2025-06-01 12:00:00.000",
            "2025-06-01 12:00:25.000", // 25 s
            "2025-06-01 12:00:55.000", // 30 s
            "2025-06-01 12:01:30.000", // 35 s
            "2025-06-01 12:01:58.000", // 28 s
            "2025-06-01 12:02:31.000", // 33 s
            "2025-06-01 12:02:56.000", // 25 s
            "2025-06-01 12:03:26.000", // 30 s
        ];

        for ts in &timestamps {
            let p = make_packet_at(ts, "0xF7FFFFFF", "EezTire", 241, 51.1);
            resolver.process(&p).unwrap();
        }

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(
            eez_count, 1,
            "a single vehicle with ±5 s jitter must not be split into multiple UUIDs"
        );
    }

    #[test]
    fn interval_check_skipped_when_insufficient_samples() {
        // When a vehicle has fewer than TX_INTERVAL_MIN_SAMPLES intervals, the
        // interval check must be skipped and only pressure matching is used.
        // Two packets 1 s apart at the same pressure must merge even though
        // the vehicle has only 1 interval sample (below MIN_SAMPLES).
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            51.1,
        );
        let p2 = make_packet_at(
            "2025-06-01 12:00:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            51.1,
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(
            vid1, vid2,
            "with < MIN_SAMPLES intervals, pressure match alone must be sufficient"
        );

        // Vehicle should have 1 interval sample and no usable median yet.
        let v = &resolver.vehicles[&vid1];
        assert_eq!(v.tx_intervals_ms.len(), 1);
        assert_eq!(v.tx_interval_median_ms, None);
    }

    #[test]
    fn tx_interval_ring_buffer_capped() {
        // Verify the ring buffer never exceeds TX_INTERVAL_WINDOW.
        let mut resolver = in_memory_resolver();

        // Send 12 EezTire packets 30 s apart → 11 intervals.
        for i in 0..12 {
            let ts = format!("2025-06-01 12:{:02}:{:02}.000", i / 2, (i % 2) * 30);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 51.1);
            resolver.process(&p).unwrap();
        }

        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .unwrap();
        assert!(
            v.tx_intervals_ms.len() <= crate::TX_INTERVAL_WINDOW,
            "ring buffer must not exceed TX_INTERVAL_WINDOW ({}), got {}",
            crate::TX_INTERVAL_WINDOW,
            v.tx_intervals_ms.len()
        );
    }
}
