use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::db::Database;
use crate::{Sighting, TpmsPacket, VehicleTrack, make_model_hint};

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
/// need a longer expiry than the default.  AVE-TPMS (rtl_433 id 208) is a
/// known low-transmission-rate protocol — aftermarket AVE sensors idle at
/// 2–8 minute intervals when parked, so a 5-minute expiry leaks the track
/// and spawns a fresh vehicle UUID for every other sighting. See
/// `crates/tracker/README.md` for the documented per-protocol values.
fn vehicle_expiry_for(rtl433_id: u16) -> Duration {
    match rtl433_id {
        208 => Duration::seconds(600), // AVE-TPMS: low TX rate when parked
        _ => Duration::seconds(300),   // default: 5 minutes
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
    db: Database,
}

impl Resolver {
    /// Open (or create) the database and restore in-memory state from it.
    pub fn new(db: Database) -> Result<Self> {
        let mut r = Self {
            fixed_map: HashMap::new(),
            vehicles: HashMap::new(),
            pending_burst: None,
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

        let sighting = Sighting {
            ts,
            protocol: packet.protocol.clone(),
            rtl433_id: packet.rtl433_id,
            sensor_id,
            pressure_kpa: packet.pressure_kpa,
            temp_c,
            alarm: packet.alarm.unwrap_or(false),
            battery_ok: packet.battery_ok.unwrap_or(true),
        };

        let pressure_reliable = packet.pressure_kpa_reliable;

        if BIT_FLIP_ID_PROTOCOLS.contains(&packet.rtl433_id) {
            self.process_fingerprint(sighting, pressure_reliable)
        } else if ROLLING_ID_PROTOCOLS.contains(&packet.rtl433_id) || !is_valid_sensor_id(sensor_id)
        {
            self.process_rolling(sighting, pressure_reliable)
        } else {
            self.process_fixed(sighting, packet.rtl433_id, pressure_reliable)
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
        pressure_reliable: bool,
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
            };
            self.fixed_map.insert(key, vid);
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        vehicle.last_seen = sighting.ts;
        vehicle.sighting_count += 1;
        if pressure_reliable {
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
        pressure_reliable: bool,
    ) -> Result<Option<Uuid>> {
        let now = sighting.ts;
        let pressure = sighting.pressure_kpa;
        let protocol = sighting.protocol.clone();
        let rtl433_id = sighting.rtl433_id;
        let expiry = vehicle_expiry_for(rtl433_id);

        // Find an active vehicle of the same protocol whose pressure
        // signature is within tolerance.  Copy the Uuid so we drop the shared
        // borrow before taking a mutable one below.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.protocol == protocol && v.fixed_sensor_id.is_none())
            .filter(|v| now.signed_duration_since(v.last_seen) < expiry)
            .find(|v| (v.pressure_signature[0] - pressure).abs() <= PRESSURE_MATCH_TOLERANCE_KPA)
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
            };
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        vehicle.last_seen = now;
        vehicle.sighting_count += 1;
        if pressure_reliable {
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
        pressure_reliable: bool,
    ) -> Result<Option<Uuid>> {
        // Unreliable readings (e.g. AVE half-range low-pressure frames) would
        // poison the pressure fingerprint if fed through the burst
        // accumulator.  Drop them entirely: do not extend the pending burst,
        // do not close it, do not start a new one.  The sighting is lost
        // because it doesn't carry a trustworthy pressure, but the track for
        // the underlying vehicle keeps its last known good signature and is
        // still matched when the next normal packet arrives.
        if !pressure_reliable {
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
        let expiry = vehicle_expiry_for(burst.rtl433_id);

        // Find an existing rolling-ID vehicle whose pressure signature is close
        // enough and that was seen recently enough to still be active.  We copy
        // the Uuid (it's Copy) so we drop the shared borrow before taking the
        // mutable one below.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.fixed_sensor_id.is_none() && v.protocol == burst.protocol)
            .filter(|v| now.signed_duration_since(v.last_seen) < expiry)
            .find(|v| l1_per_wheel(&v.pressure_signature, &sig) < PRESSURE_MATCH_TOLERANCE_KPA)
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
            };
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
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
        // After VEHICLE_EXPIRY (5 minutes) of silence a new EezTire packet at
        // the same pressure should resolve to a *different* vehicle UUID.
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

        // > 5 minutes later, a new sighting should not merge with the earlier vehicle.
        let p3 = make_packet_at(
            "2025-06-01 12:05:11.000",
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
    fn ave_has_longer_vehicle_expiry_than_default() {
        // Documented per-protocol expiry: AVE-TPMS (208) uses 10 minutes,
        // everything else (e.g. EezTire 241) keeps the 5-minute default.
        assert_eq!(vehicle_expiry_for(208), Duration::seconds(600));
        assert_eq!(vehicle_expiry_for(241), Duration::seconds(300));
        assert_eq!(vehicle_expiry_for(0), Duration::seconds(300));
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
}
