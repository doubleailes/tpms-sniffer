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

/// Sensor IDs that are decode artifacts (not real identifiers).
/// Multiple protocols emit these when the sensor ID field cannot be reliably
/// extracted.  They must never be used for fixed-ID vehicle matching.
const SENTINEL_IDS: &[u32] = &[0xFFFFFFFF, 0x00000000];

fn is_valid_sensor_id(id: u32) -> bool {
    !SENTINEL_IDS.contains(&id)
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
fn vehicle_expiry() -> Duration {
    Duration::seconds(300) // 5 minutes
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
    /// Stable sensor_id → vehicle_id for fixed-ID protocols.
    fixed_map: HashMap<u32, Uuid>,
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
                self.fixed_map.insert(sid, vehicle.vehicle_id);
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

    fn process_fixed(&mut self, sighting: Sighting, rtl433_id: u16) -> Result<Option<Uuid>> {
        let sensor_id = sighting.sensor_id;

        // Look up or create the vehicle.
        let vehicle_id = if let Some(&vid) = self.fixed_map.get(&sensor_id) {
            vid
        } else {
            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: sighting.ts,
                last_seen: sighting.ts,
                sighting_count: 0,
                protocol: sighting.protocol.clone(),
                fixed_sensor_id: Some(sensor_id),
                pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
            };
            self.fixed_map.insert(sensor_id, vid);
            self.vehicles.insert(vid, vehicle);
            vid
        };

        // Update in-memory state.
        let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
        vehicle.last_seen = sighting.ts;
        vehicle.sighting_count += 1;
        ema_update(&mut vehicle.pressure_signature[0], sighting.pressure_kpa);

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
    fn process_fingerprint(&mut self, sighting: Sighting) -> Result<Option<Uuid>> {
        let now = sighting.ts;
        let pressure = sighting.pressure_kpa;
        let protocol = sighting.protocol.clone();
        let rtl433_id = sighting.rtl433_id;
        let expiry = vehicle_expiry();

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
        ema_update(&mut vehicle.pressure_signature[0], pressure);

        // Persist.
        self.db.upsert_vehicle(vehicle)?;
        self.db.insert_sighting(&sighting, vehicle_id)?;

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Rolling-ID path (burst accumulator)
    // -----------------------------------------------------------------------

    fn process_rolling(&mut self, sighting: Sighting) -> Result<Option<Uuid>> {
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
        // enough.  We copy the Uuid (it's Copy) so we drop the shared borrow
        // before taking the mutable one below.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.fixed_sensor_id.is_none() && v.protocol == burst.protocol)
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

    fn make_packet(sensor_id: &str, protocol: &str, rtl433_id: u16, pressure_kpa: f32) -> TpmsPacket {
        make_packet_at("2025-06-01 12:00:00.000", sensor_id, protocol, rtl433_id, pressure_kpa)
    }

    fn make_packet_at(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
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
            !resolver.fixed_map.contains_key(&0xFFFFFFFF),
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
            !resolver.fixed_map.contains_key(&0x00000000),
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
            resolver.fixed_map.contains_key(&0xFEFFFFFD),
            "valid ID 0xFEFFFFFD must be in the fixed_map"
        );
        let vid = resolver.fixed_map[&0xFEFFFFFD];
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
        let v = resolver.vehicles.values().find(|v| v.protocol == "EezTire").unwrap();
        assert_eq!(v.sighting_count, 6);
        assert!(v.fixed_sensor_id.is_none());
    }

    #[test]
    fn eeztire_packet_after_expiry_creates_new_vehicle() {
        // After VEHICLE_EXPIRY (5 minutes) of silence a new EezTire packet at
        // the same pressure should resolve to a *different* vehicle UUID.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at("2025-06-01 12:00:00.000", "0xF7FFFFFF", "EezTire", 241, 51.1);
        let p2 = make_packet_at("2025-06-01 12:00:10.000", "0xBFFFFFFF", "EezTire", 241, 51.1);
        let vid1 = resolver.process(&p1).unwrap().unwrap();
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(vid1, vid2, "packets inside the expiry window must merge");

        // > 5 minutes later, a new sighting should not merge with the earlier vehicle.
        let p3 = make_packet_at("2025-06-01 12:05:11.000", "0x7FFFFF7F", "EezTire", 241, 51.1);
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

        let hi1 = make_packet_at("2025-06-01 12:00:00.000", "0xF7FFFFFF", "EezTire", 241, 51.1);
        let lo1 = make_packet_at("2025-06-01 12:00:01.000", "0xBFFFFFFF", "EezTire", 241, 25.0);
        let hi2 = make_packet_at("2025-06-01 12:00:02.000", "0x7FFEFFFE", "EezTire", 241, 51.2);
        let lo2 = make_packet_at("2025-06-01 12:00:03.000", "0xEFFFF5FF", "EezTire", 241, 24.9);

        let vhi1 = resolver.process(&hi1).unwrap().unwrap();
        let vlo1 = resolver.process(&lo1).unwrap().unwrap();
        let vhi2 = resolver.process(&hi2).unwrap().unwrap();
        let vlo2 = resolver.process(&lo2).unwrap().unwrap();

        assert_ne!(vhi1, vlo1, "51 kPa and 25 kPa sensors must not merge");
        assert_eq!(vhi1, vhi2, "both 51 kPa packets must resolve to same vehicle");
        assert_eq!(vlo1, vlo2, "both 25 kPa packets must resolve to same vehicle");

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(eez_count, 2);
    }

    #[test]
    fn is_valid_sensor_id_unit() {
        assert!(!is_valid_sensor_id(0xFFFFFFFF));
        assert!(!is_valid_sensor_id(0x00000000));
        assert!(is_valid_sensor_id(0xFEFFFFFD));
        assert!(is_valid_sensor_id(0x1A2B3C4D));
        assert!(is_valid_sensor_id(1));
    }
}
