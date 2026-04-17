use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use flate2::read::GzDecoder;
use uuid::Uuid;

use crate::{MAX_PLAUSIBLE_PRESSURE_KPA, MIN_PLAUSIBLE_PRESSURE_KPA, TpmsPacket};
use crate::db::Database;
use crate::resolver::Resolver;

/// Summary produced after replaying a fixture file through the tracker.
#[derive(Debug)]
pub struct ReplayResult {
    pub path: String,
    pub packet_count: u32,
    pub vehicles: HashMap<Uuid, VehicleReplayInfo>,
}

/// Per-vehicle information collected during replay.
#[derive(Debug, Clone)]
pub struct VehicleReplayInfo {
    pub vehicle_id: Uuid,
    pub car_id: Option<Uuid>,
    pub rtl433_ids: HashSet<u16>,
    pub pressures_kpa: Vec<f32>,
    pub sighting_count: u32,
}

impl ReplayResult {
    /// Count of distinct car groups (vehicles that have a car_id assigned).
    pub fn car_count(&self) -> usize {
        let car_ids: HashSet<Uuid> = self.vehicles.values().filter_map(|v| v.car_id).collect();
        car_ids.len()
    }

    /// Count of vehicles that have been grouped into a car.
    pub fn grouped_vehicle_count(&self) -> usize {
        self.vehicles
            .values()
            .filter(|v| v.car_id.is_some())
            .count()
    }

    /// Count of vehicles still pending grouping.
    pub fn pending_vehicle_count(&self) -> usize {
        self.vehicles
            .values()
            .filter(|v| v.car_id.is_none())
            .count()
    }
}

/// Replay a JSON-L fixture file (or `.jsonl.gz`) through the tracker.
///
/// Timestamps come from the JSON fields, not wall-clock time.  The tracker
/// uses captured time for all time-dependent calculations.
pub fn replay(path: &Path, confidence: u8) -> Result<ReplayResult> {
    let db = Database::open(":memory:")?;
    let mut resolver = Resolver::new(db)?;

    let file = File::open(path)?;
    let reader: Box<dyn BufRead> = if path.to_string_lossy().ends_with(".gz") {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut packet_count: u32 = 0;

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || !trimmed.starts_with('{') {
            continue;
        }

        let packet: TpmsPacket = match serde_json::from_str(trimmed) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("warn: skipping malformed JSON: {e}");
                continue;
            }
        };

        if packet.confidence < confidence {
            continue;
        }

        packet_count += 1;

        if let Err(e) = resolver.process(&packet) {
            eprintln!("error: {e}");
        }
    }

    resolver.flush()?;

    // Collect vehicle info from the resolver.
    let mut vehicles = HashMap::new();
    for (vid, track) in resolver.vehicles() {
        vehicles.insert(
            *vid,
            VehicleReplayInfo {
                vehicle_id: *vid,
                car_id: track.car_id,
                rtl433_ids: {
                    let mut s = HashSet::new();
                    s.insert(track.rtl433_id);
                    s
                },
                pressures_kpa: track
                    .pressure_signature
                    .iter()
                    .copied()
                    .filter(|&p| p > 0.0)
                    .collect(),
                sighting_count: track.sighting_count,
            },
        );
    }

    Ok(ReplayResult {
        path: path.display().to_string(),
        packet_count,
        vehicles,
    })
}

// ---------------------------------------------------------------------------
// Consistency assertions
// ---------------------------------------------------------------------------

/// A consistency error found during replay.
#[derive(Debug)]
pub enum ConsistencyError {
    CrossProtocolVehicle {
        vehicle_id: Uuid,
        protocols: Vec<u16>,
    },
    UnstableCarId {
        vehicle_id: Uuid,
        distinct_car_ids: usize,
    },
    ImplausiblePressure {
        vehicle_id: Uuid,
        pressure_kpa: f32,
    },
    PressureVarianceTooHigh {
        vehicle_id: Uuid,
        max_delta_kpa: f32,
    },
}

impl fmt::Display for ConsistencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsistencyError::CrossProtocolVehicle {
                vehicle_id,
                protocols,
            } => write!(
                f,
                "vehicle {vehicle_id} spans multiple protocols: {protocols:?}"
            ),
            ConsistencyError::UnstableCarId {
                vehicle_id,
                distinct_car_ids,
            } => write!(
                f,
                "vehicle {vehicle_id} has {distinct_car_ids} distinct car_ids"
            ),
            ConsistencyError::ImplausiblePressure {
                vehicle_id,
                pressure_kpa,
            } => write!(
                f,
                "vehicle {vehicle_id} has implausible pressure {pressure_kpa:.1} kPa"
            ),
            ConsistencyError::PressureVarianceTooHigh {
                vehicle_id,
                max_delta_kpa,
            } => write!(
                f,
                "vehicle {vehicle_id} has pressure variance delta={max_delta_kpa:.1} kPa"
            ),
        }
    }
}

/// Run consistency checks on a replay result.
pub fn assert_consistency(result: &ReplayResult) -> Vec<ConsistencyError> {
    let mut errors = vec![];

    for vehicle in result.vehicles.values() {
        // No vehicle UUID spans more than one protocol.
        if vehicle.rtl433_ids.len() > 1 {
            errors.push(ConsistencyError::CrossProtocolVehicle {
                vehicle_id: vehicle.vehicle_id,
                protocols: vehicle.rtl433_ids.iter().copied().collect(),
            });
        }

        // Pressure is physically plausible (per-vehicle average pressures).
        for &p in &vehicle.pressures_kpa {
            if p < MIN_PLAUSIBLE_PRESSURE_KPA || p > MAX_PLAUSIBLE_PRESSURE_KPA {
                errors.push(ConsistencyError::ImplausiblePressure {
                    vehicle_id: vehicle.vehicle_id,
                    pressure_kpa: p,
                });
            }
        }

        // No vehicle has a pressure variance so large it suggests a merge error.
        if !vehicle.pressures_kpa.is_empty() {
            let mean =
                vehicle.pressures_kpa.iter().sum::<f32>() / vehicle.pressures_kpa.len() as f32;
            let max_delta = vehicle
                .pressures_kpa
                .iter()
                .map(|p| (p - mean).abs())
                .fold(0.0_f32, f32::max);
            if max_delta > 50.0 {
                errors.push(ConsistencyError::PressureVarianceTooHigh {
                    vehicle_id: vehicle.vehicle_id,
                    max_delta_kpa: max_delta,
                });
            }
        }
    }

    errors
}

/// Print a human-readable summary of the replay result and consistency checks.
pub fn print_summary(result: &ReplayResult, errors: &[ConsistencyError]) {
    eprintln!("Replay: {}", result.path);
    eprintln!("Packets:   {}", result.packet_count);
    eprintln!("Vehicles:  {}", result.vehicles.len());
    eprintln!(
        "Cars:      {} ({} grouped, {} pending)",
        result.car_count(),
        result.grouped_vehicle_count(),
        result.pending_vehicle_count(),
    );
    eprintln!();

    // Cross-protocol check
    let cross_proto: Vec<_> = errors
        .iter()
        .filter(|e| matches!(e, ConsistencyError::CrossProtocolVehicle { .. }))
        .collect();
    if cross_proto.is_empty() {
        eprintln!("PASS  no cross-protocol vehicle merges");
    } else {
        for e in &cross_proto {
            eprintln!("FAIL  {e}");
        }
    }

    // Car ID stability check
    let unstable: Vec<_> = errors
        .iter()
        .filter(|e| matches!(e, ConsistencyError::UnstableCarId { .. }))
        .collect();
    if unstable.is_empty() {
        eprintln!("PASS  car_id stable for all vehicles");
    } else {
        for e in &unstable {
            eprintln!("FAIL  {e}");
        }
    }

    // Plausible pressure check
    let implausible: Vec<_> = errors
        .iter()
        .filter(|e| matches!(e, ConsistencyError::ImplausiblePressure { .. }))
        .collect();
    if implausible.is_empty() {
        eprintln!("PASS  all pressures in plausible range ({MIN_PLAUSIBLE_PRESSURE_KPA}-{MAX_PLAUSIBLE_PRESSURE_KPA} kPa)");
    } else {
        for e in &implausible {
            eprintln!("FAIL  {e}");
        }
    }

    // Pressure variance check
    let variance: Vec<_> = errors
        .iter()
        .filter(|e| matches!(e, ConsistencyError::PressureVarianceTooHigh { .. }))
        .collect();
    if variance.is_empty() {
        eprintln!("PASS  pressure variance within bounds for all vehicles");
    } else {
        for e in &variance {
            eprintln!("FAIL  {e}");
            eprintln!("      \u{2192} suggests cross-protocol merge bug");
        }
    }

    eprintln!();
    if errors.is_empty() {
        eprintln!("0 failures");
    } else {
        eprintln!("{} failure(s)", errors.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path() -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("../../tests/fixtures/wild/session_20260415_1000.jsonl.gz");
        p
    }

    #[test]
    fn replay_fixture_parses_all_packets() {
        let path = fixture_path();
        if !path.exists() {
            eprintln!("Skipping: fixture not found at {}", path.display());
            return;
        }
        let result = replay(&path, 65).expect("replay should succeed");
        assert!(result.packet_count > 0, "should parse at least one packet");
        assert!(
            !result.vehicles.is_empty(),
            "should create at least one vehicle"
        );
    }

    #[test]
    fn consistency_no_cross_protocol_merges() {
        let path = fixture_path();
        if !path.exists() {
            return;
        }
        let result = replay(&path, 65).expect("replay should succeed");
        let errors = assert_consistency(&result);
        let cross = errors
            .iter()
            .filter(|e| matches!(e, ConsistencyError::CrossProtocolVehicle { .. }))
            .count();
        assert_eq!(cross, 0, "no cross-protocol vehicle merges expected");
    }

    #[test]
    fn replay_result_car_counts_are_consistent() {
        let path = fixture_path();
        if !path.exists() {
            return;
        }
        let result = replay(&path, 65).expect("replay should succeed");
        assert_eq!(
            result.grouped_vehicle_count() + result.pending_vehicle_count(),
            result.vehicles.len(),
            "grouped + pending must equal total vehicles"
        );
    }
}
