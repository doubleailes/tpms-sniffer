use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Minimum number of distinct days of observations before routine detection
/// kicks in.
pub const MIN_DAYS: usize = 7;

/// A sudden pressure increase above this threshold (kPa) between consecutive
/// sessions is flagged as an inflation event.
pub const SUDDEN_INCREASE_THRESHOLD_KPA: f32 = 10.0;

/// Minimum number of data points for slow-decline linear regression.
pub const MIN_DECLINE_POINTS: usize = 3;

// ---------------------------------------------------------------------------
// Presence slots
// ---------------------------------------------------------------------------

/// Hourly presence slot for a car — one row per (car, hour).
#[derive(Debug, Clone, Serialize)]
pub struct PresenceSlot {
    pub car_id: String,
    pub slot_start: String,
    pub slot_end: String,
    pub sighting_count: i64,
    pub receiver_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Car routine
// ---------------------------------------------------------------------------

/// Behavioural routine inferred from presence slots.
#[derive(Debug, Clone, Serialize)]
pub struct CarRoutine {
    pub car_id: String,
    /// Modal arrival hour on weekdays (0–23), if detectable.
    pub typical_arrival_hour: Option<u8>,
    /// Modal departure hour on weekdays (0–23), if detectable.
    pub typical_departure_hour: Option<u8>,
    /// Presence probability per day-of-week (Mon=0 .. Sun=6), 0.0–1.0.
    pub weekday_presence: [f32; 7],
    /// Median dwell time in minutes per day when present.
    pub median_dwell_minutes: u32,
    /// Days on which the car was expected (based on weekday_presence > 0.5)
    /// but not seen.
    pub anomalous_absences: Vec<String>,
    /// Sighting timestamps outside the typical arrival–departure window.
    pub anomalous_presences: Vec<String>,
}

/// Compute a `CarRoutine` from a set of presence slots for one car.
///
/// Returns `None` if fewer than `MIN_DAYS` distinct days of data are available.
pub fn compute_routine(car_id: &str, slots: &[PresenceSlot]) -> Option<CarRoutine> {
    if slots.is_empty() {
        return None;
    }

    // Parse slot_start into DateTime to extract day-of-week and hour.
    let parsed: Vec<(DateTime<Utc>, &PresenceSlot)> = slots
        .iter()
        .filter_map(|s| {
            DateTime::parse_from_rfc3339(&s.slot_start)
                .ok()
                .map(|dt| (dt.with_timezone(&Utc), s))
        })
        .collect();

    // Distinct days of observation.
    let mut day_set: std::collections::HashSet<NaiveDate> = std::collections::HashSet::new();
    for &(dt, _) in &parsed {
        day_set.insert(dt.date_naive());
    }
    if day_set.len() < MIN_DAYS {
        return None;
    }

    // --- weekday presence probability ---
    // Group observed days by day-of-week (Mon=0..Sun=6).
    let mut dow_seen: [std::collections::HashSet<NaiveDate>; 7] = Default::default();
    for &date in &day_set {
        let dow = date.weekday().num_days_from_monday() as usize;
        dow_seen[dow].insert(date);
    }
    // Count how many weeks are covered.
    let all_dates_sorted: Vec<NaiveDate> = {
        let mut v: Vec<NaiveDate> = day_set.iter().copied().collect();
        v.sort();
        v
    };
    let first_date = all_dates_sorted[0];
    let last_date = *all_dates_sorted.last().unwrap();
    let span_days = (last_date - first_date).num_days().max(1) as f32;
    let span_weeks = (span_days / 7.0).max(1.0);

    let mut weekday_presence = [0.0f32; 7];
    for dow in 0..7 {
        weekday_presence[dow] = (dow_seen[dow].len() as f32 / span_weeks).min(1.0);
    }

    // --- typical arrival / departure (weekday hours) ---
    // Arrival = first hour seen on a given day; departure = last hour.
    let mut arrival_hours: Vec<u8> = Vec::new();
    let mut departure_hours: Vec<u8> = Vec::new();
    // Group parsed slots by date.
    let mut by_date: HashMap<NaiveDate, Vec<u8>> = HashMap::new();
    for &(dt, _) in &parsed {
        by_date
            .entry(dt.date_naive())
            .or_default()
            .push(dt.hour() as u8);
    }
    for (date, hours) in &by_date {
        // Only consider weekdays (Mon–Fri).
        let dow = date.weekday().num_days_from_monday();
        if dow >= 5 {
            continue;
        }
        let min_h = *hours.iter().min().unwrap();
        let max_h = *hours.iter().max().unwrap();
        arrival_hours.push(min_h);
        departure_hours.push(max_h);
    }

    let typical_arrival_hour = mode_u8(&arrival_hours);
    let typical_departure_hour = mode_u8(&departure_hours);

    // --- median dwell minutes ---
    let mut dwell_minutes: Vec<u32> = Vec::new();
    for hours in by_date.values() {
        // dwell = (max_hour - min_hour + 1) * 60 as an approximation
        if let (Some(&min_h), Some(&max_h)) = (hours.iter().min(), hours.iter().max()) {
            dwell_minutes.push((max_h as u32 - min_h as u32 + 1) * 60);
        }
    }
    dwell_minutes.sort_unstable();
    let median_dwell_minutes = if dwell_minutes.is_empty() {
        0
    } else {
        dwell_minutes[dwell_minutes.len() / 2]
    };

    // --- anomalous absences ---
    // Days where the car was expected (weekday_presence > 0.5 for that dow)
    // but not seen.
    let mut anomalous_absences: Vec<String> = Vec::new();
    {
        let mut d = first_date;
        while d <= last_date {
            let dow = d.weekday().num_days_from_monday() as usize;
            if weekday_presence[dow] > 0.5 && !day_set.contains(&d) {
                anomalous_absences.push(d.to_string());
            }
            d += chrono::Duration::days(1);
        }
    }

    // --- anomalous presences ---
    // Sightings outside the typical arrival–departure window on weekdays.
    let mut anomalous_presences: Vec<String> = Vec::new();
    if let (Some(arr), Some(dep)) = (typical_arrival_hour, typical_departure_hour) {
        for &(dt, _) in &parsed {
            let dow = dt.weekday().num_days_from_monday();
            if dow >= 5 {
                continue;
            }
            let h = dt.hour() as u8;
            if h < arr.saturating_sub(1) || h > dep + 1 {
                anomalous_presences.push(dt.to_rfc3339());
            }
        }
    }

    Some(CarRoutine {
        car_id: car_id.to_string(),
        typical_arrival_hour,
        typical_departure_hour,
        weekday_presence,
        median_dwell_minutes,
        anomalous_absences,
        anomalous_presences,
    })
}

/// Return the statistical mode of a `u8` slice (most frequent value).
fn mode_u8(values: &[u8]) -> Option<u8> {
    if values.is_empty() {
        return None;
    }
    let mut counts: HashMap<u8, usize> = HashMap::new();
    for &v in values {
        *counts.entry(v).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .max_by_key(|&(_, count)| count)
        .map(|(val, _)| val)
}

// ---------------------------------------------------------------------------
// Pressure events
// ---------------------------------------------------------------------------

/// Significant pressure event detected from time-series analysis.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event_type")]
pub enum PressureEvent {
    #[serde(rename = "slow_decline")]
    SlowDecline {
        car_id: String,
        vehicle_id: String,
        rate_kpa_per_day: f32,
        ts: String,
    },
    #[serde(rename = "sudden_increase")]
    SuddenIncrease {
        car_id: String,
        vehicle_id: String,
        delta_kpa: f32,
        ts: String,
    },
    #[serde(rename = "alarm")]
    AlarmThreshold {
        car_id: String,
        vehicle_id: String,
        pressure_kpa: f32,
        ts: String,
    },
}

/// A single pressure reading with timestamp, used as input for trend detection.
#[derive(Debug, Clone)]
pub struct PressureReading {
    pub ts: DateTime<Utc>,
    pub pressure_kpa: f32,
    pub alarm: bool,
}

/// Detect pressure events from a time-ordered sequence of readings for one
/// vehicle.
pub fn detect_pressure_events(
    car_id: &str,
    vehicle_id: &str,
    readings: &[PressureReading],
) -> Vec<PressureEvent> {
    let mut events: Vec<PressureEvent> = Vec::new();
    if readings.is_empty() {
        return events;
    }

    // 1) Sudden increase detection (consecutive session comparison).
    for pair in readings.windows(2) {
        let prev = &pair[0];
        let curr = &pair[1];
        let delta = curr.pressure_kpa - prev.pressure_kpa;
        if delta > SUDDEN_INCREASE_THRESHOLD_KPA {
            events.push(PressureEvent::SuddenIncrease {
                car_id: car_id.to_string(),
                vehicle_id: vehicle_id.to_string(),
                delta_kpa: delta,
                ts: curr.ts.to_rfc3339(),
            });
        }
    }

    // 2) Alarm threshold detection.
    for r in readings {
        if r.alarm {
            events.push(PressureEvent::AlarmThreshold {
                car_id: car_id.to_string(),
                vehicle_id: vehicle_id.to_string(),
                pressure_kpa: r.pressure_kpa,
                ts: r.ts.to_rfc3339(),
            });
        }
    }

    // 3) Slow decline detection via simple linear regression over the full
    //    sequence (when enough points exist and no sudden increases interrupt).
    if readings.len() >= MIN_DECLINE_POINTS {
        // Use timestamps as x (days since first reading) and pressure as y.
        let t0 = readings[0].ts;
        let xs: Vec<f64> = readings
            .iter()
            .map(|r| (r.ts - t0).num_seconds() as f64 / 86400.0)
            .collect();
        let ys: Vec<f64> = readings.iter().map(|r| r.pressure_kpa as f64).collect();
        let n = xs.len() as f64;
        let sum_x: f64 = xs.iter().sum();
        let sum_y: f64 = ys.iter().sum();
        let sum_xy: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| x * y).sum();
        let sum_x2: f64 = xs.iter().map(|x| x * x).sum();
        let denom = n * sum_x2 - sum_x * sum_x;
        if denom.abs() > f64::EPSILON {
            let slope = (n * sum_xy - sum_x * sum_y) / denom;
            // Only report if slope is negative (declining).
            if slope < -0.05 {
                let last = readings.last().unwrap();
                events.push(PressureEvent::SlowDecline {
                    car_id: car_id.to_string(),
                    vehicle_id: vehicle_id.to_string(),
                    rate_kpa_per_day: slope as f32,
                    ts: last.ts.to_rfc3339(),
                });
            }
        }
    }

    events
}

// ---------------------------------------------------------------------------
// GeoJSON export
// ---------------------------------------------------------------------------

/// A GeoJSON Feature with LineString geometry for a car's sighting path.
#[derive(Debug, Serialize)]
pub struct GeoJsonFeature {
    #[serde(rename = "type")]
    pub feature_type: String,
    pub properties: GeoJsonProperties,
    pub geometry: GeoJsonGeometry,
}

#[derive(Debug, Serialize)]
pub struct GeoJsonProperties {
    pub car_id: String,
    pub ts: String,
}

#[derive(Debug, Serialize)]
pub struct GeoJsonGeometry {
    #[serde(rename = "type")]
    pub geometry_type: String,
    pub coordinates: Vec<[f64; 2]>,
}

/// A GeoJSON FeatureCollection.
#[derive(Debug, Serialize)]
pub struct GeoJsonFeatureCollection {
    #[serde(rename = "type")]
    pub collection_type: String,
    pub features: Vec<GeoJsonFeature>,
}

/// A sighting with geographic coordinates, used as input for GeoJSON export.
#[derive(Debug, Clone)]
pub struct GeoSighting {
    pub car_id: String,
    pub ts: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
}

/// Build a GeoJSON FeatureCollection from geographic sightings grouped by car.
///
/// Each car's sighting path becomes a single LineString feature. A car needs
/// at least 2 sightings with coordinates to produce a feature.
pub fn build_geojson(sightings: &[GeoSighting]) -> GeoJsonFeatureCollection {
    // Group by car_id.
    let mut by_car: HashMap<String, Vec<&GeoSighting>> = HashMap::new();
    for s in sightings {
        by_car.entry(s.car_id.clone()).or_default().push(s);
    }

    let mut features = Vec::new();
    for (car_id, mut car_sightings) in by_car {
        if car_sightings.len() < 2 {
            continue;
        }
        car_sightings.sort_by_key(|s| s.ts);
        let first_ts = car_sightings[0].ts.to_rfc3339();
        let coordinates: Vec<[f64; 2]> = car_sightings
            .iter()
            .map(|s| [s.lon, s.lat])
            .collect();
        features.push(GeoJsonFeature {
            feature_type: "Feature".to_string(),
            properties: GeoJsonProperties {
                car_id,
                ts: first_ts,
            },
            geometry: GeoJsonGeometry {
                geometry_type: "LineString".to_string(),
                coordinates,
            },
        });
    }

    GeoJsonFeatureCollection {
        collection_type: "FeatureCollection".to_string(),
        features,
    }
}

// ---------------------------------------------------------------------------
// Report output
// ---------------------------------------------------------------------------

/// Full report for a single car, combining routine + pressure data.
#[derive(Debug, Clone, Serialize)]
pub struct CarReport {
    pub car_id: String,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
    pub total_sessions: i64,
    pub routine: Option<CarRoutine>,
    pub pressure_events: Vec<PressureEvent>,
    pub vehicles: Vec<VehicleSummary>,
}

/// Summary of one vehicle track belonging to a car.
#[derive(Debug, Clone, Serialize)]
pub struct VehicleSummary {
    pub vehicle_id: String,
    pub protocol: String,
    pub make_model: Option<String>,
    pub avg_pressure_kpa: f32,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_slot(car_id: &str, dt: DateTime<Utc>, count: i64) -> PresenceSlot {
        let start = dt.format("%Y-%m-%dT%H:00:00+00:00").to_string();
        let end_dt = dt + chrono::Duration::hours(1);
        let end = end_dt.format("%Y-%m-%dT%H:00:00+00:00").to_string();
        PresenceSlot {
            car_id: car_id.to_string(),
            slot_start: start,
            slot_end: end,
            sighting_count: count,
            receiver_ids: vec!["default".to_string()],
        }
    }

    #[test]
    fn routine_returns_none_below_min_days() {
        let car_id = "test-car";
        // Only 3 distinct days.
        let slots: Vec<PresenceSlot> = (0..3)
            .map(|d| {
                let dt = Utc.with_ymd_and_hms(2026, 4, 1 + d, 9, 0, 0).unwrap();
                make_slot(car_id, dt, 5)
            })
            .collect();
        assert!(compute_routine(car_id, &slots).is_none());
    }

    #[test]
    fn routine_computed_with_sufficient_data() {
        let car_id = "test-car";
        // 10 weekdays (Mon 2026-04-06 through Fri 2026-04-17 skipping weekends),
        // arrival 08:00, departure 17:00.
        let mut slots = Vec::new();
        let dates = [6, 7, 8, 9, 10, 13, 14, 15, 16, 17]; // April 2026 weekdays
        for &day in &dates {
            for hour in 8..=17 {
                let dt = Utc.with_ymd_and_hms(2026, 4, day, hour, 0, 0).unwrap();
                slots.push(make_slot(car_id, dt, 3));
            }
        }
        let routine = compute_routine(car_id, &slots).expect("should produce a routine");
        assert_eq!(routine.typical_arrival_hour, Some(8));
        assert_eq!(routine.typical_departure_hour, Some(17));
        // Mon–Fri should have high presence.
        for dow in 0..5 {
            assert!(
                routine.weekday_presence[dow] > 0.3,
                "dow {dow} presence should be high"
            );
        }
    }

    #[test]
    fn sudden_increase_detected() {
        let readings = vec![
            PressureReading {
                ts: Utc.with_ymd_and_hms(2026, 4, 1, 8, 0, 0).unwrap(),
                pressure_kpa: 220.0,
                alarm: false,
            },
            PressureReading {
                ts: Utc.with_ymd_and_hms(2026, 4, 1, 9, 0, 0).unwrap(),
                pressure_kpa: 235.0, // +15 kPa
                alarm: false,
            },
        ];
        let events = detect_pressure_events("car1", "v1", &readings);
        let has_sudden = events.iter().any(|e| matches!(e, PressureEvent::SuddenIncrease { delta_kpa, .. } if *delta_kpa > 10.0));
        assert!(has_sudden, "should detect a sudden increase event");
    }

    #[test]
    fn no_sudden_increase_below_threshold() {
        let readings = vec![
            PressureReading {
                ts: Utc.with_ymd_and_hms(2026, 4, 1, 8, 0, 0).unwrap(),
                pressure_kpa: 220.0,
                alarm: false,
            },
            PressureReading {
                ts: Utc.with_ymd_and_hms(2026, 4, 1, 9, 0, 0).unwrap(),
                pressure_kpa: 225.0, // +5 kPa — below threshold
                alarm: false,
            },
        ];
        let events = detect_pressure_events("car1", "v1", &readings);
        let has_sudden = events
            .iter()
            .any(|e| matches!(e, PressureEvent::SuddenIncrease { .. }));
        assert!(!has_sudden, "should not detect a sudden increase below threshold");
    }

    #[test]
    fn alarm_threshold_detected() {
        let readings = vec![PressureReading {
            ts: Utc.with_ymd_and_hms(2026, 4, 1, 8, 0, 0).unwrap(),
            pressure_kpa: 180.0,
            alarm: true,
        }];
        let events = detect_pressure_events("car1", "v1", &readings);
        let has_alarm = events
            .iter()
            .any(|e| matches!(e, PressureEvent::AlarmThreshold { .. }));
        assert!(has_alarm, "should detect an alarm threshold event");
    }

    #[test]
    fn slow_decline_detected() {
        // Simulate pressure dropping steadily over 5 days.
        let readings: Vec<PressureReading> = (0..5)
            .map(|d| PressureReading {
                ts: Utc.with_ymd_and_hms(2026, 4, 1 + d, 8, 0, 0).unwrap(),
                pressure_kpa: 230.0 - d as f32 * 1.0,
                alarm: false,
            })
            .collect();
        let events = detect_pressure_events("car1", "v1", &readings);
        let has_decline = events
            .iter()
            .any(|e| matches!(e, PressureEvent::SlowDecline { .. }));
        assert!(has_decline, "should detect a slow decline event");
    }

    #[test]
    fn geojson_builds_valid_collection() {
        let sightings = vec![
            GeoSighting {
                car_id: "car1".to_string(),
                ts: Utc.with_ymd_and_hms(2026, 4, 13, 8, 0, 0).unwrap(),
                lat: 43.0,
                lon: -1.5,
            },
            GeoSighting {
                car_id: "car1".to_string(),
                ts: Utc.with_ymd_and_hms(2026, 4, 13, 9, 0, 0).unwrap(),
                lat: 43.01,
                lon: -1.49,
            },
        ];
        let fc = build_geojson(&sightings);
        assert_eq!(fc.collection_type, "FeatureCollection");
        assert_eq!(fc.features.len(), 1);
        assert_eq!(fc.features[0].geometry.geometry_type, "LineString");
        assert_eq!(fc.features[0].geometry.coordinates.len(), 2);
        // Validate serializes to valid JSON.
        let json = serde_json::to_string_pretty(&fc).unwrap();
        assert!(json.contains("FeatureCollection"));
        assert!(json.contains("LineString"));
    }

    #[test]
    fn geojson_skips_single_point_car() {
        let sightings = vec![GeoSighting {
            car_id: "car1".to_string(),
            ts: Utc.with_ymd_and_hms(2026, 4, 13, 8, 0, 0).unwrap(),
            lat: 43.0,
            lon: -1.5,
        }];
        let fc = build_geojson(&sightings);
        assert!(fc.features.is_empty(), "single-point car should be skipped");
    }
}
