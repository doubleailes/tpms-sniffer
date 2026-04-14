use std::collections::{HashMap, HashSet};

use serde::Serialize;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Tuning constants
// ---------------------------------------------------------------------------

/// Duration of each co-occurrence window in seconds.
pub const WINDOW_SIZE_S: u64 = 60;

/// Number of rolling windows to retain for Jaccard computation.
pub const N_WINDOWS: usize = 10;

/// Minimum number of windows before Jaccard scores are used for grouping.
pub const MIN_WINDOWS: u32 = 3;

/// Iterative threshold descent: start strict, relax until all grouped.
pub const THRESHOLDS: &[f32] = &[0.75, 0.60, 0.45, 0.30];

/// Minimum number of shared most-significant bytes for two fixed-ID sensors
/// to be considered candidate wheel-mates via prefix grouping.
pub const MIN_PREFIX_BYTES: u8 = 2;

// ---------------------------------------------------------------------------
// Prefix byte similarity
// ---------------------------------------------------------------------------

/// Returns the number of most-significant bytes shared between two sensor IDs.
/// e.g. 0xA3B2C100 and 0xA3B2C200 share 3 bytes → returns 3.
pub fn common_prefix_bytes(a: u32, b: u32) -> u8 {
    let xor = a ^ b;
    if xor & 0xFF00_0000 != 0 {
        return 0;
    }
    if xor & 0x00FF_0000 != 0 {
        return 1;
    }
    if xor & 0x0000_FF00 != 0 {
        return 2;
    }
    if xor & 0x0000_00FF != 0 {
        return 3;
    }
    4 // identical
}

// ---------------------------------------------------------------------------
// Wheel position
// ---------------------------------------------------------------------------

/// Inferred wheel position for a fixed-ID sensor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum WheelPosition {
    FL,
    FR,
    RL,
    RR,
}

impl WheelPosition {
    /// Parse from a string (e.g. database column).
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "FL" => Some(Self::FL),
            "FR" => Some(Self::FR),
            "RL" => Some(Self::RL),
            "RR" => Some(Self::RR),
            _ => None,
        }
    }

    /// Return the canonical string label.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FL => "FL",
            Self::FR => "FR",
            Self::RL => "RL",
            Self::RR => "RR",
        }
    }
}

impl std::fmt::Display for WheelPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Attempt to infer wheel positions for a group of 4 sensor IDs.
///
/// Returns a mapping from sensor ID → wheel position when all 4 sensors share
/// a common byte prefix and their trailing bytes form 4 consecutive values (in
/// any order).  The lowest trailing byte is assigned FL, then FR, RL, RR.
pub fn infer_wheel_positions(group: &[u32]) -> Option<HashMap<u32, WheelPosition>> {
    if group.len() != 4 {
        return None;
    }
    let trailing: Vec<u8> = group.iter().map(|&id| (id & 0xFF) as u8).collect();
    let mut sorted = trailing.clone();
    sorted.sort();
    let is_consecutive = sorted.windows(2).all(|w| w[1] == w[0] + 1);
    if !is_consecutive {
        return None;
    }
    let positions = [
        WheelPosition::FL,
        WheelPosition::FR,
        WheelPosition::RL,
        WheelPosition::RR,
    ];
    Some(
        sorted
            .iter()
            .zip(positions.iter())
            .map(|(&byte, &pos)| {
                let id = *group
                    .iter()
                    .find(|&&id| (id & 0xFF) as u8 == byte)
                    .unwrap();
                (id, pos)
            })
            .collect(),
    )
}

/// Metadata for a vehicle track needed by prefix-based candidate filtering.
pub struct VehicleMeta {
    pub vehicle_id: Uuid,
    pub rtl433_id: u16,
    pub fixed_sensor_id: Option<u32>,
}

/// Return candidate wheel-mate vehicle IDs for `target` by filtering on
/// shared sensor-ID byte prefix.  Only applies to fixed-ID protocols; vehicles
/// without a `fixed_sensor_id` are excluded.
pub fn candidate_wheel_mates(
    target: &VehicleMeta,
    all_vehicles: &[VehicleMeta],
) -> Vec<Uuid> {
    let Some(target_sid) = target.fixed_sensor_id else {
        return vec![];
    };
    all_vehicles
        .iter()
        .filter(|v| v.rtl433_id == target.rtl433_id)
        .filter(|v| v.vehicle_id != target.vehicle_id)
        .filter(|v| {
            if let Some(b) = v.fixed_sensor_id {
                common_prefix_bytes(target_sid, b) >= MIN_PREFIX_BYTES
            } else {
                false
            }
        })
        .map(|v| v.vehicle_id)
        .collect()
}

// ---------------------------------------------------------------------------
// CoOccurrenceMatrix
// ---------------------------------------------------------------------------

/// Tracks pairwise co-occurrence of vehicle UUIDs across 1-minute sliding
/// windows for Jaccard-index computation.
pub struct CoOccurrenceMatrix {
    /// Per-window vehicle sets; most recent window is at the back.
    windows: Vec<HashSet<Uuid>>,
    /// Aggregated counts: `counts[(a, b)]` = number of windows where both
    /// `a` and `b` appeared (a < b by lexicographic order on Uuid).
    counts: HashMap<(Uuid, Uuid), u32>,
    /// `appearances[a]` = number of windows where `a` appeared at all.
    appearances: HashMap<Uuid, u32>,
    /// Total number of windows that have been flushed (accumulated).
    pub windows_accumulated: u32,
}

/// Ensure the pair key is always ordered (a < b) so that (a,b) and (b,a) map
/// to the same entry.
fn ordered_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a < b { (a, b) } else { (b, a) }
}

impl CoOccurrenceMatrix {
    pub fn new() -> Self {
        Self {
            windows: Vec::new(),
            counts: HashMap::new(),
            appearances: HashMap::new(),
            windows_accumulated: 0,
        }
    }

    /// Record that `vehicle_id` was seen in the current (most recent) window.
    /// If there is no open window yet, one is created automatically.
    pub fn record(&mut self, vehicle_id: Uuid) {
        if self.windows.is_empty() {
            self.windows.push(HashSet::new());
        }
        self.windows.last_mut().unwrap().insert(vehicle_id);
    }

    /// Flush the current window: update aggregated counts, then open a new
    /// empty window.  If the rolling history exceeds `N_WINDOWS`, remove the
    /// oldest window and decrement its contribution from the aggregates.
    pub fn advance_window(&mut self) {
        if self.windows.is_empty() {
            return;
        }

        // Update aggregates from the window that just closed.
        let current = self.windows.last().unwrap();
        let ids: Vec<Uuid> = current.iter().copied().collect();
        for &id in &ids {
            *self.appearances.entry(id).or_insert(0) += 1;
        }
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let key = ordered_pair(ids[i], ids[j]);
                *self.counts.entry(key).or_insert(0) += 1;
            }
        }
        self.windows_accumulated += 1;

        // Evict oldest window if history exceeds N_WINDOWS.
        if self.windows.len() >= N_WINDOWS {
            let oldest = self.windows.remove(0);
            let old_ids: Vec<Uuid> = oldest.iter().copied().collect();
            for &id in &old_ids {
                if let Some(c) = self.appearances.get_mut(&id) {
                    *c = c.saturating_sub(1);
                    if *c == 0 {
                        self.appearances.remove(&id);
                    }
                }
            }
            for i in 0..old_ids.len() {
                for j in (i + 1)..old_ids.len() {
                    let key = ordered_pair(old_ids[i], old_ids[j]);
                    if let Some(c) = self.counts.get_mut(&key) {
                        *c = c.saturating_sub(1);
                        if *c == 0 {
                            self.counts.remove(&key);
                        }
                    }
                }
            }
        }

        // Open a new empty window for future records.
        self.windows.push(HashSet::new());
    }

    /// Compute the Jaccard index between two vehicle tracks.
    ///
    /// Returns `0.0` when fewer than `MIN_WINDOWS` windows have been
    /// accumulated for either vehicle.
    pub fn jaccard(&self, a: Uuid, b: Uuid) -> f32 {
        let app_a = *self.appearances.get(&a).unwrap_or(&0);
        let app_b = *self.appearances.get(&b).unwrap_or(&0);
        if app_a < MIN_WINDOWS || app_b < MIN_WINDOWS {
            return 0.0;
        }
        let key = ordered_pair(a, b);
        let intersection = *self.counts.get(&key).unwrap_or(&0) as f32;
        let union = app_a as f32 + app_b as f32 - intersection;
        if union == 0.0 {
            return 0.0;
        }
        intersection / union
    }

    /// Return all vehicle IDs that have appeared in any window.
    pub fn known_vehicles(&self) -> HashSet<Uuid> {
        self.appearances.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// CarGroup
// ---------------------------------------------------------------------------

/// A group of vehicle track UUIDs that belong to the same physical car.
#[derive(Debug, Clone)]
pub struct CarGroup {
    pub car_id: Uuid,
    pub members: HashSet<Uuid>,
}

impl CarGroup {
    pub fn new(seed: Uuid) -> Self {
        let mut members = HashSet::new();
        members.insert(seed);
        Self {
            car_id: Uuid::new_v4(),
            members,
        }
    }

    pub fn contains(&self, id: Uuid) -> bool {
        self.members.contains(&id)
    }

    pub fn add(&mut self, id: Uuid) {
        self.members.insert(id);
    }

    pub fn wheel_count(&self) -> usize {
        self.members.len()
    }
}

// ---------------------------------------------------------------------------
// Grouping algorithm (iterative threshold descent)
// ---------------------------------------------------------------------------

/// Group vehicle tracks into cars using Jaccard co-occurrence scores.
///
/// Implements the iterative threshold descent from Section VI-C of
/// Lizarribar et al. (2024): start with a strict threshold and relax
/// progressively until all vehicles are assigned to a group.
pub fn group_vehicles_into_cars(
    matrix: &CoOccurrenceMatrix,
    vehicle_ids: &[Uuid],
) -> Vec<CarGroup> {
    group_vehicles_into_cars_with_meta(matrix, vehicle_ids, &[])
}

/// Extended grouping that uses sensor-ID prefix similarity to seed groups
/// for fixed-ID protocols before applying Jaccard co-occurrence scores.
///
/// When `vehicle_meta` is non-empty, vehicles whose fixed sensor IDs share
/// at least `MIN_PREFIX_BYTES` most-significant bytes are pre-grouped before
/// the iterative threshold descent.  Rolling-ID vehicles (those without a
/// `fixed_sensor_id`) skip the prefix step entirely and are grouped only by
/// Jaccard score.
pub fn group_vehicles_into_cars_with_meta(
    matrix: &CoOccurrenceMatrix,
    vehicle_ids: &[Uuid],
    vehicle_meta: &[VehicleMeta],
) -> Vec<CarGroup> {
    let mut groups: Vec<CarGroup> = vec![];
    let mut ungrouped: HashSet<Uuid> = vehicle_ids.iter().copied().collect();

    // --- Phase 0: Prefix-based seeding for fixed-ID protocols ---------------
    if !vehicle_meta.is_empty() {
        let meta_map: HashMap<Uuid, &VehicleMeta> =
            vehicle_meta.iter().map(|m| (m.vehicle_id, m)).collect();

        // Group by (rtl433_id, prefix) for each candidate in `ungrouped`.
        // We iterate in sorted order for determinism.
        let mut sorted_ungrouped: Vec<Uuid> = ungrouped.iter().copied().collect();
        sorted_ungrouped.sort();

        for &a in &sorted_ungrouped {
            if !ungrouped.contains(&a) {
                continue;
            }
            let Some(meta_a) = meta_map.get(&a) else {
                continue;
            };
            let Some(sid_a) = meta_a.fixed_sensor_id else {
                continue;
            };

            // Find prefix-matching peers among ungrouped vehicles.
            let mates: Vec<Uuid> = candidate_wheel_mates(
                meta_a,
                &ungrouped
                    .iter()
                    .filter_map(|&id| meta_map.get(&id).map(|m| VehicleMeta {
                        vehicle_id: m.vehicle_id,
                        rtl433_id: m.rtl433_id,
                        fixed_sensor_id: m.fixed_sensor_id,
                    }))
                    .collect::<Vec<_>>(),
            );

            if mates.is_empty() {
                continue;
            }

            // Find or create a group for `a`.
            let group = if let Some(g) = groups.iter_mut().find(|g| g.contains(a)) {
                g
            } else {
                ungrouped.remove(&a);
                let g = CarGroup::new(a);
                groups.push(g);
                groups.last_mut().unwrap()
            };

            for mate_id in mates {
                if ungrouped.contains(&mate_id) {
                    // Only add if sensor IDs also share a prefix with `a`.
                    if let Some(meta_b) = meta_map.get(&mate_id) {
                        if let Some(sid_b) = meta_b.fixed_sensor_id {
                            if common_prefix_bytes(sid_a, sid_b) >= MIN_PREFIX_BYTES {
                                group.add(mate_id);
                                ungrouped.remove(&mate_id);
                            }
                        }
                    }
                }
            }
        }
    }

    // --- Phase 1: Iterative Jaccard threshold descent -----------------------
    for &threshold in THRESHOLDS {
        // Iterate over a snapshot so we can mutate `ungrouped`.
        let snapshot: Vec<Uuid> = ungrouped.iter().copied().collect();
        for a in snapshot {
            if !ungrouped.contains(&a) {
                continue;
            }

            if let Some(group) = groups.iter_mut().find(|g| g.contains(a)) {
                // Extend existing group with high-Jaccard neighbours.
                let candidates: Vec<Uuid> = ungrouped.iter().copied().collect();
                for b in candidates {
                    if a != b && matrix.jaccard(a, b) >= threshold {
                        group.add(b);
                        ungrouped.remove(&b);
                    }
                }
            } else {
                // Seed a new group.
                let mut new_group = CarGroup::new(a);
                ungrouped.remove(&a);
                let candidates: Vec<Uuid> = ungrouped.iter().copied().collect();
                for b in candidates {
                    if matrix.jaccard(a, b) >= threshold {
                        new_group.add(b);
                        ungrouped.remove(&b);
                    }
                }
                groups.push(new_group);
            }
        }
    }

    // Any remaining ungrouped vehicles become singleton groups.
    for id in ungrouped {
        groups.push(CarGroup::new(id));
    }

    groups
}

// ---------------------------------------------------------------------------
// JSON export types
// ---------------------------------------------------------------------------

/// A single pair entry in the Jaccard export JSON.
#[derive(Debug, Serialize)]
pub struct JaccardPairExport {
    pub a: String,
    pub b: String,
    pub jaccard: f32,
}

/// Top-level Jaccard export JSON structure.
#[derive(Debug, Serialize)]
pub struct JaccardExport {
    pub window_size_s: u64,
    pub windows_accumulated: u32,
    pub pairs: Vec<JaccardPairExport>,
}

impl CoOccurrenceMatrix {
    /// Produce a serializable export of all non-zero Jaccard pairs.
    pub fn export(&self) -> JaccardExport {
        let mut pairs = Vec::new();
        for (&(a, b), _) in &self.counts {
            let score = self.jaccard(a, b);
            if score > 0.0 {
                pairs.push(JaccardPairExport {
                    a: a.to_string(),
                    b: b.to_string(),
                    jaccard: score,
                });
            }
        }
        // Sort for deterministic output.
        pairs.sort_by(|x, y| {
            x.a.cmp(&y.a)
                .then(x.b.cmp(&y.b))
        });
        JaccardExport {
            window_size_s: WINDOW_SIZE_S,
            windows_accumulated: self.windows_accumulated,
            pairs,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jaccard_returns_zero_below_min_windows() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        // Record both in 2 windows (below MIN_WINDOWS = 3).
        for _ in 0..2 {
            m.record(a);
            m.record(b);
            m.advance_window();
        }

        assert_eq!(m.jaccard(a, b), 0.0);
    }

    #[test]
    fn jaccard_perfect_cooccurrence() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        // Both appear together in every window.
        for _ in 0..5 {
            m.record(a);
            m.record(b);
            m.advance_window();
        }

        let score = m.jaccard(a, b);
        assert!(
            (score - 1.0).abs() < f32::EPSILON,
            "perfect co-occurrence should give Jaccard ≈ 1.0, got {score}"
        );
    }

    #[test]
    fn jaccard_no_overlap() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        // a appears in windows 0–4, b appears in windows 5–9 (no overlap).
        for _ in 0..5 {
            m.record(a);
            m.advance_window();
        }
        for _ in 0..5 {
            m.record(b);
            m.advance_window();
        }

        assert_eq!(
            m.jaccard(a, b),
            0.0,
            "non-overlapping vehicles should have Jaccard = 0"
        );
    }

    #[test]
    fn jaccard_partial_overlap() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        // a appears in 5 windows, b appears in 4 of those + 1 alone.
        for i in 0..6 {
            m.record(a);
            if i < 4 {
                m.record(b);
            }
            m.advance_window();
        }
        // b also appears alone in 1 extra window.
        m.record(b);
        m.advance_window();

        // intersection = 4, union = 6 + 5 - 4 = 7 → J = 4/7 ≈ 0.5714
        let score = m.jaccard(a, b);
        assert!(
            (score - 4.0 / 7.0).abs() < 0.01,
            "expected ~0.571, got {score}"
        );
    }

    #[test]
    fn window_eviction_removes_old_data() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();

        // Fill N_WINDOWS windows with `a`.
        for _ in 0..N_WINDOWS {
            m.record(a);
            m.advance_window();
        }

        let app_before = *m.appearances.get(&a).unwrap_or(&0);

        // Advance one more window without `a` → the oldest window (containing
        // `a`) should be evicted.
        m.advance_window();

        let app_after = *m.appearances.get(&a).unwrap_or(&0);
        assert!(
            app_after < app_before,
            "eviction should decrement appearance count"
        );
    }

    #[test]
    fn group_four_cooccurring_plus_one_outsider() {
        // Acceptance-criteria test: inject 4 vehicle tracks with high mutual
        // co-occurrence and 1 with low; assert exactly one CarGroup of size 4
        // is produced.
        let mut m = CoOccurrenceMatrix::new();
        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let v3 = Uuid::new_v4();
        let v4 = Uuid::new_v4();
        let outsider = Uuid::new_v4();

        // v1..v4 always appear together; outsider appears in separate windows.
        for i in 0..10 {
            m.record(v1);
            m.record(v2);
            m.record(v3);
            m.record(v4);
            if i % 3 == 0 {
                // outsider occasionally appears in a different window
            }
            m.advance_window();
            if i % 3 == 0 {
                m.record(outsider);
                m.advance_window();
            }
        }
        // Give outsider enough solo windows to reach MIN_WINDOWS.
        for _ in 0..5 {
            m.record(outsider);
            m.advance_window();
        }

        let all = vec![v1, v2, v3, v4, outsider];
        let groups = group_vehicles_into_cars(&m, &all);

        // Find the group containing v1.
        let car_group = groups.iter().find(|g| g.contains(v1)).unwrap();
        assert!(car_group.contains(v2));
        assert!(car_group.contains(v3));
        assert!(car_group.contains(v4));
        assert_eq!(car_group.wheel_count(), 4);

        // Outsider must be in a different group.
        assert!(
            !car_group.contains(outsider),
            "outsider must not be in the same group as the 4 co-occurring vehicles"
        );

        // The outsider should be in its own singleton group.
        let outsider_group = groups.iter().find(|g| g.contains(outsider)).unwrap();
        assert_eq!(outsider_group.wheel_count(), 1);
    }

    #[test]
    fn export_produces_valid_json() {
        let mut m = CoOccurrenceMatrix::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        for _ in 0..5 {
            m.record(a);
            m.record(b);
            m.advance_window();
        }

        let export = m.export();
        assert_eq!(export.window_size_s, WINDOW_SIZE_S);
        assert_eq!(export.windows_accumulated, 5);
        assert!(!export.pairs.is_empty());

        // Should be serializable to JSON.
        let json = serde_json::to_string_pretty(&export).unwrap();
        assert!(json.contains("jaccard"));
    }

    // -----------------------------------------------------------------------
    // common_prefix_bytes tests (0–4)
    // -----------------------------------------------------------------------

    #[test]
    fn common_prefix_bytes_returns_0_for_first_byte_diff() {
        // 0xA3... vs 0xB3... → differ in byte 0.
        assert_eq!(common_prefix_bytes(0xA300_0000, 0xB300_0000), 0);
    }

    #[test]
    fn common_prefix_bytes_returns_1_for_second_byte_diff() {
        // Same first byte, different second byte.
        assert_eq!(common_prefix_bytes(0xA3B2_0000, 0xA3C2_0000), 1);
    }

    #[test]
    fn common_prefix_bytes_returns_2_for_third_byte_diff() {
        // Same first two bytes, different third byte.
        assert_eq!(common_prefix_bytes(0xA3B2_C100, 0xA3B2_C200), 2);
    }

    #[test]
    fn common_prefix_bytes_returns_3_for_fourth_byte_diff() {
        // Same first three bytes, different last byte.
        assert_eq!(common_prefix_bytes(0xA3B2_C101, 0xA3B2_C102), 3);
    }

    #[test]
    fn common_prefix_bytes_returns_4_for_identical() {
        assert_eq!(common_prefix_bytes(0xA3B2_C1D4, 0xA3B2_C1D4), 4);
    }

    // -----------------------------------------------------------------------
    // infer_wheel_positions tests
    // -----------------------------------------------------------------------

    #[test]
    fn infer_wheel_positions_consecutive_trailing_bytes() {
        let ids = [0xA3B2C100, 0xA3B2C101, 0xA3B2C102, 0xA3B2C103];
        let result = infer_wheel_positions(&ids).unwrap();
        assert_eq!(result[&0xA3B2C100], WheelPosition::FL);
        assert_eq!(result[&0xA3B2C101], WheelPosition::FR);
        assert_eq!(result[&0xA3B2C102], WheelPosition::RL);
        assert_eq!(result[&0xA3B2C103], WheelPosition::RR);
    }

    #[test]
    fn infer_wheel_positions_shuffled_order() {
        // IDs in non-sorted order; trailing bytes 0x05..0x08 are consecutive.
        let ids = [0xA3B2C107, 0xA3B2C105, 0xA3B2C108, 0xA3B2C106];
        let result = infer_wheel_positions(&ids).unwrap();
        assert_eq!(result[&0xA3B2C105], WheelPosition::FL);
        assert_eq!(result[&0xA3B2C106], WheelPosition::FR);
        assert_eq!(result[&0xA3B2C107], WheelPosition::RL);
        assert_eq!(result[&0xA3B2C108], WheelPosition::RR);
    }

    #[test]
    fn infer_wheel_positions_non_consecutive_returns_none() {
        let ids = [0xA3B2C100, 0xA3B2C102, 0xA3B2C104, 0xA3B2C106];
        assert!(infer_wheel_positions(&ids).is_none());
    }

    #[test]
    fn infer_wheel_positions_wrong_group_size_returns_none() {
        let ids = [0xA3B2C100, 0xA3B2C101, 0xA3B2C102];
        assert!(infer_wheel_positions(&ids).is_none());
    }

    // -----------------------------------------------------------------------
    // candidate_wheel_mates tests
    // -----------------------------------------------------------------------

    #[test]
    fn candidate_wheel_mates_filters_by_prefix() {
        let target = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 298,
            fixed_sensor_id: Some(0xA3B2C100),
        };
        let mate = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 298,
            fixed_sensor_id: Some(0xA3B2C201),
        };
        let stranger = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 298,
            fixed_sensor_id: Some(0xFF00_0000),
        };
        let all = vec![
            VehicleMeta { vehicle_id: target.vehicle_id, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C100) },
            VehicleMeta { vehicle_id: mate.vehicle_id, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C201) },
            VehicleMeta { vehicle_id: stranger.vehicle_id, rtl433_id: 298, fixed_sensor_id: Some(0xFF00_0000) },
        ];
        let result = candidate_wheel_mates(&target, &all);
        assert!(result.contains(&mate.vehicle_id));
        assert!(!result.contains(&stranger.vehicle_id));
        assert!(!result.contains(&target.vehicle_id));
    }

    #[test]
    fn candidate_wheel_mates_skips_rolling_id() {
        let target = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 208,
            fixed_sensor_id: None,
        };
        let all = vec![
            VehicleMeta { vehicle_id: Uuid::new_v4(), rtl433_id: 208, fixed_sensor_id: None },
        ];
        let result = candidate_wheel_mates(&target, &all);
        assert!(result.is_empty());
    }

    #[test]
    fn candidate_wheel_mates_different_protocol_not_matched() {
        let target = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 298,
            fixed_sensor_id: Some(0xA3B2C100),
        };
        let other = VehicleMeta {
            vehicle_id: Uuid::new_v4(),
            rtl433_id: 140,
            fixed_sensor_id: Some(0xA3B2C200),
        };
        let all = vec![
            VehicleMeta { vehicle_id: target.vehicle_id, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C100) },
            VehicleMeta { vehicle_id: other.vehicle_id, rtl433_id: 140, fixed_sensor_id: Some(0xA3B2C200) },
        ];
        let result = candidate_wheel_mates(&target, &all);
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // Prefix-seeded grouping tests
    // -----------------------------------------------------------------------

    #[test]
    fn prefix_seeded_grouping_groups_same_prefix_sensors() {
        // 4 fixed-ID sensors sharing a 3-byte prefix should be pre-grouped
        // even with an empty co-occurrence matrix (zero Jaccard windows).
        let m = CoOccurrenceMatrix::new();
        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let v3 = Uuid::new_v4();
        let v4 = Uuid::new_v4();
        let outsider = Uuid::new_v4();

        let ids = vec![v1, v2, v3, v4, outsider];
        let meta = vec![
            VehicleMeta { vehicle_id: v1, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C100) },
            VehicleMeta { vehicle_id: v2, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C101) },
            VehicleMeta { vehicle_id: v3, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C102) },
            VehicleMeta { vehicle_id: v4, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C103) },
            VehicleMeta { vehicle_id: outsider, rtl433_id: 298, fixed_sensor_id: Some(0xFF000001) },
        ];
        let groups = group_vehicles_into_cars_with_meta(&m, &ids, &meta);

        let car = groups.iter().find(|g| g.contains(v1)).unwrap();
        assert!(car.contains(v2));
        assert!(car.contains(v3));
        assert!(car.contains(v4));
        assert_eq!(car.wheel_count(), 4);
        assert!(!car.contains(outsider));
    }

    #[test]
    fn prefix_seeded_grouping_candidate_reduction() {
        // Fixture: 25 fixed-ID sensors of the same protocol.
        // 4 share a prefix → candidates for one target should be ≤ 3 (not 24).
        let target_id = Uuid::new_v4();
        let mut meta: Vec<VehicleMeta> = vec![
            VehicleMeta { vehicle_id: target_id, rtl433_id: 298, fixed_sensor_id: Some(0xA3B2C100) },
        ];
        // 3 peers with the same prefix.
        for i in 1..=3u32 {
            meta.push(VehicleMeta {
                vehicle_id: Uuid::new_v4(),
                rtl433_id: 298,
                fixed_sensor_id: Some(0xA3B2C100 + i),
            });
        }
        // 21 other sensors with different prefixes.
        for i in 0..21u32 {
            meta.push(VehicleMeta {
                vehicle_id: Uuid::new_v4(),
                rtl433_id: 298,
                fixed_sensor_id: Some(0xDD000000 + i * 0x0100_0000),
            });
        }
        let target = &meta[0];
        let mates = candidate_wheel_mates(target, &meta);
        // Should find exactly 3 candidates (the peers), not all 24.
        assert_eq!(mates.len(), 3);
        // That is a reduction of at least 80% from the 24 non-target vehicles.
        assert!(mates.len() as f32 / 24.0 <= 0.2);
    }

    #[test]
    fn wheel_position_round_trip() {
        for pos in &[WheelPosition::FL, WheelPosition::FR, WheelPosition::RL, WheelPosition::RR] {
            let s = pos.as_str();
            let parsed = WheelPosition::from_str(s).unwrap();
            assert_eq!(*pos, parsed);
        }
    }
}
