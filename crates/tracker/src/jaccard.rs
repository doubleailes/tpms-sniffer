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
    let mut groups: Vec<CarGroup> = vec![];
    let mut ungrouped: HashSet<Uuid> = vehicle_ids.iter().copied().collect();

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
    /// Produce a serialisable export of all non-zero Jaccard pairs.
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

        // Should be serialisable to JSON.
        let json = serde_json::to_string_pretty(&export).unwrap();
        assert!(json.contains("jaccard"));
    }
}
