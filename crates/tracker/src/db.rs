use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::collections::VecDeque;
use uuid::Uuid;

use crate::{Sighting, VehicleTrack};

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS vehicles (
                vehicle_id     TEXT PRIMARY KEY,
                first_seen     TEXT NOT NULL,
                last_seen      TEXT NOT NULL,
                sighting_count INTEGER NOT NULL DEFAULT 0,
                protocol       TEXT NOT NULL,
                rtl433_id      INTEGER NOT NULL DEFAULT 0,
                sensor_id      INTEGER,        -- NULL for rolling-ID protocols
                make_model     TEXT,
                pressure_sig   TEXT NOT NULL DEFAULT '[0,0,0,0]'
            );

            CREATE TABLE IF NOT EXISTS sightings (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id   TEXT NOT NULL REFERENCES vehicles(vehicle_id),
                ts           TEXT NOT NULL,
                protocol     TEXT NOT NULL,
                sensor_id    INTEGER NOT NULL,
                pressure_kpa REAL NOT NULL,
                temp_c       REAL,            -- NULL for sentinel values (≥200°C)
                alarm        INTEGER NOT NULL DEFAULT 0,
                battery_ok   INTEGER NOT NULL DEFAULT 1,
                lat          REAL,            -- NULL unless GPS is attached
                lon          REAL
            );

            CREATE INDEX IF NOT EXISTS idx_sightings_vehicle ON sightings(vehicle_id, ts);
            CREATE INDEX IF NOT EXISTS idx_sightings_ts      ON sightings(ts);
            "#,
        )?;

        // Migration: add rtl433_id column for databases created before the
        // cross-protocol-absorption fix. SQLite has no `ADD COLUMN IF NOT
        // EXISTS`, so check pragma_table_info first.
        let has_rtl433_id: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('vehicles') WHERE name='rtl433_id'",
            [],
            |row| row.get(0),
        )?;
        if has_rtl433_id == 0 {
            self.conn.execute(
                "ALTER TABLE vehicles ADD COLUMN rtl433_id INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        // Migration: add tx_interval columns for the tx-interval fingerprint
        // feature.  `tx_interval_median_ms` is nullable (NULL until enough
        // samples); `tx_interval_samples` defaults to 0.
        let has_tx_interval: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('vehicles') WHERE name='tx_interval_median_ms'",
            [],
            |row| row.get(0),
        )?;
        if has_tx_interval == 0 {
            self.conn.execute(
                "ALTER TABLE vehicles ADD COLUMN tx_interval_median_ms INTEGER",
                [],
            )?;
            self.conn.execute(
                "ALTER TABLE vehicles ADD COLUMN tx_interval_samples INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        Ok(())
    }

    /// Insert or update a vehicle row.  `first_seen`, `protocol`, `sensor_id`,
    /// and `make_model` are written only on first insert; subsequent updates
    /// refresh `last_seen`, `sighting_count`, and `pressure_sig`.
    pub fn upsert_vehicle(&self, v: &VehicleTrack) -> Result<()> {
        let pressure_sig = serde_json::to_string(&v.pressure_signature)?;
        self.conn.execute(
            r#"
            INSERT INTO vehicles
                (vehicle_id, first_seen, last_seen, sighting_count, protocol, rtl433_id, sensor_id, make_model, pressure_sig,
                 tx_interval_median_ms, tx_interval_samples)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(vehicle_id) DO UPDATE SET
                last_seen      = excluded.last_seen,
                sighting_count = excluded.sighting_count,
                pressure_sig   = excluded.pressure_sig,
                rtl433_id      = CASE WHEN vehicles.rtl433_id = 0 THEN excluded.rtl433_id ELSE vehicles.rtl433_id END,
                protocol       = CASE WHEN vehicles.rtl433_id = 0 THEN excluded.protocol   ELSE vehicles.protocol   END,
                tx_interval_median_ms = excluded.tx_interval_median_ms,
                tx_interval_samples   = excluded.tx_interval_samples
            "#,
            params![
                v.vehicle_id.to_string(),
                v.first_seen.to_rfc3339(),
                v.last_seen.to_rfc3339(),
                v.sighting_count as i64,
                v.protocol,
                v.rtl433_id as i64,
                v.fixed_sensor_id.map(|id| id as i64),
                v.make_model_hint.as_deref(),
                pressure_sig,
                v.tx_interval_median_ms.map(|ms| ms as i64),
                v.tx_intervals_ms.len() as i64,
            ],
        )?;
        Ok(())
    }

    /// Append one sighting row linked to `vehicle_id`.
    pub fn insert_sighting(&self, s: &Sighting, vehicle_id: Uuid) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sightings
                (vehicle_id, ts, protocol, sensor_id, pressure_kpa, temp_c, alarm, battery_ok)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                vehicle_id.to_string(),
                s.ts.to_rfc3339(),
                s.protocol,
                s.sensor_id as i64,
                s.pressure_kpa,
                s.temp_c,
                s.alarm as i64,
                s.battery_ok as i64,
            ],
        )?;
        Ok(())
    }

    /// Update the `rtl433_id` column for an existing vehicle row.
    /// Used when a legacy row (migrated with `rtl433_id=0`) is adopted by a
    /// packet carrying the real decoder ID, so the stored key is corrected
    /// in place rather than creating a duplicate vehicle.
    pub fn update_rtl433_id(&self, vehicle_id: Uuid, rtl433_id: u16) -> Result<()> {
        self.conn.execute(
            "UPDATE vehicles SET rtl433_id = ?1 WHERE vehicle_id = ?2",
            params![rtl433_id as i64, vehicle_id.to_string()],
        )?;
        Ok(())
    }

    /// Look up a vehicle by its fixed sensor_id.
    pub fn find_vehicle_by_sensor_id(&self, sensor_id: u32) -> Result<Option<VehicleTrack>> {
        let mut stmt = self.conn.prepare(
            "SELECT vehicle_id, first_seen, last_seen, sighting_count, protocol,
                    sensor_id, make_model, pressure_sig, rtl433_id,
                    tx_interval_median_ms, tx_interval_samples
             FROM vehicles WHERE sensor_id = ?1 LIMIT 1",
        )?;
        let mut rows = stmt.query(params![sensor_id as i64])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row_to_vehicle(row)?))
        } else {
            Ok(None)
        }
    }

    /// Return all vehicles ordered by most-recently-seen first.
    pub fn all_vehicles(&self) -> Result<Vec<VehicleTrack>> {
        let mut stmt = self.conn.prepare(
            "SELECT vehicle_id, first_seen, last_seen, sighting_count, protocol,
                    sensor_id, make_model, pressure_sig, rtl433_id,
                    tx_interval_median_ms, tx_interval_samples
             FROM vehicles ORDER BY last_seen DESC",
        )?;
        let vehicles = stmt
            .query_map([], row_to_vehicle)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(vehicles)
    }
}

fn row_to_vehicle(row: &rusqlite::Row<'_>) -> rusqlite::Result<VehicleTrack> {
    let vid: String = row.get(0)?;
    let first_seen_s: String = row.get(1)?;
    let last_seen_s: String = row.get(2)?;
    let sighting_count: i64 = row.get(3)?;
    let protocol: String = row.get(4)?;
    let sensor_id: Option<i64> = row.get(5)?;
    let make_model: Option<String> = row.get(6)?;
    let pressure_sig_s: String = row.get(7)?;
    let rtl433_id: i64 = row.get(8)?;
    let tx_interval_median: Option<i64> = row.get(9)?;
    let tx_interval_samples: i64 = row.get(10)?;

    let parse_dt = |s: &str| -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now())
    };

    // Reconstruct the in-memory tx_intervals_ms ring buffer: we don't persist
    // individual intervals, so after a DB reload the buffer is empty and the
    // median will be recalculated from live traffic.  We do carry over the
    // sample count / median so the DB row reflects the last-known state.
    let _ = tx_interval_samples; // acknowledged but not used for ring buffer

    Ok(VehicleTrack {
        vehicle_id: Uuid::parse_str(&vid).unwrap_or_else(|_| Uuid::nil()),
        first_seen: parse_dt(&first_seen_s),
        last_seen: parse_dt(&last_seen_s),
        sighting_count: sighting_count as u32,
        protocol,
        rtl433_id: u16::try_from(rtl433_id)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(8, rtl433_id))?,
        fixed_sensor_id: sensor_id.map(|id| id as u32),
        pressure_signature: serde_json::from_str(&pressure_sig_s).unwrap_or([0.0; 4]),
        make_model_hint: make_model,
        battery_ok: true,
        tx_intervals_ms: VecDeque::new(),
        tx_interval_median_ms: tx_interval_median.map(|ms| ms as u32),
    })
}
