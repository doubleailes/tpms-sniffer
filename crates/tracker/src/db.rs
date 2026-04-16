use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::collections::VecDeque;
use uuid::Uuid;

use crate::analytics::{
    self, CarReport, GeoSighting, PresenceSlot, PressureEvent, PressureReading, VehicleSummary,
};
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

        // Migration: add cars table and car_id column for Jaccard wheel
        // grouping.  The `cars` table stores aggregated car-level records; each
        // vehicle track may reference a car via `car_id`.
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cars (
                car_id      TEXT PRIMARY KEY,
                first_seen  TEXT,
                last_seen   TEXT,
                wheel_count INTEGER,
                make_model  TEXT,
                notes       TEXT
            );
            "#,
        )?;

        let has_car_id: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('vehicles') WHERE name='car_id'",
            [],
            |row| row.get(0),
        )?;
        if has_car_id == 0 {
            self.conn.execute(
                "ALTER TABLE vehicles ADD COLUMN car_id TEXT REFERENCES cars(car_id)",
                [],
            )?;
        }

        // Migration: add receiver_id column to sightings table and create the
        // receivers table for multi-node deployments.
        let has_receiver_id: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('sightings') WHERE name='receiver_id'",
            [],
            |row| row.get(0),
        )?;
        if has_receiver_id == 0 {
            self.conn.execute(
                "ALTER TABLE sightings ADD COLUMN receiver_id TEXT NOT NULL DEFAULT 'default'",
                [],
            )?;
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sightings_receiver ON sightings(receiver_id, ts)",
                [],
            )?;
        }

        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS receivers (
                receiver_id   TEXT PRIMARY KEY,
                first_seen    TEXT,
                last_seen     TEXT,
                lat           REAL,
                lon           REAL,
                notes         TEXT
            );
            "#,
        )?;

        // Migration: add presence_slots table for hourly behavioural analytics.
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS presence_slots (
                car_id         TEXT REFERENCES cars(car_id),
                slot_start     TEXT,
                slot_end       TEXT,
                sighting_count INTEGER,
                receiver_ids   TEXT,
                PRIMARY KEY (car_id, slot_start)
            );

            CREATE INDEX IF NOT EXISTS idx_presence_slots_car_ts
                ON presence_slots(car_id, slot_start);
            "#,
        )?;

        // Migration: add pressure_events table for trend analysis.
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS pressure_events (
                id          INTEGER PRIMARY KEY,
                car_id      TEXT REFERENCES cars(car_id),
                vehicle_id  TEXT REFERENCES vehicles(vehicle_id),
                event_type  TEXT,
                value_kpa   REAL,
                ts          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pressure_events_car_ts
                ON pressure_events(car_id, ts);
            "#,
        )?;

        // Migration: add wheel_position column for prefix-byte wheel inference.
        let has_wheel_position: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('vehicles') WHERE name='wheel_position'",
            [],
            |row| row.get(0),
        )?;
        if has_wheel_position == 0 {
            self.conn
                .execute("ALTER TABLE vehicles ADD COLUMN wheel_position TEXT", [])?;
        }

        // Migration: add vehicle_class column for pressure-based classification.
        let has_vehicle_class: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('vehicles') WHERE name='vehicle_class'",
            [],
            |row| row.get(0),
        )?;
        if has_vehicle_class == 0 {
            self.conn.execute(
                "ALTER TABLE vehicles ADD COLUMN vehicle_class TEXT NOT NULL DEFAULT 'Unknown'",
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
                 tx_interval_median_ms, tx_interval_samples, car_id, wheel_position, vehicle_class)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(vehicle_id) DO UPDATE SET
                last_seen      = excluded.last_seen,
                sighting_count = excluded.sighting_count,
                pressure_sig   = excluded.pressure_sig,
                rtl433_id      = CASE WHEN vehicles.rtl433_id = 0 THEN excluded.rtl433_id ELSE vehicles.rtl433_id END,
                protocol       = CASE WHEN vehicles.rtl433_id = 0 THEN excluded.protocol   ELSE vehicles.protocol   END,
                tx_interval_median_ms = excluded.tx_interval_median_ms,
                tx_interval_samples   = excluded.tx_interval_samples,
                car_id                = COALESCE(excluded.car_id, vehicles.car_id),
                wheel_position        = COALESCE(excluded.wheel_position, vehicles.wheel_position),
                vehicle_class         = excluded.vehicle_class
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
                v.car_id.map(|id| id.to_string()),
                v.wheel_position.map(|wp| wp.as_str().to_string()),
                v.vehicle_class.as_str(),
            ],
        )?;
        Ok(())
    }

    /// Append one sighting row linked to `vehicle_id`.
    pub fn insert_sighting(&self, s: &Sighting, vehicle_id: Uuid) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sightings
                (vehicle_id, ts, protocol, sensor_id, pressure_kpa, temp_c, alarm, battery_ok, receiver_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
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
                s.receiver_id,
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
                    tx_interval_median_ms, tx_interval_samples, car_id, wheel_position,
                    vehicle_class
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
                    tx_interval_median_ms, tx_interval_samples, car_id, wheel_position,
                    vehicle_class
             FROM vehicles ORDER BY last_seen DESC",
        )?;
        let vehicles = stmt
            .query_map([], row_to_vehicle)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(vehicles)
    }

    /// Upsert a car group record.
    pub fn upsert_car(
        &self,
        car_id: Uuid,
        first_seen: &str,
        last_seen: &str,
        wheel_count: usize,
        make_model: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cars (car_id, first_seen, last_seen, wheel_count, make_model)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(car_id) DO UPDATE SET
                last_seen   = excluded.last_seen,
                wheel_count = excluded.wheel_count,
                make_model  = COALESCE(excluded.make_model, cars.make_model)
            "#,
            params![
                car_id.to_string(),
                first_seen,
                last_seen,
                wheel_count as i64,
                make_model,
            ],
        )?;
        Ok(())
    }

    /// Set the `car_id` of a vehicle.
    pub fn set_vehicle_car_id(&self, vehicle_id: Uuid, car_id: Uuid) -> Result<()> {
        self.conn.execute(
            "UPDATE vehicles SET car_id = ?1 WHERE vehicle_id = ?2",
            params![car_id.to_string(), vehicle_id.to_string()],
        )?;
        Ok(())
    }

    /// Reassign all vehicles that reference `discard` to point to `keep`
    /// instead.  Used when merging two CarGroups.
    pub fn reassign_vehicles_car_id(&self, discard: Uuid, keep: Uuid) -> Result<u64> {
        let affected = self.conn.execute(
            "UPDATE vehicles SET car_id = ?1 WHERE car_id = ?2",
            params![keep.to_string(), discard.to_string()],
        )?;
        Ok(affected as u64)
    }

    /// Delete a car record from the `cars` table.
    pub fn delete_car(&self, car_id: Uuid) -> Result<()> {
        self.conn.execute(
            "DELETE FROM cars WHERE car_id = ?1",
            params![car_id.to_string()],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Presence-slot analytics
    // -----------------------------------------------------------------------

    /// Upsert an hourly presence slot for a car.  Called after each sighting
    /// when the car_id is known.
    pub fn upsert_presence_slot(
        &self,
        car_id: &str,
        ts: &DateTime<Utc>,
        receiver_id: &str,
    ) -> Result<()> {
        let slot_start = ts.format("%Y-%m-%dT%H:00:00+00:00").to_string();
        let slot_end = (*ts + chrono::Duration::hours(1))
            .format("%Y-%m-%dT%H:00:00+00:00")
            .to_string();

        // Check if slot already exists to merge receiver_ids.
        let existing: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT sighting_count, receiver_ids FROM presence_slots WHERE car_id = ?1 AND slot_start = ?2",
                params![car_id, slot_start],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        if let Some((count, existing_receivers)) = existing {
            let mut receivers: Vec<String> =
                serde_json::from_str(&existing_receivers).unwrap_or_default();
            if !receivers.contains(&receiver_id.to_string()) {
                receivers.push(receiver_id.to_string());
            }
            let receivers_json = serde_json::to_string(&receivers)?;
            self.conn.execute(
                r#"
                UPDATE presence_slots
                SET sighting_count = ?1, receiver_ids = ?2
                WHERE car_id = ?3 AND slot_start = ?4
                "#,
                params![count + 1, receivers_json, car_id, slot_start],
            )?;
        } else {
            let receivers_json = serde_json::to_string(&[receiver_id])?;
            self.conn.execute(
                r#"
                INSERT INTO presence_slots (car_id, slot_start, slot_end, sighting_count, receiver_ids)
                VALUES (?1, ?2, ?3, 1, ?4)
                "#,
                params![car_id, slot_start, slot_end, receivers_json],
            )?;
        }
        Ok(())
    }

    /// Backfill `presence_slots` from existing sighting data.
    /// This processes all sightings that have an associated car_id (via the
    /// vehicles table) and inserts hourly slots.
    pub fn backfill_presence_slots(&self) -> Result<u64> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT v.car_id, s.ts, s.receiver_id
            FROM sightings s
            JOIN vehicles v ON s.vehicle_id = v.vehicle_id
            WHERE v.car_id IS NOT NULL
            ORDER BY s.ts
            "#,
        )?;
        let mut count = 0u64;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let car_id: String = row.get(0)?;
            let ts_s: String = row.get(1)?;
            let receiver_id: String = row.get(2)?;
            if let Ok(dt) = DateTime::parse_from_rfc3339(&ts_s) {
                let utc = dt.with_timezone(&Utc);
                // Drop the borrow on `rows` by using direct SQL here to avoid
                // borrow issues with self.
                let slot_start = utc.format("%Y-%m-%dT%H:00:00+00:00").to_string();
                let slot_end = (utc + chrono::Duration::hours(1))
                    .format("%Y-%m-%dT%H:00:00+00:00")
                    .to_string();

                let receivers_json = serde_json::to_string(&[&receiver_id])?;
                self.conn.execute(
                    r#"
                    INSERT INTO presence_slots (car_id, slot_start, slot_end, sighting_count, receiver_ids)
                    VALUES (?1, ?2, ?3, 1, ?4)
                    ON CONFLICT(car_id, slot_start) DO UPDATE SET
                        sighting_count = presence_slots.sighting_count + 1,
                        receiver_ids   = CASE
                            WHEN INSTR(presence_slots.receiver_ids, ?5) > 0
                            THEN presence_slots.receiver_ids
                            ELSE JSON_INSERT(presence_slots.receiver_ids, '$[#]', ?5)
                        END
                    "#,
                    params![car_id, slot_start, slot_end, receivers_json, receiver_id],
                )?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Get all presence slots for a car, optionally filtered by date range.
    pub fn get_presence_slots(
        &self,
        car_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<PresenceSlot>> {
        let map_row = |row: &rusqlite::Row<'_>| {
            let receiver_ids_s: String = row.get(4)?;
            let receiver_ids: Vec<String> =
                serde_json::from_str(&receiver_ids_s).unwrap_or_default();
            Ok(PresenceSlot {
                car_id: row.get(0)?,
                slot_start: row.get(1)?,
                slot_end: row.get(2)?,
                sighting_count: row.get(3)?,
                receiver_ids,
            })
        };

        let slots = match (from, to) {
            (Some(f), Some(t)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT car_id, slot_start, slot_end, sighting_count, receiver_ids \
                     FROM presence_slots WHERE car_id = ?1 AND slot_start >= ?2 AND slot_start <= ?3 \
                     ORDER BY slot_start",
                )?;
                stmt.query_map(params![car_id, f, t], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(f), None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT car_id, slot_start, slot_end, sighting_count, receiver_ids \
                     FROM presence_slots WHERE car_id = ?1 AND slot_start >= ?2 \
                     ORDER BY slot_start",
                )?;
                stmt.query_map(params![car_id, f], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(t)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT car_id, slot_start, slot_end, sighting_count, receiver_ids \
                     FROM presence_slots WHERE car_id = ?1 AND slot_start <= ?2 \
                     ORDER BY slot_start",
                )?;
                stmt.query_map(params![car_id, t], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT car_id, slot_start, slot_end, sighting_count, receiver_ids \
                     FROM presence_slots WHERE car_id = ?1 ORDER BY slot_start",
                )?;
                stmt.query_map(params![car_id], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };
        Ok(slots)
    }

    // -----------------------------------------------------------------------
    // Pressure event analytics
    // -----------------------------------------------------------------------

    /// Get pressure readings for a specific vehicle, ordered by timestamp.
    pub fn get_pressure_readings(
        &self,
        vehicle_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<PressureReading>> {
        let map_row = |row: &rusqlite::Row<'_>| {
            let ts_s: String = row.get(0)?;
            let pressure_kpa: f64 = row.get(1)?;
            let alarm: i64 = row.get(2)?;
            let ts = DateTime::parse_from_rfc3339(&ts_s)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            Ok(PressureReading {
                ts,
                pressure_kpa: pressure_kpa as f32,
                alarm: alarm != 0,
            })
        };

        let readings = match (from, to) {
            (Some(f), Some(t)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT ts, pressure_kpa, alarm FROM sightings \
                     WHERE vehicle_id = ?1 AND ts >= ?2 AND ts <= ?3 ORDER BY ts",
                )?;
                stmt.query_map(params![vehicle_id, f, t], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(f), None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT ts, pressure_kpa, alarm FROM sightings \
                     WHERE vehicle_id = ?1 AND ts >= ?2 ORDER BY ts",
                )?;
                stmt.query_map(params![vehicle_id, f], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(t)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT ts, pressure_kpa, alarm FROM sightings \
                     WHERE vehicle_id = ?1 AND ts <= ?2 ORDER BY ts",
                )?;
                stmt.query_map(params![vehicle_id, t], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT ts, pressure_kpa, alarm FROM sightings \
                     WHERE vehicle_id = ?1 ORDER BY ts",
                )?;
                stmt.query_map(params![vehicle_id], map_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };
        Ok(readings)
    }

    /// Insert a pressure event record.
    pub fn insert_pressure_event(&self, event: &PressureEvent) -> Result<()> {
        let (car_id, vehicle_id, event_type, value_kpa, ts) = match event {
            PressureEvent::SlowDecline {
                car_id,
                vehicle_id,
                rate_kpa_per_day,
                ts,
            } => (car_id, vehicle_id, "slow_decline", *rate_kpa_per_day, ts),
            PressureEvent::SuddenIncrease {
                car_id,
                vehicle_id,
                delta_kpa,
                ts,
            } => (car_id, vehicle_id, "sudden_increase", *delta_kpa, ts),
            PressureEvent::AlarmThreshold {
                car_id,
                vehicle_id,
                pressure_kpa,
                ts,
            } => (car_id, vehicle_id, "alarm", *pressure_kpa, ts),
        };
        self.conn.execute(
            r#"
            INSERT INTO pressure_events (car_id, vehicle_id, event_type, value_kpa, ts)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![car_id, vehicle_id, event_type, value_kpa as f64, ts],
        )?;
        Ok(())
    }

    /// Get all pressure events for a car.
    pub fn get_pressure_events(&self, car_id: &str) -> Result<Vec<PressureEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT car_id, vehicle_id, event_type, value_kpa, ts \
             FROM pressure_events WHERE car_id = ?1 ORDER BY ts",
        )?;
        let events = stmt
            .query_map(params![car_id], |row| {
                let car_id: String = row.get(0)?;
                let vehicle_id: String = row.get(1)?;
                let event_type: String = row.get(2)?;
                let value_kpa: f64 = row.get(3)?;
                let ts: String = row.get(4)?;
                Ok(match event_type.as_str() {
                    "slow_decline" => PressureEvent::SlowDecline {
                        car_id,
                        vehicle_id,
                        rate_kpa_per_day: value_kpa as f32,
                        ts,
                    },
                    "sudden_increase" => PressureEvent::SuddenIncrease {
                        car_id,
                        vehicle_id,
                        delta_kpa: value_kpa as f32,
                        ts,
                    },
                    _ => PressureEvent::AlarmThreshold {
                        car_id,
                        vehicle_id,
                        pressure_kpa: value_kpa as f32,
                        ts,
                    },
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(events)
    }

    // -----------------------------------------------------------------------
    // GeoJSON export
    // -----------------------------------------------------------------------

    /// Get all sightings with GPS coordinates for GeoJSON export.
    pub fn get_geo_sightings(&self, car_id: Option<&str>) -> Result<Vec<GeoSighting>> {
        let map_row = |row: &rusqlite::Row<'_>| {
            let car_id: String = row.get(0)?;
            let ts_s: String = row.get(1)?;
            let lat: f64 = row.get(2)?;
            let lon: f64 = row.get(3)?;
            let ts = DateTime::parse_from_rfc3339(&ts_s)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            Ok(GeoSighting {
                car_id,
                ts,
                lat,
                lon,
            })
        };

        let sightings = if let Some(cid) = car_id {
            let mut stmt = self.conn.prepare(
                "SELECT v.car_id, s.ts, s.lat, s.lon \
                 FROM sightings s JOIN vehicles v ON s.vehicle_id = v.vehicle_id \
                 WHERE v.car_id = ?1 AND s.lat IS NOT NULL AND s.lon IS NOT NULL \
                 ORDER BY s.ts",
            )?;
            stmt.query_map(params![cid], map_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT v.car_id, s.ts, s.lat, s.lon \
                 FROM sightings s JOIN vehicles v ON s.vehicle_id = v.vehicle_id \
                 WHERE v.car_id IS NOT NULL AND s.lat IS NOT NULL AND s.lon IS NOT NULL \
                 ORDER BY s.ts",
            )?;
            stmt.query_map([], map_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(sightings)
    }

    // -----------------------------------------------------------------------
    // Report generation
    // -----------------------------------------------------------------------

    /// Generate a full report for a car.
    pub fn generate_car_report(
        &self,
        car_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<CarReport> {
        // Get car metadata.
        let car_meta: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT first_seen, last_seen FROM cars WHERE car_id = ?1",
                params![car_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        // Get vehicles belonging to this car.
        let mut stmt = self.conn.prepare(
            "SELECT vehicle_id, protocol, make_model, pressure_sig, wheel_position, vehicle_class \
             FROM vehicles WHERE car_id = ?1",
        )?;
        let vehicles: Vec<VehicleSummary> = stmt
            .query_map(params![car_id], |row| {
                let vehicle_id: String = row.get(0)?;
                let protocol: String = row.get(1)?;
                let make_model: Option<String> = row.get(2)?;
                let pressure_sig_s: String = row.get(3)?;
                let wheel_position: Option<String> = row.get(4)?;
                let vehicle_class: Option<String> = row.get(5)?;
                let sig: [f32; 4] = serde_json::from_str(&pressure_sig_s).unwrap_or([0.0; 4]);
                let avg = sig.iter().filter(|&&p| p > 0.0).sum::<f32>()
                    / sig.iter().filter(|&&p| p > 0.0).count().max(1) as f32;
                Ok(VehicleSummary {
                    vehicle_id,
                    protocol,
                    make_model,
                    avg_pressure_kpa: avg,
                    wheel_position,
                    vehicle_class: vehicle_class.unwrap_or_else(|| "Unknown".to_string()),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        // Total sighting count via subquery on car_id.
        let total_sessions: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sightings WHERE vehicle_id IN \
             (SELECT vehicle_id FROM vehicles WHERE car_id = ?1)",
            params![car_id],
            |row| row.get(0),
        )?;

        // Presence slots for routine.
        let slots = self.get_presence_slots(car_id, from, to)?;
        let routine = analytics::compute_routine(car_id, &slots);

        // Pressure events.
        let mut pressure_events: Vec<PressureEvent> = Vec::new();
        for v in &vehicles {
            let readings = self.get_pressure_readings(&v.vehicle_id, from, to)?;
            let events = analytics::detect_pressure_events(car_id, &v.vehicle_id, &readings);
            pressure_events.extend(events);
        }

        Ok(CarReport {
            car_id: car_id.to_string(),
            first_seen: car_meta.as_ref().map(|(f, _)| f.clone()),
            last_seen: car_meta.as_ref().map(|(_, l)| l.clone()),
            total_sessions,
            routine,
            pressure_events,
            vehicles,
        })
    }

    /// List all car IDs in the database.
    pub fn all_car_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT car_id FROM cars ORDER BY last_seen DESC")?;
        let ids = stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(ids)
    }

    // -----------------------------------------------------------------------
    // Web API helpers
    // -----------------------------------------------------------------------

    /// Aggregate stats for the dashboard `/api/stats` endpoint.
    pub fn api_stats(&self) -> Result<crate::server::ApiStats> {
        let vehicle_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM vehicles", [], |r| r.get(0))?;
        let car_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM cars", [], |r| r.get(0))?;
        let active_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM vehicles WHERE last_seen >= datetime('now', '-5 minutes')",
            [],
            |r| r.get(0),
        )?;
        let alarm_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sightings s \
             INNER JOIN ( \
                 SELECT vehicle_id, MAX(id) AS max_id FROM sightings GROUP BY vehicle_id \
             ) latest ON s.id = latest.max_id \
             WHERE s.alarm = 1",
            [],
            |r| r.get(0),
        )?;
        let grouped_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM vehicles WHERE car_id IS NOT NULL",
            [],
            |r| r.get(0),
        )?;
        let pending_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM vehicles WHERE car_id IS NULL",
            [],
            |r| r.get(0),
        )?;
        let last_packet_ts: Option<String> = self
            .conn
            .query_row("SELECT MAX(last_seen) FROM vehicles", [], |r| r.get(0))
            .ok()
            .flatten();

        Ok(crate::server::ApiStats {
            vehicle_count,
            car_count,
            active_count,
            alarm_count,
            grouped_count,
            pending_count,
            last_packet_ts,
        })
    }

    /// Vehicle list for `/api/cars` with optional filter and search.
    pub fn api_vehicles(
        &self,
        filter: Option<&str>,
        search: Option<&str>,
    ) -> Result<Vec<crate::server::VehicleRow>> {
        // Build the query dynamically based on filter/search.
        let mut sql = String::from(
            "SELECT v.vehicle_id, v.car_id, v.protocol, v.rtl433_id, \
                    v.vehicle_class, v.last_seen, v.first_seen, v.sighting_count, \
                    v.tx_interval_median_ms, v.pressure_sig, \
                    CASE WHEN v.last_seen >= datetime('now', '-5 minutes') THEN 1 ELSE 0 END AS active \
             FROM vehicles v",
        );

        let mut conditions: Vec<String> = Vec::new();

        match filter {
            Some("active") => {
                conditions.push("v.last_seen >= datetime('now', '-5 minutes')".to_string());
            }
            Some("alarm") => {
                // Join to latest sighting to check alarm status.
                sql = String::from(
                    "SELECT v.vehicle_id, v.car_id, v.protocol, v.rtl433_id, \
                            v.vehicle_class, v.last_seen, v.first_seen, v.sighting_count, \
                            v.tx_interval_median_ms, v.pressure_sig, \
                            CASE WHEN v.last_seen >= datetime('now', '-5 minutes') THEN 1 ELSE 0 END AS active \
                     FROM vehicles v \
                     INNER JOIN ( \
                         SELECT vehicle_id, MAX(id) AS max_id FROM sightings GROUP BY vehicle_id \
                     ) latest ON v.vehicle_id = latest.vehicle_id \
                     INNER JOIN sightings s ON s.id = latest.max_id",
                );
                conditions.push("s.alarm = 1".to_string());
            }
            Some("grouped") => {
                conditions.push("v.car_id IS NOT NULL".to_string());
            }
            _ => {}
        }

        if let Some(q) = search {
            if !q.is_empty() {
                conditions.push(format!(
                    "(LOWER(v.protocol) LIKE '%{}%' OR LOWER(COALESCE(v.car_id,'')) LIKE '%{}%')",
                    q.to_lowercase().replace('\'', "''"),
                    q.to_lowercase().replace('\'', "''")
                ));
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY v.last_seen DESC");

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |row| {
                let vehicle_id: String = row.get(0)?;
                let car_id: Option<String> = row.get(1)?;
                let protocol: String = row.get(2)?;
                let rtl433_id: i64 = row.get(3)?;
                let vehicle_class: Option<String> = row.get(4)?;
                let last_seen: String = row.get(5)?;
                let first_seen: String = row.get(6)?;
                let sighting_count: i64 = row.get(7)?;
                let tx_interval_median_ms: Option<i64> = row.get(8)?;
                let pressure_sig_s: String = row.get(9)?;
                let active: i64 = row.get(10)?;

                // Extract first non-zero pressure from signature.
                let sig: [f64; 4] = serde_json::from_str(&pressure_sig_s).unwrap_or([0.0; 4]);
                let pressure_kpa = sig.iter().find(|&&p| p > 0.0).copied().unwrap_or(0.0);

                // Look up alarm and battery from latest sighting — done in Rust
                // to keep the SQL simpler for all filter paths.
                Ok(crate::server::VehicleRow {
                    vehicle_id,
                    car_id,
                    protocol,
                    rtl433_id,
                    vehicle_class: vehicle_class.unwrap_or_else(|| "Unknown".to_string()),
                    pressure_kpa,
                    alarm: false,     // patched below
                    battery_ok: true, // patched below
                    first_seen,
                    last_seen,
                    sighting_count,
                    tx_interval_median_ms,
                    active: active != 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        // Patch alarm / battery from latest sighting per vehicle.
        let mut result = rows;
        for v in &mut result {
            if let Ok(Some((alarm, battery))) = self.latest_alarm_battery(&v.vehicle_id) {
                v.alarm = alarm;
                v.battery_ok = battery;
            }
        }
        Ok(result)
    }

    /// Fetch alarm and battery_ok from the most recent sighting for a vehicle.
    fn latest_alarm_battery(&self, vehicle_id: &str) -> Result<Option<(bool, bool)>> {
        let result = self.conn.query_row(
            "SELECT alarm, battery_ok FROM sightings WHERE vehicle_id = ?1 ORDER BY id DESC LIMIT 1",
            rusqlite::params![vehicle_id],
            |row| {
                let alarm: i64 = row.get(0)?;
                let battery: i64 = row.get(1)?;
                Ok((alarm != 0, battery != 0))
            },
        );
        match result {
            Ok(pair) => Ok(Some(pair)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Car detail for `/api/cars/:car_id` including last 50 sightings.
    pub fn api_car_detail(&self, car_id: &str) -> Result<Option<crate::server::CarDetailResponse>> {
        // Fetch car metadata.
        let meta: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT first_seen, last_seen FROM cars WHERE car_id = ?1",
                rusqlite::params![car_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        if meta.is_none() {
            return Ok(None);
        }
        let (first_seen, last_seen) = meta.unwrap();

        // Member vehicles.
        let mut stmt = self
            .conn
            .prepare("SELECT vehicle_id, vehicle_class FROM vehicles WHERE car_id = ?1")?;
        let members: Vec<String> = stmt
            .query_map(rusqlite::params![car_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let vehicle_class: Option<String> = {
            let mut cls_stmt = self
                .conn
                .prepare("SELECT vehicle_class FROM vehicles WHERE car_id = ?1 LIMIT 1")?;
            cls_stmt
                .query_row(rusqlite::params![car_id], |row| row.get(0))
                .ok()
        };

        // Last 50 sightings across all member vehicles.
        let mut sight_stmt = self.conn.prepare(
            "SELECT s.ts, s.pressure_kpa, s.alarm, s.sensor_id, s.receiver_id \
             FROM sightings s \
             INNER JOIN vehicles v ON s.vehicle_id = v.vehicle_id \
             WHERE v.car_id = ?1 \
             ORDER BY s.ts DESC \
             LIMIT 50",
        )?;
        let sightings = sight_stmt
            .query_map(rusqlite::params![car_id], |row| {
                let ts: String = row.get(0)?;
                let pressure_kpa: f64 = row.get(1)?;
                let alarm_i: i64 = row.get(2)?;
                let sensor_id_i: i64 = row.get(3)?;
                let receiver_id: String = row.get(4)?;
                Ok(crate::server::SightingRow {
                    ts,
                    pressure_kpa,
                    alarm: alarm_i != 0,
                    sensor_id: format!("0x{:08X}", sensor_id_i),
                    receiver_id,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(Some(crate::server::CarDetailResponse {
            car_id: car_id.to_string(),
            first_seen: Some(first_seen),
            last_seen: Some(last_seen),
            vehicle_class,
            members,
            sightings,
        }))
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
    let _tx_interval_samples: i64 = row.get(10)?;
    let car_id_s: Option<String> = row.get(11)?;
    let wheel_position_s: Option<String> = row.get(12)?;
    let vehicle_class_s: Option<String> = row.get(13)?;

    let parse_dt = |s: &str| -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now())
    };

    let car_id_uuid = car_id_s.and_then(|s| Uuid::parse_str(&s).ok());

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
        car_id: car_id_uuid,
        receiver_sightings: HashMap::new(),
        wheel_position: wheel_position_s.and_then(|s| crate::jaccard::WheelPosition::from_str(&s)),
        vehicle_class: vehicle_class_s
            .map(|s| crate::classification::VehicleClass::from_str(&s))
            .unwrap_or(crate::classification::VehicleClass::Unknown),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sighting;
    use chrono::TimeZone;

    fn test_db() -> Database {
        Database::open(":memory:").expect("in-memory DB should open")
    }

    #[test]
    fn presence_slot_upsert_and_query() {
        let db = test_db();
        let car_id = Uuid::new_v4();
        db.upsert_car(
            car_id,
            "2026-04-13T08:00:00+00:00",
            "2026-04-13T10:00:00+00:00",
            4,
            None,
        )
        .unwrap();

        let ts1 = Utc.with_ymd_and_hms(2026, 4, 13, 8, 15, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 4, 13, 8, 45, 0).unwrap();
        let car_id_s = car_id.to_string();

        db.upsert_presence_slot(&car_id_s, &ts1, "node-01").unwrap();
        db.upsert_presence_slot(&car_id_s, &ts2, "node-02").unwrap();

        let slots = db.get_presence_slots(&car_id_s, None, None).unwrap();
        assert_eq!(slots.len(), 1, "same hour should merge into one slot");
        assert_eq!(slots[0].sighting_count, 2);
        assert_eq!(slots[0].receiver_ids.len(), 2);
    }

    #[test]
    fn backfill_presence_slots_populates_from_sightings() {
        let db = test_db();
        let vid = Uuid::new_v4();
        let car_id = Uuid::new_v4();

        // Create car first (FK constraint).
        db.upsert_car(
            car_id,
            "2026-04-13T08:00:00+00:00",
            "2026-04-13T10:00:00+00:00",
            1,
            None,
        )
        .unwrap();

        // Create vehicle and car.
        let vehicle = VehicleTrack {
            vehicle_id: vid,
            first_seen: Utc.with_ymd_and_hms(2026, 4, 13, 8, 0, 0).unwrap(),
            last_seen: Utc.with_ymd_and_hms(2026, 4, 13, 10, 0, 0).unwrap(),
            sighting_count: 2,
            protocol: "test".to_string(),
            rtl433_id: 59,
            fixed_sensor_id: Some(12345),
            pressure_signature: [230.0, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_id),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: crate::classification::VehicleClass::PassengerCar,
        };
        db.upsert_vehicle(&vehicle).unwrap();

        // Insert sightings.
        let sighting1 = Sighting {
            ts: Utc.with_ymd_and_hms(2026, 4, 13, 8, 0, 0).unwrap(),
            protocol: "test".to_string(),
            rtl433_id: 59,
            sensor_id: 12345,
            pressure_kpa: 230.0,
            temp_c: Some(25.0),
            alarm: false,
            battery_ok: true,
            pressure_reliable: true,
            tx_interval_hint_ms: None,
            receiver_id: "default".to_string(),
        };
        let sighting2 = Sighting {
            ts: Utc.with_ymd_and_hms(2026, 4, 13, 10, 0, 0).unwrap(),
            ..sighting1.clone()
        };
        db.insert_sighting(&sighting1, vid).unwrap();
        db.insert_sighting(&sighting2, vid).unwrap();

        let count = db.backfill_presence_slots().unwrap();
        assert_eq!(count, 2);

        let slots = db
            .get_presence_slots(&car_id.to_string(), None, None)
            .unwrap();
        assert_eq!(
            slots.len(),
            2,
            "two distinct hours should produce two slots"
        );
    }

    #[test]
    fn pressure_event_insert_and_query() {
        let db = test_db();
        let car_id = Uuid::new_v4();
        let vid = Uuid::new_v4();
        db.upsert_car(
            car_id,
            "2026-04-13T08:00:00+00:00",
            "2026-04-13T10:00:00+00:00",
            1,
            None,
        )
        .unwrap();
        // Create a vehicle so the FK to vehicles(vehicle_id) is satisfied.
        let vehicle = VehicleTrack {
            vehicle_id: vid,
            first_seen: Utc.with_ymd_and_hms(2026, 4, 13, 8, 0, 0).unwrap(),
            last_seen: Utc.with_ymd_and_hms(2026, 4, 13, 10, 0, 0).unwrap(),
            sighting_count: 1,
            protocol: "test".to_string(),
            rtl433_id: 59,
            fixed_sensor_id: Some(12345),
            pressure_signature: [230.0, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_id),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: crate::classification::VehicleClass::PassengerCar,
        };
        db.upsert_vehicle(&vehicle).unwrap();

        let event = PressureEvent::SuddenIncrease {
            car_id: car_id.to_string(),
            vehicle_id: vid.to_string(),
            delta_kpa: 15.0,
            ts: "2026-04-13T09:00:00+00:00".to_string(),
        };
        db.insert_pressure_event(&event).unwrap();

        let events = db.get_pressure_events(&car_id.to_string()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], PressureEvent::SuddenIncrease { delta_kpa, .. } if (*delta_kpa - 15.0).abs() < 0.1)
        );
    }

    #[test]
    fn generate_car_report_with_no_data() {
        let db = test_db();
        let car_id = Uuid::new_v4();
        db.upsert_car(
            car_id,
            "2026-04-13T08:00:00+00:00",
            "2026-04-13T10:00:00+00:00",
            1,
            None,
        )
        .unwrap();

        let report = db
            .generate_car_report(&car_id.to_string(), None, None)
            .unwrap();
        assert_eq!(report.car_id, car_id.to_string());
        assert_eq!(report.total_sessions, 0);
        assert!(report.routine.is_none());
    }

    #[test]
    fn schema_has_correct_indexes() {
        let db = test_db();
        // Verify presence_slots index exists.
        let count: i64 = db.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_presence_slots_car_ts'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1, "idx_presence_slots_car_ts index should exist");

        // Verify pressure_events index exists.
        let count: i64 = db.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_pressure_events_car_ts'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1, "idx_pressure_events_car_ts index should exist");
    }
}
