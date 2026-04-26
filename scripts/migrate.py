#!/usr/bin/env python3
"""
TPMS Tracker database migration script
Applies migrations 1-4 for temporal behavioural fingerprinting (issue #34)
 
Usage:
    python3 migrate.py tpms.db
    python3 migrate.py tpms.db --dry-run     # show what would happen, no changes
    python3 migrate.py tpms.db --step 1      # run a single step only
"""
 
import argparse
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
 
 
# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------
 
MIGRATION_V1_DESCRIPTION = "initial schema baseline"
 
MIGRATION_V2_DESCRIPTION = "clean artifact vehicles (pressure < 1.5 kPa)"
MIGRATION_V2 = """
PRAGMA foreign_keys=OFF;
DELETE FROM sightings WHERE pressure_kpa < 1.5;
DELETE FROM vehicles
    WHERE vehicle_id NOT IN (SELECT DISTINCT vehicle_id FROM sightings);
DELETE FROM cars
    WHERE car_id NOT IN (
        SELECT DISTINCT car_id FROM vehicles WHERE car_id IS NOT NULL
    );
PRAGMA foreign_keys=ON;
"""
 
MIGRATION_V3_DESCRIPTION = "add session_log and temporal_fingerprints tables"
MIGRATION_V3 = """
CREATE TABLE IF NOT EXISTS session_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint_id  TEXT    NOT NULL REFERENCES fingerprints(fingerprint_id),
    session_start   TEXT    NOT NULL,
    session_end     TEXT    NOT NULL,
    dwell_secs      INTEGER NOT NULL,
    arrival_hour    REAL    NOT NULL,
    day_of_week     INTEGER NOT NULL
);
 
CREATE INDEX IF NOT EXISTS idx_session_log_fp
    ON session_log(fingerprint_id);
CREATE INDEX IF NOT EXISTS idx_session_log_start
    ON session_log(session_start);
 
CREATE TABLE IF NOT EXISTS temporal_fingerprints (
    fingerprint_id          TEXT    PRIMARY KEY
                                    REFERENCES fingerprints(fingerprint_id),
    arrival_gmm_json        TEXT,
    arrival_peak_hours      TEXT,
    dwell_lognormal_mu      REAL,
    dwell_lognormal_sigma   REAL,
    dwell_median_secs       INTEGER,
    dwell_class             TEXT,
    dominant_period_hrs     REAL,
    periodicity_class       TEXT,
    acf_peak_value          REAL,
    presence_map_json       TEXT,
    computed_at             TEXT    NOT NULL,
    observation_days        INTEGER NOT NULL,
    min_sessions_met        INTEGER NOT NULL DEFAULT 0
);
"""
 
MIGRATION_V4_DESCRIPTION = "backfill session_log from existing sightings"
# Note: strftime('%w') returns 0=Sunday in SQLite; we remap to 0=Monday
MIGRATION_V4 = """
WITH ordered AS (
    SELECT
        s.vehicle_id,
        v.fingerprint_id,
        s.ts,
        LAG(s.ts) OVER (
            PARTITION BY v.fingerprint_id
            ORDER BY s.ts
        ) AS prev_ts
    FROM sightings s
    JOIN vehicles v USING (vehicle_id)
    WHERE v.fingerprint_id IS NOT NULL
),
session_breaks AS (
    SELECT *,
        CASE
            WHEN prev_ts IS NULL
              OR (julianday(ts) - julianday(prev_ts)) * 86400.0 > 600
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM ordered
),
session_ids AS (
    SELECT *,
        SUM(is_new_session) OVER (
            PARTITION BY fingerprint_id
            ORDER BY ts
            ROWS UNBOUNDED PRECEDING
        ) AS session_num
    FROM session_breaks
),
sessions AS (
    SELECT
        fingerprint_id,
        session_num,
        MIN(ts)  AS session_start,
        MAX(ts)  AS session_end,
        CAST(
            (julianday(MAX(ts)) - julianday(MIN(ts))) * 86400.0
            AS INTEGER
        )         AS dwell_secs,
        CAST(strftime('%H', MIN(ts)) AS REAL)
            + CAST(strftime('%M', MIN(ts)) AS REAL) / 60.0
                  AS arrival_hour,
        -- SQLite strftime %w gives 0=Sun, remap to 0=Mon..6=Sun
        (CAST(strftime('%w', MIN(ts)) AS INTEGER) + 6) % 7
                  AS day_of_week
    FROM session_ids
    GROUP BY fingerprint_id, session_num
)
INSERT INTO session_log
    (fingerprint_id, session_start, session_end,
     dwell_secs, arrival_hour, day_of_week)
SELECT
    fingerprint_id,
    session_start,
    session_end,
    dwell_secs,
    arrival_hour,
    day_of_week
FROM sessions
-- skip sessions already present (idempotent re-run safety)
WHERE NOT EXISTS (
    SELECT 1 FROM session_log sl
    WHERE sl.fingerprint_id = sessions.fingerprint_id
      AND sl.session_start   = sessions.session_start
);
"""
 
 
MIGRATION_V5_DESCRIPTION = "add rke_events and vehicle_state_transitions tables"
MIGRATION_V5 = """
CREATE TABLE IF NOT EXISTS rke_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    rtl433_id    INTEGER NOT NULL,
    event_type   TEXT    NOT NULL,
    burst_count  INTEGER NOT NULL DEFAULT 1,
    rssi         REAL,
    raw_code     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rke_events_ts ON rke_events(ts);
CREATE TABLE IF NOT EXISTS vehicle_state_transitions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT    NOT NULL,
    transition_type  TEXT    NOT NULL,
    fingerprint_id   TEXT    REFERENCES fingerprints(fingerprint_id),
    rke_event_id     INTEGER REFERENCES rke_events(id),
    candidate_fps    TEXT,
    confidence       TEXT    NOT NULL,
    tpms_gap_secs    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_transitions_fp
    ON vehicle_state_transitions(fingerprint_id);
CREATE INDEX IF NOT EXISTS idx_transitions_ts
    ON vehicle_state_transitions(ts);
"""
 
MIGRATION_V6_DESCRIPTION = "correct EezTire pressure: raw unit is 0.1 PSI not 0.1 kPa"
MIGRATION_V6 = """
UPDATE sightings
SET pressure_kpa = pressure_kpa * 6.89476
WHERE vehicle_id IN (
    SELECT vehicle_id FROM vehicles
    WHERE protocol LIKE '%EezTire%'
);
"""
 
MIGRATION_V7_DESCRIPTION = "re-correct EezTire sightings added after v6 with unfixed decoder"
MIGRATION_V7 = """
UPDATE sightings
SET pressure_kpa = pressure_kpa * 6.89476
WHERE vehicle_id IN (
    SELECT vehicle_id FROM vehicles
    WHERE protocol LIKE '%EezTire%'
)
AND ts > '2026-04-22T20:09:00';
"""
 
MIGRATION_V8_DESCRIPTION = "wipe corrupt TRW data: pressure was read from ID bytes not pressure field"
MIGRATION_V8 = """
PRAGMA foreign_keys=OFF;
DELETE FROM sightings WHERE vehicle_id IN (
    SELECT vehicle_id FROM vehicles
    WHERE protocol LIKE '%TRW%'
);
DELETE FROM vehicles WHERE protocol LIKE '%TRW%';
DELETE FROM cars WHERE car_id NOT IN (
    SELECT DISTINCT car_id FROM vehicles WHERE car_id IS NOT NULL
);
PRAGMA foreign_keys=ON;
"""
 
MIGRATION_V9_DESCRIPTION = "correct Jansite pressure: raw unit is 0.25 PSI not 0.1 kPa (factor x17)"
MIGRATION_V9 = """
UPDATE sightings
SET pressure_kpa = pressure_kpa * 17.0
WHERE vehicle_id IN (
    SELECT vehicle_id FROM vehicles
    WHERE protocol LIKE '%Jansite%'
);
"""

MIGRATION_V10_DESCRIPTION = "add interval_samples table and jitter columns to fingerprints"
MIGRATION_V10 = """
ALTER TABLE fingerprints ADD COLUMN jitter_sigma_ms   REAL;
ALTER TABLE fingerprints ADD COLUMN jitter_skewness   REAL;
ALTER TABLE fingerprints ADD COLUMN jitter_kurtosis   REAL;
ALTER TABLE fingerprints ADD COLUMN jitter_acf_lag1   REAL;
ALTER TABLE fingerprints ADD COLUMN jitter_samples    INTEGER;
ALTER TABLE fingerprints ADD COLUMN jitter_updated_at TEXT;
CREATE TABLE IF NOT EXISTS interval_samples (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint_id TEXT    NOT NULL REFERENCES fingerprints(fingerprint_id),
    vehicle_id     TEXT    NOT NULL REFERENCES vehicles(vehicle_id),
    ts             TEXT    NOT NULL,
    interval_ms    INTEGER NOT NULL,
    session_id     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_interval_fp
    ON interval_samples(fingerprint_id);
CREATE INDEX IF NOT EXISTS idx_interval_ts
    ON interval_samples(ts);
"""

MIGRATIONS = [
    (1, MIGRATION_V1_DESCRIPTION, None),
    (2, MIGRATION_V2_DESCRIPTION, MIGRATION_V2),
    (3, MIGRATION_V3_DESCRIPTION, MIGRATION_V3),
    (4, MIGRATION_V4_DESCRIPTION, MIGRATION_V4),
    (5, MIGRATION_V5_DESCRIPTION, MIGRATION_V5),
    (6, MIGRATION_V6_DESCRIPTION, MIGRATION_V6),
    (7, MIGRATION_V7_DESCRIPTION, MIGRATION_V7),
    (8, MIGRATION_V8_DESCRIPTION, MIGRATION_V8),
    (9, MIGRATION_V9_DESCRIPTION, MIGRATION_V9),
    (10, MIGRATION_V10_DESCRIPTION, MIGRATION_V10),
]
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
 
def log(msg: str, dry_run: bool = False) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"{prefix}{msg}")
 
 
def ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            applied_at  TEXT    NOT NULL,
            description TEXT    NOT NULL
        )
    """)
    conn.commit()
 
 
def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return row[0] if row[0] is not None else 0
 
 
def stamp(conn: sqlite3.Connection, version: int, description: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_migrations VALUES (?, ?, ?)",
        (version, datetime.now(timezone.utc).isoformat(), description),
    )
 
 
def row_counts(conn: sqlite3.Connection) -> dict:
    tables = [
        "sightings",
        "vehicles",
        "cars",
        "fingerprints",
        "session_log",
        "temporal_fingerprints",
        "interval_samples",
        "schema_migrations",
    ]
    counts = {}
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            counts[t] = n
        except sqlite3.OperationalError:
            counts[t] = None  # table does not exist yet
    return counts
 
 
def print_counts(label: str, counts: dict) -> None:
    print(f"\n  {label}")
    for table, n in counts.items():
        if n is not None:
            print(f"    {table:<30} {n:>8} rows")
        else:
            print(f"    {table:<30}  (not present)")
 
 
# ---------------------------------------------------------------------------
# Migration runners
# ---------------------------------------------------------------------------
 
 
def run_step(
    conn: sqlite3.Connection,
    version: int,
    description: str,
    sql: str | None,
    dry_run: bool,
) -> None:
    log(f"  Applying migration v{version}: {description}", dry_run)
 
    if sql is None:
        if not dry_run:
            stamp(conn, version, description)
            conn.commit()
        return
 
    if dry_run:
        preview = sql.strip().splitlines()
        for line in preview[:20]:
            print(f"    SQL> {line}")
        if len(preview) > 20:
            print(f"    SQL> ... ({len(preview) - 20} more lines)")
        return
 
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    # PRAGMAs must run outside the transaction
    pragmas = [s for s in statements if s.upper().startswith("PRAGMA")]
    rest = [s for s in statements if not s.upper().startswith("PRAGMA")]
 
    for pragma in [p for p in pragmas if "OFF" in p.upper()]:
        conn.execute(pragma)
 
    with conn:
        for stmt in rest:
            conn.execute(stmt)
        stamp(conn, version, description)
 
    for pragma in [p for p in pragmas if "ON" in p.upper()]:
        conn.execute(pragma)
 
 
def migrate(
    db_path: Path,
    target_version: int | None,
    dry_run: bool,
) -> None:
    # ---- backup first -------------------------------------------------------
    backup_path = db_path.with_suffix(
        f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    )
    if not dry_run:
        log(f"Creating backup: {backup_path}")
        shutil.copy2(db_path, backup_path)
        log(f"Backup created ✓")
    else:
        log(f"[DRY RUN] Would create backup: {backup_path}")
 
    # ---- open connection ----------------------------------------------------
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
 
    ensure_migrations_table(conn)
 
    current = current_version(conn)
    log(f"\nCurrent schema version: v{current}")
 
    # ---- determine which steps to run ---------------------------------------
    pending = [
        (v, desc, sql)
        for v, desc, sql in MIGRATIONS
        if v > current and (target_version is None or v <= target_version)
    ]
 
    if not pending:
        log("Nothing to do — database is already up to date.")
        conn.close()
        return
 
    log(f"Pending migrations: {[v for v, _, _ in pending]}\n")
 
    # ---- pre-migration counts -----------------------------------------------
    before = row_counts(conn)
    print_counts("Row counts BEFORE migration:", before)
 
    # ---- run each step ------------------------------------------------------
    print()
    for version, description, sql in pending:
        run_step(conn, version, description, sql, dry_run)
 
    # ---- post-migration counts ----------------------------------------------
    after = row_counts(conn)
    print_counts("\nRow counts AFTER migration:", after)
 
    # ---- diff summary -------------------------------------------------------
    print("\n  Changes:")
    for table in before:
        b = before[table]
        a = after[table]
        if b is None and a is not None:
            print(f"    {table:<30} created  ({a} rows)")
        elif b is not None and a is None:
            print(f"    {table:<30} dropped")
        elif b != a:
            delta = (a or 0) - (b or 0)
            sign = "+" if delta >= 0 else ""
            print(f"    {table:<30} {sign}{delta:>8} rows  ({b} → {a})")
 
    if not dry_run:
        final_version = current_version(conn)
        log(f"\nMigration complete. Schema version: v{final_version}")
        log(f"Backup retained at: {backup_path}")
    else:
        log("\n[DRY RUN] No changes were written.")
 
    conn.close()
 
 
# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
 
 
def main() -> None:
    parser = argparse.ArgumentParser(
        description="TPMS tracker database migration tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 migrate.py tpms.db              # apply all pending migrations
  python3 migrate.py tpms.db --dry-run    # preview without changes
  python3 migrate.py tpms.db --step 2     # apply only migration v2
        """,
    )
    parser.add_argument("db", help="path to tpms.db")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="preview changes without modifying the database",
    )
    parser.add_argument(
        "--step",
        type=int,
        metavar="N",
        help="apply only up to migration version N",
    )
    args = parser.parse_args()
 
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
 
    print(f"TPMS Tracker — database migration")
    print(f"Database: {db_path.resolve()}")
    print(f"Mode:     {'dry-run' if args.dry_run else 'live'}")
    if args.step:
        print(f"Target:   v{args.step}")
    print()
 
    try:
        migrate(db_path, target_version=args.step, dry_run=args.dry_run)
    except sqlite3.Error as e:
        print(f"\nDatabase error: {e}", file=sys.stderr)
        print("No changes were committed. Check the backup if needed.", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
 
 
if __name__ == "__main__":
    main()