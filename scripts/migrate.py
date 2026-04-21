#!/usr/bin/env python3
"""Database migration script for tpms-sniffer.

Run migrations sequentially against a SQLite database.  Each migration is
applied at most once (tracked in a ``schema_version`` table).

Usage:
    python3 scripts/migrate.py tpms.db            # apply all pending
    python3 scripts/migrate.py tpms.db --dry-run   # preview only
"""

import argparse
import sqlite3
import sys

# ---------------------------------------------------------------------------
# Migration registry
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

MIGRATIONS = [
    {
        "version": 6,
        "description": "correct EezTire pressure: multiply by 6.89476 (PSI→kPa)",
        "sql": """
UPDATE sightings
SET pressure_kpa = pressure_kpa * 6.89476
WHERE vehicle_id IN (
    SELECT vehicle_id FROM vehicles
    WHERE protocol LIKE '%EezTire%'
);

UPDATE vehicles
SET avg_pressure_kpa = avg_pressure_kpa * 6.89476
WHERE protocol LIKE '%EezTire%';
""",
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def log(msg: str, dry_run: bool = False) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"{prefix}{msg}")

def ensure_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version   INTEGER PRIMARY KEY,
            applied   TEXT DEFAULT (datetime('now'))
        )
        """
    )


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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply database migrations.")
    parser.add_argument("database", help="Path to the SQLite database file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print pending migrations without applying them",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    ensure_version_table(conn)
    cur = current_version(conn)
    print(f"Current schema version: {cur}")

    pending = [m for m in MIGRATIONS if m["version"] > cur]
    if not pending:
        print("Nothing to do — all migrations already applied.")
        sys.exit(0)

    print(f"{len(pending)} pending migration(s):")
    for m in pending:
        apply_migration(conn, m, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"Schema version is now {current_version(conn)}.")

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
