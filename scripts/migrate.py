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
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version"
        ).fetchone()
        return row[0]
    except sqlite3.OperationalError:
        return 0


def apply_migration(
    conn: sqlite3.Connection, migration: dict, *, dry_run: bool = False
) -> None:
    ver = migration["version"]
    desc = migration["description"]
    sql = migration["sql"]

    if dry_run:
        print(f"  [DRY-RUN] v{ver}: {desc}")
        print(f"    SQL:\n{sql.strip()}")
        return

    print(f"  Applying v{ver}: {desc} ...", end=" ", flush=True)
    conn.executescript(sql)
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (ver,))
    conn.commit()
    print("OK")


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
        "--dry-run", action="store_true",
        help="preview changes without modifying the database",
    )
    parser.add_argument(
        "--step", type=int, metavar="N",
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
        print("No changes were committed. Check the backup if needed.",
              file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
