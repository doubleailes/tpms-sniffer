#!/usr/bin/env bash
set -euo pipefail

DB="${1:-tpms.db}"
LOG_DIR="logs"

exec ./target/release/tpms-tracker \
  --db "$DB" \
  --serve 0.0.0.0:8080 \
  --log-level info \
  --log-file "$LOG_DIR/tpms-tracker.log" \
  --log-max-size 10 \
  --log-max-files 5
