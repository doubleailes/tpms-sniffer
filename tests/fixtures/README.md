# TPMS Fixture Files

This directory contains JSON-L captures of raw `tpms-sniffer` output, used for
deterministic regression testing of the tracker.

## Collection guidelines

- **Capture duration:** minimum 20 minutes, ideally 60+
- **Capture location:** any environment with vehicle traffic (parking lot,
  roadside)
- **Naming:** `session_YYYYMMDD_HHMM_<location>.jsonl`
- **No PII:** the JSON contains sensor IDs but no human-identifiable
  information
- **Compression:** commit as `.jsonl.gz`, decompressed on the fly during replay

## Capturing

```bash
# Capture 30 minutes of data
./tpms-sniffer --json --duration-secs 1800 > session_$(date +%Y%m%d_%H%M).jsonl

# Compress before committing
gzip session_*.jsonl
mv session_*.jsonl.gz tests/fixtures/wild/
```

## Replaying

```bash
# Replay a single fixture
./tpms-tracker --replay tests/fixtures/wild/session_20260415_1000.jsonl.gz \
  --db :memory: --assert-consistency

# Replay all fixtures
for f in tests/fixtures/wild/*.jsonl.gz; do
  echo "=== Replaying $f ==="
  ./tpms-tracker --replay "$f" --db :memory: --assert-consistency
done
```

## Current fixtures

| File | Date | Duration | Packets | Notes |
|------|------|----------|---------|-------|
| `session_20260415_1000.jsonl.gz` | 2026-04-15 | ~5 h | 3 589 | Initial capture, 6 protocols |
