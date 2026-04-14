# tpms-tracker

Stateful consumer of `tpms-sniffer` JSON output that consolidates repeated
sightings into persistent vehicle tracks.

## Per-protocol `VEHICLE_EXPIRY`

`VEHICLE_EXPIRY` is the window after a vehicle's most recent sighting during
which a new packet can still be correlated to it via the pressure fingerprint.
Once the window lapses, the next matching packet spawns a new vehicle UUID
because we can no longer be sure the sensor is the same physical one returning
into range.

The default is **5 minutes** (300 s). A few protocols have known low
transmission rates that require a longer window; the mapping lives in
`vehicle_expiry_for()` in `src/resolver.rs`.

| rtl_433 ID | Protocol  | Expiry | Rationale |
|-----------:|-----------|-------:|-----------|
| 208        | AVE-TPMS  | 600 s  | Aftermarket AVE clip-on sensors transmit every 2–8 minutes when the vehicle is stationary. A 5-minute window leaks the track between sightings and spawns a fresh UUID for what is the same physical vehicle. |
| *(any other)* | *(default)* | 300 s | Matches the prior behaviour of a single global constant. |

If a new protocol is added that transmits less often than once every 5 minutes,
extend the `match` arm in `vehicle_expiry_for()` and add a row to the table
above. Keep the entries in `rtl_433` ID order.

## AVE-TPMS half-range pressure artifact

The AVE decoder (protocol 208) occasionally emits a frame whose pressure field
decodes to roughly half the real operating pressure — a ~382 kPa sensor will
occasionally transmit a ~190 kPa frame. This is a protocol-level dual-range
encoding quirk used for low-pressure detection, not a genuine pressure reading.

The sniffer side sets `pressure_kpa_reliable = false` on those frames (see
`AVE_MIN_VALID_KPA` in `crates/sniffer/src/decoder.rs`). The tracker then
skips pressure-fingerprint updates for them so the half-range value does not
drift the vehicle's signature and does not spawn a duplicate UUID.
