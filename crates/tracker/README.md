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
| 241        | EezTire   | 480 s  | EezTire sensors may transmit infrequently when the battery is low, with gaps of up to 6+ minutes observed in the field. |
| 298        | TRW-OOK   | 480 s  | Similar slow stationary TX cadence to EezTire. |
| *(any other)* | *(default)* | 300 s | Matches the prior behaviour of a single global constant. |

If a new protocol is added that transmits less often than once every 5 minutes,
extend the `match` arm in `vehicle_expiry_for()` and add a row to the table
above. Keep the entries in `rtl_433` ID order.

### Battery-state extension

When a sensor reports `battery_ok = false`, the effective expiry is extended by
an additional **5 minutes** (300 s) on top of the per-protocol base value. This
acknowledges that a low-battery sensor may transmit less frequently but is still
physically present. The extension avoids spurious track restarts for sensors
whose transmit power or duty cycle is degraded by a weak battery.

The logic lives in `effective_expiry()` in `src/resolver.rs`:

| Protocol  | Good battery | Low battery |
|-----------|------------:|------------:|
| AVE-TPMS  | 600 s       | 900 s       |
| EezTire   | 480 s       | 780 s       |
| TRW-OOK   | 480 s       | 780 s       |
| *(default)* | 300 s     | 600 s       |

## AVE-TPMS half-range pressure artifact

The AVE decoder (protocol 208) occasionally emits a frame whose pressure field
decodes to roughly half the real operating pressure — a ~382 kPa sensor will
occasionally transmit a ~190 kPa frame. This is a protocol-level dual-range
encoding quirk used for low-pressure detection, not a genuine pressure reading.
See the [rtl_433 AVE decoder source](https://github.com/merbanan/rtl_433/blob/master/src/devices/tpms_ave.c)
for the field definition (`pressure = byte * 1.5 kPa`).

The sniffer side sets `pressure_kpa_reliable = false` on those frames (see
`AVE_MIN_RELIABLE_KPA` in `crates/sniffer/src/decoder.rs`). The tracker then
skips pressure-fingerprint updates for them so the half-range value does not
drift the vehicle's signature and does not spawn a duplicate UUID.
