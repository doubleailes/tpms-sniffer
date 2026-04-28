#import "@preview/charged-ieee:0.1.4": ieee

#show: ieee.with(
  title: [Passive Vehicle Tracking from Tire Pressure Telemetry: A Reference Implementation in Rust],
  abstract: [
    Direct Tire Pressure Monitoring Systems (dTPMS) broadcast cleartext sensor frames in the 315 and 433 MHz ISM bands at intervals of 30 to 90 seconds. These transmissions carry a sensor identifier, a pressure reading, and a temperature reading, and are receivable up to 50 metres away with a commodity software-defined radio. We present `tpms-sniffer`, an end-to-end open-source Rust implementation that demodulates 25 protocols ported from the `rtl_433` reference, and feeds a tracker pipeline that derives stable per-vehicle identities even for sensors that rotate their identifier on every transmission. The tracker combines four orthogonal signals — pressure fingerprinting, transmission-interval medians, oscillator jitter statistics, and Jaccard co-occurrence over rolling windows — to defeat the privacy mitigations introduced by the post-2018 European TPMS mandate. We describe the architecture, the matching logic, the persistence layer, and the longitudinal Temporal Behavioural Fingerprint (TBF) that links vehicle identities across separate tracking sessions. We discuss the privacy implications of a result obtainable for under USD 30 of hardware, and outline mitigations.
  ],
  authors: (
    (
      name: "Philippe Llerena",
      department: [Independent Research],
      organization: [doubleailes],
      location: [France],
      email: "philippe.llerena@forticheprod.com"
    ),
  ),
  index-terms: (
    "TPMS",
    "Software-defined radio",
    "RTL-SDR",
    "Passive surveillance",
    "Vehicle tracking",
    "Privacy",
    "Wireless protocols",
  ),
  bibliography: bibliography("refs.bib"),
  figure-supplement: [Fig.],
)

= Introduction
Direct Tire Pressure Monitoring Systems (dTPMS) became mandatory on new passenger vehicles in the United States in 2007 (TREAD Act, FMVSS 138) and in the European Union in 2014 (Regulation (EC) No 661/2009). Each wheel embeds a battery-powered sensor that periodically transmits a cleartext radio frame containing a sensor identifier, the measured tire pressure, and an internal temperature reading. The transmissions occur in the unlicensed 315 MHz (US) and 433.92 MHz (EU) ISM bands, are unencrypted, are unauthenticated, and are designed to be receivable by a low-power receiver mounted in the dashboard at a range of a few metres @rouf2010.

Because these transmissions are radio-frequency broadcasts, they are also receivable by anyone with a compatible antenna. A 2024 longitudinal study by Lizarribar et al. @lizarribar2024 deployed five RTL-SDR receivers across an urban neighbourhood for ten weeks and captured six million packets from more than twenty thousand distinct vehicles, demonstrating that passive TPMS reception alone is sufficient to reconstruct commute patterns, work schedules, and behavioural routines for the surveyed population. The threat is no longer hypothetical; the cost of the equipment required to mount it is approximately the price of a meal.

This paper describes `tpms-sniffer`, an open-source reference implementation of the full passive-tracking pipeline, written in Rust, and released under the MIT license. The design goal is to demonstrate, end to end, every step that separates a raw IQ stream from a persistent, queryable record of the vehicles that have passed within range of a single antenna. The tool decodes 25 sensor protocols, correlates frames into vehicle tracks, persists them to SQLite, and exposes a dashboard. It is intended as research instrumentation: a substrate for studying the radio-frequency leakage of automotive infrastructure, for evaluating proposed mitigations, and for raising practitioner awareness of an attack surface that is presently invisible to the general public.

== Contributions
The contributions of this work are:

- A complete, self-contained Rust implementation of OOK and FSK demodulation, Manchester and Differential Manchester decoding, and 25 distinct dTPMS protocol decoders, ported from the C reference of `rtl_433` @rtl433.
- A tracker pipeline that derives stable vehicle identities from rolling-ID sensors using four orthogonal correlation signals.
- A jitter-based cross-session fingerprinting layer that exploits the statistical signature of each sensor's local oscillator to re-identify vehicles whose identifier has rotated.
- A Temporal Behavioural Fingerprint (TBF) that captures per-vehicle arrival, dwell, and presence distributions for long-horizon re-identification.
- A discussion of the privacy implications of these techniques, with concrete recommendations for future protocol revisions.

== Paper overview
@sec:background reviews the relevant aspects of dTPMS protocols and prior privacy work. @sec:architecture presents the overall architecture of the system. @sec:sniffer describes the radio front-end, demodulators, and protocol decoders. @sec:tracker describes the tracker pipeline. @sec:longitudinal describes the longitudinal fingerprinting layers. @sec:discussion discusses the privacy implications and outlines mitigations. @sec:conclusion concludes.

= Background <sec:background>
== Direct TPMS architecture
A direct TPMS sensor is a battery-powered MEMS pressure transducer co-packaged with an inexpensive UHF transmitter, mounted inside each wheel. The sensor wakes periodically — typically every 30 to 90 seconds while the vehicle is moving, and every 10 to 60 minutes while stationary — and transmits a short frame containing:

- a sensor identifier (24, 28, or 32 bits depending on the protocol),
- a pressure reading (linearly encoded, typically with a 2.5 kPa quantization step),
- a temperature reading (8-bit two's complement, in degrees Celsius),
- a status field (low-battery, alarm, motion, in some protocols),
- an integrity field (CRC-8, CRC-16, or 8-bit checksum).

The frame is modulated using On-Off Keying (OOK) or Frequency-Shift Keying (FSK), generally with Manchester or Differential-Manchester line coding to maintain DC balance and clock recovery. Symbol rates cluster around 19.2 kbps. Frames are transmitted with no acknowledgement, no retry handshake, and no encryption, because the design assumption is that the receiver is the in-cab control unit located less than two metres away.

== The 2018 mitigation and its limits
The European Type Approval review of 2017 acknowledged the privacy concern of static identifiers and required, from 2018, that TPMS sensors used in EU vehicles support identifier rotation. In practice, this is implemented in two ways:

- Some manufacturers rotate a portion of the identifier on every transmission (the "rolling-ID" pattern; AVE-TPMS is representative).
- Some manufacturers retain a static identifier but combine it with a per-power-cycle pseudonym.

Neither mitigation defends against the techniques described in this paper. Pressure fingerprints, oscillator jitter, and co-occurrence patterns are physical-layer side channels that the protocol designers did not consider, and that no firmware update can remove without replacing the sensor.

= System architecture <sec:architecture>
The system is organised as two communicating Unix processes connected by a JSON-Lines pipe:

```
RTL-SDR ── tpms-sniffer ──(JSON-L)── tpms-tracker ── SQLite
                                          └── HTTP dashboard
```

The split is deliberate. `tpms-sniffer` is a real-time DSP loop with hard latency requirements: a frame missed because of a garbage-collection pause is unrecoverable. `tpms-tracker` is a stateful correlator that must persist data, expose an HTTP surface, and run rusqlite migrations; its failure mode is bounded recovery from disk. Decoupling the two via stdout/stdin means the sniffer remains a small, stateless DSP component that can be replaced (for instance, by `rtl_433 -F json`) without modifying the tracker, and the tracker can ingest pre-recorded captures for offline replay-based testing.

The codebase is a Cargo workspace with two crates: `tpms-sniffer` (six files, ~2400 lines) and `tpms-tracker` (eleven files, ~12000 lines). Both target stable Rust 2024.

= The sniffer <sec:sniffer>
== Radio front-end
The sniffer interfaces with an RTL-SDR dongle through the `rtlsdr-next` crate. Samples are received as a stream of 8-bit unsigned IQ pairs at a configurable sample rate (default 250 kHz). The dongle is tuned to 315.000 MHz (US band) or 433.920 MHz (EU band). Auto-gain is the default, although a user-supplied gain figure in tenths of dB can be specified for environments with strong adjacent-channel interference.

== OOK demodulation
The OOK demodulator computes the per-sample magnitude $sqrt(I^2 + Q^2)$ and applies an adaptive threshold tracked by a slow IIR filter:

$ T_n = 0.9999 dot T_(n-1) + 0.0001 dot |s_n| $ <eq:threshold>

A bit is emitted as `1` when the instantaneous magnitude exceeds $1.4 T_n$, and `0` otherwise. The factor $1.4$ provides hysteresis against the noise floor and is a deliberate compromise: too low yields false bits during the inter-frame gap, too high suppresses the leading edge of weak frames. Symbol-clock recovery uses a phase accumulator advanced by one symbol period (52 µs at 19.2 kbps); the accumulator is realigned on every detected edge in the magnitude trace. The output of this stage is a hard-decision bit stream at the symbol rate.

== FSK demodulation
The FSK demodulator implements a two-point frequency discriminator. The instantaneous phase $phi_n = arctan(Q_n / I_n)$ is differentiated as $Delta phi_n = phi_n - phi_(n-1)$, with the customary $2 pi$ unwrap. The sign of $Delta phi$ is the demodulated bit. Because $Delta phi$ is also a coarse estimate of the instantaneous frequency, this stage doubles as a cheap signal-presence detector.

== Framing
Each TPMS protocol begins with a known preamble. The framer searches the bit stream for two preamble patterns in parallel:

- the alternating preamble `0xAAAAAAAA` (used by Schrader, Renault, Ford, Citroën, BMW, and the majority of FSK protocols),
- the Huf preamble `0x00FF` (used by AVE, EezTire, and several aftermarket OOK protocols).

When a preamble is detected, the framer extracts the maximum-length payload and emits it for protocol decoding. False positives at this stage are common; they are suppressed by the per-protocol integrity check.

== Protocol decoders
The decoder bank contains 25 protocol implementations covering the majority of OEM and aftermarket sensors deployed in Europe and North America. Each decoder validates the integrity field, extracts the structured fields, and emits a normalised packet record. Each record carries a confidence score in the range 0 to 100, computed as:

$ c = 5 + 60 dot bb(1)_("CRC ok") + 20 dot bb(1)_("p plausible") $ $ + 10 dot bb(1)_("p typical") + 5 dot bb(1)_("T typical") $ <eq:confidence>

where the indicator variables denote, respectively, that the integrity field passes, that the pressure is in the physically plausible range $[1.5, 900]$ kPa, that the pressure is in the typical passenger-tyre range $[150, 350]$ kPa, and that the temperature is in the typical range $[-20, 80] degree C$. The default acceptance threshold of 65 retains every CRC-validated frame and rejects all frames that fail the integrity check.

= The tracker <sec:tracker>
== Resolver
The resolver receives a stream of normalised packets and is responsible for assigning each one to a `vehicle_uuid`. Two regimes coexist:

*Fixed-ID regime.* The resolver maintains an in-memory map keyed by the pair `(rtl433_id, sensor_id)`. The protocol identifier is part of the key because two different protocols can incidentally encode the same 32-bit value with different semantics. When a packet arrives whose key is already known, the existing UUID is returned. A guard rejects sensor IDs with fewer than three bits cleared from `0xFFFFFFFF`; these are demodulation artefacts, not real sensors. Decoded sensor IDs that pass this filter are typically stable for the operational lifetime of the sensor (5 to 10 years).

*Rolling-ID regime.* When the protocol is known to rotate identifiers, or when the fixed-ID regime cannot find a candidate, the resolver consults the fingerprint correlator. The correlator scores each currently-tracked vehicle against the incoming packet using the criteria described in @sec:fingerprint, and either returns the best-scoring vehicle (if any candidate exceeds the acceptance threshold) or allocates a new UUID. The acceptance threshold is intentionally conservative: a new UUID is preferred over a wrong merge, because UUIDs can later be coalesced offline using the longitudinal fingerprints, while wrong merges are difficult to undo.

== Pressure fingerprinting <sec:fingerprint>
A vehicle's tyre pressures evolve slowly. Within a single tracking session (a few hours), all four pressures may be treated as constant up to a few kPa of measurement noise. The resolver therefore maintains, for each vehicle, an exponential moving average of the last 16 pressures, separately for each of the four wheel slots:

$ p_(n+1) = alpha dot p_("obs") + (1 - alpha) dot p_n, quad alpha = 0.2 $

A candidate vehicle is considered a pressure match if $|p_("obs") - p_v| < tau_p$, where $tau_p$ depends on the inferred vehicle class (4.0 kPa for motorcycles, 5.0 kPa for passenger cars, 6.0 kPa for SUVs, 8.0 kPa for light commercial vans, 15.0 kPa for heavy trucks; see @sec:classification). The class-dependent tolerance accounts for the larger absolute pressure variation that heavier-loaded tyres exhibit under temperature and load changes.

== Transmission-interval matching
Each TPMS sensor transmits at an approximately fixed cadence determined by its firmware. The tracker maintains, for each vehicle, a ring buffer of the last 8 inter-packet intervals, and computes the median when at least 3 samples are available. Two vehicles whose median intervals differ by more than 8 seconds are treated as distinct, even if their pressures match. The 8-second tolerance is deliberately wide to absorb sensor crystal drift (which has a non-negligible temperature coefficient on the cheap parts used in TPMS sensors) and the sub-second jitter introduced by the SDR demodulator.

== Jaccard co-occurrence grouping
A passenger car carries four TPMS sensors. They are mounted on the same physical chassis and therefore appear, and disappear, from the receiver simultaneously. The Jaccard grouper exploits this strict co-occurrence to associate the four wheel-level vehicle UUIDs into a single car-level identifier.

For each vehicle, the grouper maintains a set $W_v$ of 60-second windows in which the vehicle was sighted. The Jaccard similarity between two vehicles is:

$ J(u, v) = (|W_u inter W_v|) / (|W_u union W_v|) $ <eq:jaccard>

Two vehicles are merged into the same `car_id` when their Jaccard similarity exceeds an iterative threshold descent of $[0.75, 0.60, 0.45, 0.30]$. Starting strict and relaxing produces stable groups before ambiguous ones, which avoids the failure mode of merging unrelated vehicles that happen to coincide once. After at least three windows are observed for both vehicles, the score is admissible. The expected group size is taken from the inferred vehicle class — 4 for cars, 2 for motorcycles, variable for trucks. Lizarribar et al. report 12-of-12 correct identifications using this method on a labelled set @lizarribar2024.

== Wheel-position inference
Many fixed-ID protocols share a common high-byte prefix among the four sensors of a single vehicle, with only the low byte differing. When two sensors that have already been Jaccard-grouped share at least two high-order bytes, the trailing byte differential is treated as evidence of wheel position (FL, FR, RL, RR), inferred from the canonical ascending-byte assignment used by most OEM commissioning procedures. This is a heuristic and the implementation reports it as such.

== Vehicle classification <sec:classification>
The classifier infers a coarse vehicle class from the pressure range and the sensor count of the associated car group. The boundary table is:

#figure(
  caption: [Vehicle class boundaries by tire pressure and sensor count.],
  placement: top,
  table(
    columns: (auto, auto, auto),
    align: (left, right, right),
    inset: (x: 8pt, y: 4pt),
    stroke: (x, y) => if y <= 1 { (top: 0.5pt) },
    fill: (x, y) => if y > 0 and calc.rem(y, 2) == 0 { rgb("#efefef") },

    table.header[Class][Pressure (kPa)][Sensors],
    [Motorcycle], [200–290], [2],
    [Passenger car], [200–280], [4],
    [SUV / light truck], [260–340], [4],
    [Light commercial van], [350–480], [4+],
    [Heavy truck], [550–800], [6–18],
  )
) <tab:vehicle-class>

The 200–350 kPa range overlaps for cars and SUVs and is resolved by sensor count when available, defaulting to the lower class otherwise. The class is then used to select the per-class pressure tolerance described above and the expected group size used by the Jaccard grouper.

= Longitudinal fingerprinting <sec:longitudinal>
The signals described so far are sufficient within a single tracking session, where the sensor identifier may rotate but the physical sensor is continuously visible. They are not sufficient across sessions: after a vehicle parks for a week and returns, its tyre pressures will have drifted, its Jaccard window history will be empty, and its rotating identifier will have rolled over many times. Two layers address the cross-session problem.

== Oscillator jitter fingerprinting
Each TPMS sensor's transmission cadence is governed by a low-cost crystal or RC oscillator. While the nominal interval is fixed in firmware, the actual interval-to-interval jitter has a per-device statistical signature determined by the physical oscillator. The tracker collects up to 10 000 inter-packet intervals per fingerprint, filters them with a Tukey IQR fence to suppress missed-frame outliers, and computes four moments:

- $sigma$, the standard deviation of the interval distribution,
- the skewness of the interval distribution,
- the excess kurtosis of the interval distribution,
- the lag-1 autocorrelation of the interval series.

Two fingerprints are compared by a weighted Euclidean distance over these four normalised dimensions, with weights $(0.40, 0.25, 0.20, 0.15)$. A similarity score above 0.80 — corresponding to a normalised distance below 0.20 — links two fingerprints across sessions. The window of the smallest fingerprint must contain at least 50 samples before its profile is admissible. In practice, this requires a sensor to be observed for at least one full transmission cycle, typically 25 to 75 minutes of continuous reception.

== Temporal Behavioural Fingerprinting (TBF)
The TBF layer captures four distributions per vehicle, computed from the persistent sighting history:

+ an *arrival-time distribution*, modelled as a Gaussian mixture over hour-of-day,
+ a *dwell-time distribution*, fitted as log-normal,
+ a *periodicity score*, extracted as the dominant peak of the autocorrelation of the daily sighting count,
+ a *presence map*, a 24×7 matrix encoding the empirical probability of presence at each weekday/hour bin.

A fingerprint requires at least 7 distinct sessions before it becomes admissible. Two TBFs are matched by a similarity score derived from KL divergence over the arrival GMM, Bhattacharyya distance over the dwell log-normal, and cosine similarity over the flattened presence map. A combined similarity above 0.85 identifies the two fingerprints as belonging to the same physical vehicle.

The TBF is computationally inexpensive — the FFT is performed at most once per hour per fingerprint — but its discriminative power grows with the observation window. A fingerprint observed for one day is likely indistinguishable from a few thousand others; a fingerprint observed for one month is essentially unique within the population observed by a single receiver.

= Multi-receiver federation
A single receiver yields presence-only data. Two receivers separated by a known distance, observing the same vehicle within a known time delta, yield direction and approximate speed. The tracker's database schema includes a `receiver_id` column on every sighting, and the resolver implements a 5-second cross-receiver deduplication window to prevent the same physical event from being counted twice. A future revision will publish sightings via MQTT to a federation broker that performs cross-receiver TBF matching, enabling city-scale traffic analysis from a sparse network of nodes.

= Persistence and dashboard
Sightings are persisted to a local SQLite database via `rusqlite` (bundled), with separate tables for vehicles, sightings, fingerprints, sessions, TBFs, and the Jaccard adjacency matrix. The schema migration system is idempotent and forward-compatible: every release adds columns rather than dropping them, so historical capture data remains queryable across upgrades.

The tracker exposes an HTTP dashboard via `axum` on a configurable port. The dashboard endpoint serves a single-page application that visualises the current vehicle list, the Jaccard graph, and the per-vehicle TBF presence maps. JSON endpoints expose the same data for programmatic consumption. The default deployment runs the tracker as a `systemd` service, with the sniffer pipe configured in the unit file.

= Discussion <sec:discussion>
== Cost and accessibility
The hardware required to reproduce the results described in this paper consists of an RTL-SDR v3 dongle (USD 30), an antenna (USD 10), and any computer capable of running Rust binaries. The software is open source and operates on a stock Linux or macOS host. There is no specialist knowledge required beyond familiarity with a terminal and a `cargo build` invocation. This is a meaningfully lower bar than that of, for instance, ALPR camera systems, and it does not require line of sight or daylight.

== Defensive implications
Three mitigations would meaningfully raise the bar for passive TPMS surveillance:

+ *Lower transmit power.* Current sensors transmit at approximately +5 dBm to ensure reliable reception at the dashboard. Halving the transmit power would reduce the received signal-to-noise ratio at 50 metres by approximately 3 dB, which (combined with other mitigations) would push the practical reception range below the typical depth of an urban kerbside.
+ *Per-session pseudonymous identifiers.* The current rolling-ID scheme rotates a portion of the identifier on every transmission, but the rotation is predictable. A cryptographic pseudonym, derived per ignition cycle from a vehicle-private key, would defeat fixed-ID matching and limit rolling-ID matching to within a session.
+ *Frame-level encryption.* The marginal cost of an authenticated-encryption layer over the 7-byte payload is negligible for modern automotive MCUs. The barrier is regulatory (any change to the radio interface requires re-certification across multiple jurisdictions) rather than technical.

The first two are deployable on existing hardware via firmware update; the third requires next-generation sensors. None of them affect the operating principle of TPMS, which is to alert the driver when a tyre pressure deviates from its nominal value.

== Ethical considerations
The reference implementation described here is published with a clear legal notice that it is for *passive reception only*, that no transmission is performed, and that the user is responsible for verifying local regulations regarding the reception of automotive RF signals. The intent is to make the technique visible — to vehicle owners, to manufacturers, and to regulators — so that informed mitigation decisions can be made. Suppressing the implementation would not suppress the technique, which has been publicly described since 2010 @rouf2010; it would only ensure that the asymmetry between attacker and defender continues to grow.

= Conclusion <sec:conclusion>
We have described an open-source, end-to-end implementation of passive vehicle tracking from TPMS telemetry. The system decodes 25 sensor protocols, defeats post-2018 identifier rotation through a combination of pressure fingerprinting, transmission-interval matching, oscillator jitter analysis, and Jaccard co-occurrence grouping, and persists the resulting tracks in a queryable database. A longitudinal fingerprinting layer extends re-identification across sessions through statistical analysis of arrival, dwell, and presence patterns. The total cost of a deployment is dominated by the antenna and is reproducible by any practitioner familiar with the Rust toolchain.

The result is a privacy regression of an automotive subsystem mandated for safety. Reconciling the two — preserving the tyre-pressure alerts that have measurably reduced highway fatalities, while removing the surveillance side channel that the original specification did not anticipate — will require coordinated revision of the radio interface across at least the next vehicle generation.

The source code, the protocol decoders, and a corpus of recorded captures are available under the MIT license at `github.com/doubleailes/tpms-sniffer`.
