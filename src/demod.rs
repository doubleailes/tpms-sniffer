// ============================================================
//  demod.rs — OOK and FSK demodulators for TPMS
//
//  Input:  raw u8 IQ buffer from rtlsdr-next (offset binary,
//          DC = 127).  Interleaved: [I0, Q0, I1, Q1, …]
//
//  OOK: compute envelope (magnitude), threshold, output bits
//       at symbol clock recovered by simple edge timing.
//
//  FSK: two-point discriminator; sign of d/dt(phase) → bit.
// ============================================================

const OOK_SYMBOL_US: f32 = 52.0; // ~19.2 kbps (Schrader / TRW)
const FSK_SYMBOL_US: f32 = 52.0;

// ─── OOK ────────────────────────────────────────────────────

pub struct OokDemod {
    sample_rate: u32,
    /// samples per TPMS symbol
    sps: f32,
    /// adaptive threshold (IIR)
    threshold: f32,
    // clock recovery
    phase_accum: f32,
    last_mag: f32,
    bits: Vec<u8>,
}

impl OokDemod {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            sps: sample_rate as f32 * OOK_SYMBOL_US / 1_000_000.0,
            threshold: 50.0,
            phase_accum: 0.0,
            last_mag: 0.0,
            bits: Vec::new(),
        }
    }

    pub fn process(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();

        let samples: Vec<(f32, f32)> = chunk
            .chunks_exact(2)
            .map(|s| (s[0] as f32 - 127.5, s[1] as f32 - 127.5))
            .collect();

        for (i, q) in &samples {
            let mag = (i * i + q * q).sqrt();

            // ── Adaptive threshold (slow IIR) ──────────────
            // Track signal peak vs noise floor across window
            self.threshold = self.threshold * 0.9999 + mag * 0.0001;

            let bit = if mag > self.threshold * 1.4 { 1u8 } else { 0u8 };

            // ── Clock recovery: advance phase accumulator ──
            self.phase_accum += 1.0;
            if self.phase_accum >= self.sps {
                self.phase_accum -= self.sps;
                out.push(bit);
            }

            self.last_mag = mag;
        }

        out
    }
}

// ─── FSK ────────────────────────────────────────────────────
// Simple FM discriminator: atan2 of conjugate product
// Δφ = arg( z[n] × conj(z[n-1]) )

pub struct FskDemod {
    sps: f32,
    phase_accum: f32,
    prev_i: f32,
    prev_q: f32,
    /// DC-block IIR state
    dc_i: f32,
    dc_q: f32,
}

impl FskDemod {
    pub fn new(sample_rate: u32) -> Self {
        Self {
            sps: sample_rate as f32 * FSK_SYMBOL_US / 1_000_000.0,
            phase_accum: 0.0,
            prev_i: 0.0,
            prev_q: 0.0,
            dc_i: 0.0,
            dc_q: 0.0,
        }
    }

    pub fn process(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut demod_acc: f32 = 0.0;
        let mut demod_count = 0u32;

        for pair in chunk.chunks_exact(2) {
            let i = pair[0] as f32 - 127.5;
            let q = pair[1] as f32 - 127.5;

            // ── Conjugate product ──────────────────────────
            // d = z[n] × conj(z[n-1])
            let di = i * self.prev_i + q * self.prev_q;
            let dq = q * self.prev_i - i * self.prev_q;

            self.prev_i = i;
            self.prev_q = q;

            // Phase difference (FM discriminator output)
            let delta = dq.atan2(di + 1e-10);

            demod_acc += delta;
            demod_count += 1;

            // ── Sample clock ──────────────────────────────
            self.phase_accum += 1.0;
            if self.phase_accum >= self.sps {
                self.phase_accum -= self.sps;
                let avg = demod_acc / demod_count as f32;
                out.push(if avg >= 0.0 { 1u8 } else { 0u8 });
                demod_acc = 0.0;
                demod_count = 0;
            }
        }

        out
    }
}
