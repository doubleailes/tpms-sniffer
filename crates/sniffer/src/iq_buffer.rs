// ============================================================
//  iq_buffer.rs — sliding IQ ring buffer for CFO estimation
//
//  Issue #45 wiring: the demod/framer chain converts raw IQ
//  samples into bits before the framer emits a frame, so by the
//  time `decoder::decode` runs the original carrier samples are
//  gone.  This module keeps a sliding window of the most recent
//  IQ samples — large enough to cover a full burst — so that
//  `main` can hand a preamble-sized window of f32 samples to the
//  decoder for CFO measurement.
//
//  The buffer stores raw u8 IQ pairs as the RTL-SDR delivers
//  them (offset binary, DC = 127.5).  `extract_window` returns
//  interleaved f32 samples with DC removed, ready to feed
//  `cfo::estimate_cfo`.
// ============================================================

/// Number of IQ samples retained in the ring buffer.
///
/// At 250 kHz a single TPMS burst spans roughly (preamble + frame) ×
/// samples-per-symbol = (16 + 128) × 13 ≈ 1.9k samples.  We keep
/// 131,072 samples (≈524 ms, ≈256 KB at u8 IQ) to leave plenty of
/// headroom over RTL-SDR USB chunk sizes.
///
/// Sizing rationale (issue #45 wiring fix): the framer can emit a
/// frame whose preamble lies anywhere within the chunk that just
/// completed, so by the time we look up an IQ window the preamble
/// may be `(chunk_samples - frame_offset + burst_samples)` samples
/// behind "now".  A typical RTL-SDR USB transfer is 8k–32k samples;
/// retaining 131k samples (≥4× the largest realistic chunk size)
/// guarantees the preamble is still in the buffer regardless of
/// where in the chunk the frame appeared, eliminating the silent
/// fallback that produced CFO ≡ 0 Hz across all fingerprints.
pub const IQ_RING_SAMPLES: usize = 131_072;

pub struct IqRingBuffer {
    /// Interleaved I,Q bytes.  Length is fixed at `capacity * 2`.
    buf: Vec<u8>,
    /// Total number of *samples* (I,Q pairs) ever pushed.  Used
    /// only to detect the warm-up phase before the buffer fills.
    pushed: u64,
    /// Capacity in samples (I,Q pairs).
    capacity: usize,
    /// Write position in *bytes*.  Wraps modulo `buf.len()`.
    write_pos: usize,
}

impl IqRingBuffer {
    pub fn new(capacity_samples: usize) -> Self {
        Self {
            buf: vec![127u8; capacity_samples * 2],
            pushed: 0,
            capacity: capacity_samples,
            write_pos: 0,
        }
    }

    /// Append a chunk of interleaved I,Q bytes.  Bytes after the
    /// most recent `capacity` samples evict the oldest entries.
    pub fn push_chunk(&mut self, chunk: &[u8]) {
        // Caller may pass an odd-length chunk in pathological cases;
        // truncate to whole samples.
        let n_bytes = chunk.len() & !1;
        let cap_bytes = self.buf.len();

        for &b in &chunk[..n_bytes] {
            self.buf[self.write_pos] = b;
            self.write_pos = (self.write_pos + 1) % cap_bytes;
        }
        self.pushed = self.pushed.saturating_add((n_bytes / 2) as u64);
    }

    /// Number of samples ever pushed into the buffer.  When this
    /// is below `capacity` the window is partially uninitialised
    /// (filled with DC) and CFO estimates will be unreliable.
    pub fn pushed(&self) -> u64 {
        self.pushed
    }

    /// Return `n` IQ samples ending `samples_back` samples before
    /// the most recent write, as interleaved f32 with DC removed.
    ///
    /// `samples_back` is measured from "now" (one past the most
    /// recently written sample).  The returned slice covers
    /// `[now - samples_back - n, now - samples_back)`.
    ///
    /// Returns `None` if `samples_back + n` exceeds capacity, or
    /// if fewer than `samples_back + n` samples have ever been
    /// pushed (i.e. the requested window is older than the data
    /// the buffer has seen).
    pub fn extract_window(&self, samples_back: usize, n: usize) -> Option<Vec<f32>> {
        let needed = samples_back + n;
        if needed > self.capacity || (self.pushed as usize) < needed {
            return None;
        }
        let cap_bytes = self.buf.len();
        // Byte index of the first sample of the window.  `write_pos`
        // points at the next write slot (one past the most recently
        // written byte), so "now" in bytes is `write_pos`.
        let start_bytes = (self.write_pos + cap_bytes - (samples_back + n) * 2) % cap_bytes;

        let mut out = Vec::with_capacity(n * 2);
        for k in 0..n {
            let p = (start_bytes + k * 2) % cap_bytes;
            let i = self.buf[p] as f32 - 127.5;
            let q = self.buf[p + 1] as f32 - 127.5;
            out.push(i);
            out.push(q);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_returns_none_until_filled() {
        let mut b = IqRingBuffer::new(64);
        b.push_chunk(&[120, 130, 121, 131]); // 2 samples
        assert!(b.extract_window(0, 32).is_none());
    }

    #[test]
    fn extract_recovers_recent_samples() {
        let mut b = IqRingBuffer::new(64);
        // Push 64 samples (128 bytes) with monotonic I,Q.
        let mut bytes = Vec::with_capacity(128);
        for k in 0..64u8 {
            bytes.push(k);
            bytes.push(k.wrapping_add(1));
        }
        b.push_chunk(&bytes);
        let w = b.extract_window(0, 4).expect("window");
        // The last 4 samples are k = 60..64.
        assert_eq!(w.len(), 8);
        assert!((w[0] - (60.0 - 127.5)).abs() < 1e-3);
        assert!((w[1] - (61.0 - 127.5)).abs() < 1e-3);
        assert!((w[6] - (63.0 - 127.5)).abs() < 1e-3);
        assert!((w[7] - (64.0 - 127.5)).abs() < 1e-3);
    }

    #[test]
    fn extract_handles_wrap_around() {
        // Capacity 4 samples (8 bytes).  Push 12 bytes (6 samples) so
        // the buffer wraps once and the last 4 samples span the wrap.
        let mut b = IqRingBuffer::new(4);
        let bytes: Vec<u8> = (0u8..12).collect();
        b.push_chunk(&bytes);
        let w = b.extract_window(0, 4).expect("window");
        // Most recent 4 samples are pairs (4,5), (6,7), (8,9), (10,11).
        let expect_i = [4.0, 6.0, 8.0, 10.0];
        let expect_q = [5.0, 7.0, 9.0, 11.0];
        for k in 0..4 {
            assert!(
                (w[k * 2] - (expect_i[k] - 127.5)).abs() < 1e-3,
                "I[{k}]={}",
                w[k * 2]
            );
            assert!((w[k * 2 + 1] - (expect_q[k] - 127.5)).abs() < 1e-3);
        }
    }

    #[test]
    fn samples_back_offset() {
        let mut b = IqRingBuffer::new(8);
        let bytes: Vec<u8> = (0u8..16).collect(); // 8 samples
        b.push_chunk(&bytes);
        // Samples back = 4 means skip the most recent 4 samples and
        // take the 2 before that.  Most recent sample index is 7
        // (bytes 14,15); samples_back=4 → index 3 (bytes 6,7).
        // Take 2 samples → indices 2,3 → bytes (4,5),(6,7).
        let w = b.extract_window(4, 2).expect("window");
        assert!((w[0] - (4.0 - 127.5)).abs() < 1e-3);
        assert!((w[1] - (5.0 - 127.5)).abs() < 1e-3);
        assert!((w[2] - (6.0 - 127.5)).abs() < 1e-3);
        assert!((w[3] - (7.0 - 127.5)).abs() < 1e-3);
    }
}
