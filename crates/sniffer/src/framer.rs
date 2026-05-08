// ============================================================
//  framer.rs — Preamble detection and frame extraction
//
//  Supported preambles
//  ───────────────────
//  Schrader EG53MA4 / TRW   : 0b_1010_1010_1010_1010  (8+ alternating)
//  Huf TPMS                 : 0b_0000_0000_1111_1111  (8-bit sync word)
//  Continental / General    : same alternating as Schrader
//
//  After preamble we collect a fixed window of bits and hand
//  them to the decoder.  We search for ALL of the preambles
//  simultaneously using a single shift-register.
//
//  Each emitted frame carries the IQ sample index within the
//  current chunk where the frame's last bit was decided
//  (issue #45 fix — main needs this to locate the preamble
//  IQ samples for CFO measurement).  `last_bit_sample_idx`
//  is sourced from `DemodBit::sample_idx`.
// ============================================================

use crate::demod::DemodBit;

const ALT_PREAMBLE_MIN: usize = 16; // minimum alternating bits
const HUF_PREAMBLE: u32 = 0x00FF; // 16 bit
const MAX_FRAME_BITS: usize = 128; // maximum frame to collect

/// A complete frame as emitted by the framer.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Decoded bit values, one per symbol.
    pub bits: Vec<u8>,
    /// Sample index within the current chunk at which the last
    /// bit of this frame was decided.  Used by `main` to compute
    /// the offset back into the IQ ring buffer.
    pub last_bit_sample_idx: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Idle,
    AltPreamble { count: usize },
    HufPreamble { reg: u32 },
    Collecting { bits: usize, total: usize },
}

pub struct Framer {
    state: State,
    /// rolling shift register (max 128 bits → u128)
    sr: u128,
    sr_len: usize,
    frame_buf: Vec<u8>,
    #[allow(unused)]
    pub frames: Vec<Vec<u8>>,
}

impl Framer {
    pub fn new() -> Self {
        Self {
            state: State::Idle,
            sr: 0,
            sr_len: 0,
            frame_buf: Vec::with_capacity(128),
            frames: Vec::new(),
        }
    }

    /// Feed a slice of demodulated bits (each carrying its source
    /// sample index within the current chunk).  Returns any
    /// complete frames found.
    pub fn feed(&mut self, bits: &[DemodBit]) -> Vec<Frame> {
        let mut out = Vec::new();

        for &db in bits {
            let b = db.bit & 1;

            // ── Update shift register ──────────────────────
            self.sr = (self.sr << 1) | b as u128;
            if self.sr_len < 128 {
                self.sr_len += 1;
            }

            // Pre-compute before the match to avoid simultaneous mutable
            // borrow of self.state and immutable borrow of self.
            let alt = self.alternating_count();

            match self.state {
                // ── Idle: look for alternating pattern ─────
                State::Idle => {
                    if alt >= ALT_PREAMBLE_MIN {
                        self.state = State::AltPreamble {
                            count: ALT_PREAMBLE_MIN,
                        };
                    } else if self.sr_len >= 16 {
                        let reg16 = (self.sr & 0xFFFF) as u32;
                        if reg16 == HUF_PREAMBLE {
                            self.state = State::HufPreamble { reg: reg16 };
                        }
                    }
                }

                // ── Alternating preamble: wait for it to end
                State::AltPreamble { ref mut count } => {
                    if alt > *count {
                        *count = alt;
                    } else {
                        // Preamble ended — start collecting frame
                        self.frame_buf.clear();
                        self.frame_buf.push(b);
                        self.state = State::Collecting {
                            bits: 1,
                            total: MAX_FRAME_BITS,
                        };
                    }
                }

                // ── Huf preamble ────────────────────────────
                State::HufPreamble { .. } => {
                    self.frame_buf.clear();
                    self.frame_buf.push(b);
                    self.state = State::Collecting {
                        bits: 1,
                        total: 72, // Huf frames are ~72 bits
                    };
                }

                // ── Collecting frame bits ────────────────────
                State::Collecting {
                    ref mut bits,
                    total,
                } => {
                    self.frame_buf.push(b);
                    *bits += 1;
                    if *bits >= total {
                        out.push(Frame {
                            bits: self.frame_buf.clone(),
                            last_bit_sample_idx: db.sample_idx,
                        });
                        self.frame_buf.clear();
                        self.state = State::Idle;
                        self.sr = 0;
                        self.sr_len = 0;
                    }
                }
            }
        }

        out
    }

    /// Count trailing alternating bits in shift register
    fn alternating_count(&self) -> usize {
        let mut count = 0usize;
        let mut prev = (self.sr >> 1) & 1;
        let bits = self.sr_len.min(64);
        for i in 0..bits {
            let cur = (self.sr >> i) & 1;
            if cur != prev {
                count += 1;
                prev = cur;
            } else {
                break;
            }
        }
        count
    }
}
