// ============================================================
//  manchester.rs — Manchester and Differential Manchester
//                  codecs, ported from rtl_433 logic
//
//  Manchester (IEEE 802.3 / rtl_433 default):
//    1 → 10  (high→low transition)
//    0 → 01  (low→high transition)
//  Violation → skipped (invalid symbol)
//
//  Differential Manchester (Toyota / PMV-C210):
//    Transition at start of bit period → 0
//    No transition at start of bit period → 1
// ============================================================

/// Decode IEEE-style Manchester.
/// Input: raw bit slice.  Output: decoded bytes.
pub fn manchester_decode(bits: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut byte = 0u8;
    let mut bit_count = 0usize;
    let mut i = 0;
    while i + 1 < bits.len() {
        let decoded = match (bits[i] & 1, bits[i + 1] & 1) {
            (1, 0) => Some(1u8),
            (0, 1) => Some(0u8),
            _      => None,   // Manchester violation
        };
        i += 2;
        if let Some(b) = decoded {
            byte = (byte << 1) | b;
            bit_count += 1;
            if bit_count == 8 {
                out.push(byte);
                byte = 0;
                bit_count = 0;
            }
        }
    }
    out
}

/// Decode Differential Manchester.
/// Input: raw bit slice.  Output: decoded bytes.
pub fn differential_manchester_decode(bits: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut byte = 0u8;
    let mut bit_count = 0usize;
    if bits.len() < 2 {
        return out;
    }
    let mut prev = bits[0] & 1;
    let mut i = 1;
    while i + 1 < bits.len() {
        // Transition at start of symbol period → 0
        // No transition → 1
        let start = bits[i] & 1;
        let b = if start == prev { 1u8 } else { 0u8 };
        prev = bits[i + 1] & 1;
        byte = (byte << 1) | b;
        bit_count += 1;
        if bit_count == 8 {
            out.push(byte);
            byte = 0;
            bit_count = 0;
        }
        i += 2;
    }
    out
}

/// Search for a preamble pattern (as byte slice) within a bit stream
/// and return the bit offset immediately after it, or None.
pub fn find_preamble(bits: &[u8], preamble: &[u8]) -> Option<usize> {
    if bits.len() < preamble.len() {
        return None;
    }
    'outer: for start in 0..=(bits.len() - preamble.len()) {
        for (i, &pb) in preamble.iter().enumerate() {
            if bits[start + i] & 1 != pb & 1 {
                continue 'outer;
            }
        }
        return Some(start + preamble.len());
    }
    None
}
