//! Helpers shared by the pool and the test miner.

use std::time::{SystemTime, UNIX_EPOCH};

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

pub fn unhex32(s: &str) -> Option<[u8; 32]> {
    unhex(s)?.try_into().ok()
}

pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// A 256-bit little-endian value as a float (enough precision for accounting and hash rates).
pub fn le_to_f64(v: &[u8; 32]) -> f64 {
    v.iter().rev().fold(0.0, |acc, &b| acc * 256.0 + b as f64)
}

/// Expected attempts for one ticket at or below `target`: 2^256 / (target + 1).
pub fn work_of(target: &[u8; 32]) -> f64 {
    2f64.powi(256) / (le_to_f64(target) + 1.0)
}
