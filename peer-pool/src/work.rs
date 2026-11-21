//! PeerCash work rules, written from the node source (consensus/randomx/hasher.go,
//! miner/remote_sealer.go) and the XMRig stratum job format.

/// Bytes of the RandomX input: sealHash (32), 3 zero bytes, 8-byte big-endian block nonce.
pub const BLOB_LEN: usize = 43;
/// XMRig writes its 4-byte nonce here (Job::nonceOffset for RandomX).
pub const MINER_NONCE_AT: usize = 39;
/// The pool's per-session extranonce: the upper 4 bytes of the block nonce.
pub const EXTRANONCE_AT: usize = 35;

/// The job blob for one session: sealHash, 000, extranonce, 0000 (XMRig fills the last 4).
pub fn blob(seal_hash: &[u8; 32], extranonce: [u8; 4]) -> [u8; BLOB_LEN] {
    let mut b = [0u8; BLOB_LEN];
    b[..32].copy_from_slice(seal_hash);
    b[EXTRANONCE_AT..MINER_NONCE_AT].copy_from_slice(&extranonce);
    b
}

/// The input a miner hashed: the blob with its 4 nonce bytes (as sent: hex of the blob bytes).
pub fn with_nonce(blob: &[u8; BLOB_LEN], miner_nonce: [u8; 4]) -> [u8; BLOB_LEN] {
    let mut b = *blob;
    b[MINER_NONCE_AT..].copy_from_slice(&miner_nonce);
    b
}

/// The 8-byte block nonce for eth_submitWork: bytes 35..43 of the input (the node reads them
/// big-endian and writes them back big-endian, so the bytes round-trip unchanged).
pub fn block_nonce(input: &[u8; BLOB_LEN]) -> [u8; 8] {
    input[EXTRANONCE_AT..].try_into().unwrap()
}

/// The value compared with a target: the hash's last 8 bytes read little-endian.
pub fn hash_value(hash: &[u8; 32]) -> u64 {
    u64::from_le_bytes(hash[24..].try_into().unwrap())
}

/// Target of a difficulty (node rule): maxUint64 / difficulty, all ones for difficulty <= 1.
pub fn target_of(difficulty: u64) -> u64 {
    if difficulty <= 1 { u64::MAX } else { u64::MAX / difficulty }
}

/// The node's rule: hash_value <= maxUint64 / difficulty.
pub fn meets(hash: &[u8; 32], difficulty: u64) -> bool {
    hash_value(hash) <= target_of(difficulty)
}

/// The node's target from eth_getWork word 3 (a 32-byte big-endian value that fits in 8 bytes).
pub fn target_from_word(word: &[u8; 32]) -> Option<u64> {
    if word[..24].iter().any(|b| *b != 0) {
        return None;
    }
    Some(u64::from_be_bytes(word[24..].try_into().unwrap()))
}

/// Difficulty implied by a target (maxUint64 / target), for stats.
pub fn difficulty_of_target(target: u64) -> u64 {
    if target == 0 { u64::MAX } else { u64::MAX / target }
}

/// XMRig's 8-byte job target: the share target as little-endian hex (16 hex digits).
pub fn stratum_target(difficulty: u64) -> String {
    crate::common::hex(&target_of(difficulty).to_le_bytes())
}

/// An EVM address as the pool stores it: 0x + 40 lowercase hex digits.
pub fn normalize_address(a: &str) -> Option<String> {
    let h = a.strip_prefix("0x").or_else(|| a.strip_prefix("0X"))?;
    if h.len() != 40 || !h.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("0x{}", h.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_layout() {
        let seal = [7u8; 32];
        let b = blob(&seal, [1, 2, 3, 4]);
        assert_eq!(&b[32..35], &[0, 0, 0]);
        assert_eq!(&b[35..39], &[1, 2, 3, 4]);
        assert_eq!(&b[39..43], &[0, 0, 0, 0]);
        let i = with_nonce(&b, [9, 8, 7, 6]);
        assert_eq!(block_nonce(&i), [1, 2, 3, 4, 9, 8, 7, 6]);
    }

    #[test]
    fn targets() {
        assert_eq!(target_of(1), u64::MAX);
        assert_eq!(u64::MAX / 321_570, 0x342c3e65097e); // live eth_getWork sample in SCOPE.md
        let mut w = [0u8; 32];
        w[26..].copy_from_slice(&[0x34, 0x2c, 0x3e, 0x65, 0x09, 0x7e]);
        assert_eq!(target_from_word(&w), Some(0x342c3e65097e));
        assert_eq!(difficulty_of_target(0x342c3e65097e), 321_570);
        assert_eq!(stratum_target(256), "ffffffffffffff00");
    }

    #[test]
    fn addresses() {
        assert_eq!(
            normalize_address("0xAbCdEf0123456789abcdef0123456789ABCDEF01").unwrap(),
            "0xabcdef0123456789abcdef0123456789abcdef01"
        );
        assert!(normalize_address("abcdef0123456789abcdef0123456789abcdef01").is_none());
        assert!(normalize_address("0x123").is_none());
    }
}
