//! The pool's share check against vectors made with the PeerCash node's own code
//! (gotools/vectors: consensus/randomx SealHash, NewHasher, seed and target rules):
//!   - test/vectors-chain.jsonl: real mainnet blocks, whose mixHash the network's nodes accepted;
//!   - test/vectors-random.jsonl: random keys, seal hashes and nonces through the node's hasher.
//! The input is rebuilt with the pool's own packing (blob + extranonce + XMRig nonce), not copied.

use peerpool::common::{unhex, unhex32};
use peerpool::rx::{Cache, Vm};
use peerpool::work;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

fn load(name: &str) -> Vec<Value> {
    let path = format!("{}/../test/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    text.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn check(vectors: &[Value]) -> (usize, f64) {
    let mut cache: Option<Arc<Cache>> = None;
    let mut vm: Option<Vm> = None;
    let mut hashes = 0;
    let mut hash_secs = 0.0;
    for v in vectors {
        let s = |k: &str| v[k].as_str().unwrap().to_string();
        let key = unhex32(&s("key")).unwrap();
        let seal = unhex32(&s("seal_hash")).unwrap();
        let nonce: [u8; 8] = unhex(&s("nonce")).unwrap().try_into().unwrap();
        // The pool's path: session blob with the extranonce (upper 4 bytes), XMRig's nonce (lower 4).
        let blob = work::blob(&seal, nonce[..4].try_into().unwrap());
        let input = work::with_nonce(&blob, nonce[4..].try_into().unwrap());
        assert_eq!(input.to_vec(), unhex(&s("input")).unwrap(), "packing differs from the node's sealInput");
        assert_eq!(work::block_nonce(&input), nonce);
        if cache.as_ref().is_none_or(|c| c.key != key) {
            cache = Some(Arc::new(Cache::new(&key)));
        }
        let c = cache.as_ref().unwrap();
        let vm = vm.get_or_insert_with(|| Vm::new(c.clone()));
        let t = Instant::now();
        let got = vm.hash(c, &input);
        hash_secs += t.elapsed().as_secs_f64();
        hashes += 1;
        assert_eq!(got.to_vec(), unhex(&s("hash")).unwrap(), "RandomX hash differs from the node's hasher: {v}");
        if let Some(mix) = v.get("mix_hash").and_then(|m| m.as_str()) {
            assert_eq!(got.to_vec(), unhex(mix).unwrap(), "hash differs from the block's mixHash: {v}");
        }
        let diff = v["difficulty"].as_u64().unwrap();
        assert_eq!(work::meets(&got, diff), v["meets"].as_bool().unwrap(), "target rule differs: {v}");
        if let Some(tw) = v.get("target_word").and_then(|t| t.as_str()) {
            assert_eq!(work::target_from_word(&unhex32(tw).unwrap()), Some(work::target_of(diff)));
        }
    }
    (hashes, hash_secs)
}

#[test]
fn mainnet_blocks() {
    let v = load("vectors-chain.jsonl");
    assert!(v.len() >= 10);
    let (n, secs) = check(&v);
    // Every real block must meet its own difficulty with the pool's rule.
    eprintln!("mainnet blocks: {n} match the chain; light-mode hash {:.1} ms on average", secs * 1000.0 / n as f64);
}

#[test]
fn random_vectors() {
    let v = load("vectors-random.jsonl");
    assert!(v.len() >= 20);
    let (n, secs) = check(&v);
    eprintln!("random vectors: {n} match the node's hasher; light-mode hash {:.1} ms on average", secs * 1000.0 / n as f64);
}
