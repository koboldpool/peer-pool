//! peer-pool: Kobold Pool's PeerCash (PEER) pool. Work from its node's eth_getWork, Monero-style stratum for stock
//! XMRig (rx/0), every submit re-checked with RandomX (light mode, keyed by the epoch seed), blocks sent with eth_submitWork.
//! Same data files as neb-pool (shares.jsonl, blocks.jsonl, stats.json), read by payout.py and the site.

use peerpool::common::*;
use peerpool::rpc::{self, Rpc};
use peerpool::rx::{Cache, Verifier};
use peerpool::work;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MAX_LINE: usize = 16_384;
/// Templates kept (the node remembers its last 8 sealHashes).
const KEEP_TEMPLATES: usize = 8;
/// A share on a replaced template still counts for this long (it was in flight).
const STALE_GRACE: Duration = Duration::from_secs(10);
/// Jobs remembered per session (a vardiff change makes a new job on the same template).
const KEEP_SESSION_JOBS: usize = 8;
/// Vardiff aims at about one share every 10 s: it retunes after VARDIFF_WINDOW.
const VARDIFF_WINDOW: Duration = Duration::from_secs(60);
const RATE_LOW: usize = 3; // fewer per window: easier
const RATE_HIGH: usize = 12; // more per window: harder
const MAX_SUBMITS_PER_SEC: usize = 25;
const MAX_BAD_IN_A_ROW: u32 = 30;
const HASHRATE_WINDOW: Duration = Duration::from_secs(600);

struct Config {
    listen: String,
    /// Every login on this port mines solo.
    listen_solo: String,
    node_rpc: String,
    data: PathBuf,
    /// Share difficulty = 2^e. Vardiff keeps e in min_e..=max_e.
    start_e: u32,
    min_e: u32,
    max_e: u32,
    verify_threads: usize,
    poll_ms: u64,
    /// Hold jobs while the node has no peers (production). Off for a private test chain.
    require_peers: bool,
    name: String,
}

fn log2_floor(v: u64) -> u32 {
    63 - v.max(1).leading_zeros()
}

fn config() -> Config {
    let mut c = Config {
        listen: "0.0.0.0:3500".into(),
        listen_solo: "0.0.0.0:3510".into(),
        node_rpc: "http://127.0.0.1:8545".into(),
        data: "pool-data".into(),
        start_e: 12,
        min_e: 8,
        max_e: 40,
        verify_threads: 2,
        poll_ms: 250,
        require_peers: true,
        name: "peer-pool".into(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = args.get(i + 1).cloned().unwrap_or_default();
        let num = |what: &str| -> u64 { v.parse().unwrap_or_else(|_| panic!("{what} needs a number")) };
        match args[i].as_str() {
            "--listen" => c.listen = v,
            "--listen-solo" => c.listen_solo = v,
            "--node-rpc" => c.node_rpc = v,
            "--data" => c.data = v.into(),
            "--start-diff" => c.start_e = log2_floor(num("--start-diff")),
            "--min-diff" => c.min_e = log2_floor(num("--min-diff")),
            "--max-diff" => c.max_e = log2_floor(num("--max-diff")),
            "--verify-threads" => c.verify_threads = num("--verify-threads") as usize,
            "--poll-ms" => c.poll_ms = num("--poll-ms"),
            "--name" => c.name = v,
            "--allow-no-peers" => {
                c.require_peers = false;
                i += 1;
                continue;
            }
            "--help" | "-h" => {
                println!(
                    "peer-pool [--listen 0.0.0.0:3500] [--listen-solo 0.0.0.0:3510] [--node-rpc http://127.0.0.1:8545] [--data pool-data]\n          \
                     [--start-diff 4096] [--min-diff 256] [--max-diff 2^40] [--verify-threads 2] [--poll-ms 250] [--allow-no-peers] [--name peer-pool]\n\
                     Difficulties are rounded down to a power of two. --allow-no-peers is for a private test chain only."
                );
                std::process::exit(0);
            }
            other => panic!("unknown option {other}"),
        }
        i += 2;
    }
    c.start_e = c.start_e.clamp(c.min_e, c.max_e);
    c
}

/// One piece of work from the node.
struct Template {
    id: u64,
    seal: [u8; 32],
    seed: [u8; 32],
    /// Network target (a hash wins if its value is at or below it) and its difficulty.
    target: u64,
    difficulty: u64,
    height: u64,
    cache: Arc<Cache>,
    created: Instant,
    replaced: Mutex<Option<Instant>>,
    /// The node took a block from this template: it forgets the sealHash, so later solutions are
    /// counted as shares only (they would be refused, and they would only orphan our own block).
    won: AtomicBool,
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    rejected: AtomicU64,
    blocks_submitted: AtomicU64,
    blocks_accepted: AtomicU64,
}

struct Shared {
    cfg: Config,
    rpc: Rpc,
    verifier: Verifier,
    templates: Mutex<VecDeque<Arc<Template>>>,
    /// RandomX caches by key, newest last (two are kept around an epoch switch).
    caches: Mutex<Vec<Arc<Cache>>>,
    /// Block nonces already submitted, per template (duplicates).
    seen: Mutex<HashMap<u64, HashSet<[u8; 8]>>>,
    sessions: Mutex<Vec<Arc<Session>>>,
    /// Recent accepted shares for the hash rate: (time, address, worker, work, solo).
    recent: Mutex<VecDeque<(Instant, String, String, f64, bool)>>,
    next_session: AtomicU64,
    next_template: AtomicU64,
    next_en: AtomicU32,
    node_up: AtomicBool,
    /// The node serves work (eth_getWork answers; it refuses while syncing or behind its peers).
    work_up: AtomicBool,
    peers: AtomicU64,
    /// Fees in basis points (100 = 1 %), from <data>/pool-config.json, re-read every 10 s.
    fee_bp: AtomicU32,
    solo_fee_bp: AtomicU32,
    /// Share work since the pool's last block: effort = round work / block difficulty.
    round_work: Mutex<f64>,
    counters: Counters,
    files: Mutex<(File, File, File)>, // shares.jsonl, blocks.jsonl, pool.log
    started: u64,
}

impl Shared {
    fn log(&self, msg: &str) {
        let line = format!("{} {msg}\n", now());
        print!("{line}");
        if let Ok(mut f) = self.files.lock() {
            let _ = f.2.write_all(line.as_bytes());
        }
    }

    fn record(&self, which: usize, v: Value) {
        if let Ok(mut f) = self.files.lock() {
            let file = if which == 0 { &mut f.0 } else { &mut f.1 };
            let _ = writeln!(file, "{v}");
        }
    }

    fn latest(&self) -> Option<Arc<Template>> {
        self.templates.lock().unwrap().back().cloned()
    }

    fn template(&self, id: u64) -> Option<Arc<Template>> {
        self.templates.lock().unwrap().iter().rev().find(|t| t.id == id).cloned()
    }

    fn cache_for(&self, key: &[u8; 32]) -> Arc<Cache> {
        if let Some(c) = self.caches.lock().unwrap().iter().find(|c| c.key == key) {
            return c.clone();
        }
        let t = Instant::now();
        let c = Arc::new(Cache::new(key));
        self.log(&format!("RandomX cache built for seed {} in {} ms", hex(key), t.elapsed().as_millis()));
        let mut caches = self.caches.lock().unwrap();
        caches.push(c.clone());
        while caches.len() > 2 {
            caches.remove(0);
        }
        c
    }
}

struct SessState {
    /// (address, worker) after login.
    login: Option<(String, String)>,
    solo: bool,
    en: [u8; 4],
    e: u32,
    /// Jobs sent: (job id, template id, share difficulty).
    jobs: VecDeque<(String, u64, u64)>,
    next_job: u64,
    window_start: Instant,
    window_count: usize,
    second: (Instant, usize),
    bad_in_a_row: u32,
}

struct Session {
    id: u64,
    peer: String,
    solo_port: bool,
    out: Mutex<TcpStream>,
    st: Mutex<SessState>,
    alive: AtomicBool,
}

impl Session {
    fn send(&self, v: &Value) {
        let mut line = v.to_string();
        line.push('\n');
        let ok = self.out.lock().map(|mut s| s.write_all(line.as_bytes()).is_ok()).unwrap_or(false);
        if !ok {
            self.alive.store(false, Ordering::Relaxed);
        }
    }
}

/// Builds the session's job for a template at its current difficulty (capped at the network's).
fn make_job(sess: &Session, t: &Template) -> Option<Value> {
    let mut st = sess.st.lock().unwrap();
    st.login.as_ref()?;
    let diff = (1u64 << st.e).min(t.difficulty.max(1));
    st.next_job += 1;
    let job_id = format!("{:x}", st.next_job);
    st.jobs.push_back((job_id.clone(), t.id, diff));
    while st.jobs.len() > KEEP_SESSION_JOBS {
        st.jobs.pop_front();
    }
    let blob = work::blob(&t.seal, st.en);
    Some(json!({
        "blob": hex(&blob), "job_id": job_id, "target": work::stratum_target(diff), "algo": "rx/0",
        "height": t.height, "seed_hash": hex(&t.seed),
    }))
}

fn push_job(sess: &Session, t: &Template) {
    if let Some(job) = make_job(sess, t) {
        sess.send(&json!({"jsonrpc": "2.0", "method": "job", "params": job}));
    }
}

// ------------------------------------------------------------------------------------- node link

fn node_loop(sh: Arc<Shared>) {
    let mut last_peer_check = Instant::now() - Duration::from_secs(60);
    let mut last_err = String::new();
    let mut held_logged = false;
    loop {
        thread::sleep(Duration::from_millis(sh.cfg.poll_ms));
        if last_peer_check.elapsed() >= Duration::from_secs(5) {
            last_peer_check = Instant::now();
            match sh.rpc.call("net_peerCount", json!([])) {
                Ok(v) => {
                    sh.peers.store(rpc::quantity(&v).unwrap_or(0), Ordering::Relaxed);
                    if !sh.node_up.swap(true, Ordering::Relaxed) {
                        sh.log(&format!("node RPC {} reachable", sh.cfg.node_rpc));
                    }
                }
                Err(e) => {
                    if sh.node_up.swap(false, Ordering::Relaxed) || last_err != e {
                        sh.log(&format!("node RPC {} not reachable: {e}", sh.cfg.node_rpc));
                        last_err = e;
                    }
                    sh.work_up.store(false, Ordering::Relaxed);
                    continue;
                }
            }
        }
        let w = match sh.rpc.call("eth_getWork", json!([])) {
            Ok(w) => w,
            Err(e) => {
                if sh.work_up.swap(false, Ordering::Relaxed) || last_err != e {
                    sh.log(&format!("no work from the node: {e}"));
                    last_err = e;
                }
                continue;
            }
        };
        let parsed = (|| {
            let a = w.as_array()?;
            Some((rpc::word(a.first()?)?, rpc::word(a.get(1)?)?, work::target_from_word(&rpc::word(a.get(2)?)?)?))
        })();
        let Some((seal, seed, target)) = parsed else {
            sh.log(&format!("eth_getWork answer not understood: {w}"));
            continue;
        };
        if !sh.work_up.swap(true, Ordering::Relaxed) {
            sh.log("node serves work");
            last_err.clear();
        }
        if sh.latest().is_some_and(|t| t.seal == seal) {
            continue;
        }
        if sh.cfg.require_peers && sh.peers.load(Ordering::Relaxed) == 0 {
            if !held_logged {
                sh.log("node has no peers: jobs held (use --allow-no-peers only on a private test chain)");
                held_logged = true;
            }
            continue;
        }
        held_logged = false;
        let height = match sh.rpc.call("eth_blockNumber", json!([])).ok().as_ref().and_then(rpc::quantity) {
            Some(h) => h + 1,
            None => continue,
        };
        let cache = sh.cache_for(&seed);
        install_template(&sh, seal, seed, target, height, cache);
    }
}

fn install_template(sh: &Arc<Shared>, seal: [u8; 32], seed: [u8; 32], target: u64, height: u64, cache: Arc<Cache>) {
    let id = sh.next_template.fetch_add(1, Ordering::Relaxed);
    let difficulty = work::difficulty_of_target(target);
    let t = Arc::new(Template {
        id,
        seal,
        seed,
        target,
        difficulty,
        height,
        cache,
        created: Instant::now(),
        replaced: Mutex::new(None),
        won: AtomicBool::new(false),
    });
    {
        let mut ts = sh.templates.lock().unwrap();
        if let Some(prev) = ts.back() {
            *prev.replaced.lock().unwrap() = Some(Instant::now());
        }
        ts.push_back(t.clone());
        while ts.len() > KEEP_TEMPLATES {
            ts.pop_front();
        }
        let ids: HashSet<u64> = ts.iter().map(|t| t.id).collect();
        sh.seen.lock().unwrap().retain(|k, _| ids.contains(k));
    }
    sh.log(&format!("new work: height {height} difficulty {difficulty} seal {}", hex(&seal[..8])));
    let sessions: Vec<Arc<Session>> = sh.sessions.lock().unwrap().clone();
    for s in sessions {
        if s.alive.load(Ordering::Relaxed) {
            push_job(&s, &t);
        }
    }
}

/// Sends a solution to the node and records the block. Returns whether the node took it.
fn submit_block(sh: &Shared, t: &Template, input: &[u8; work::BLOB_LEN], hash: &[u8; 32], finder: &str, mode: &str, effort: f64) {
    let nonce = work::block_nonce(input);
    sh.counters.blocks_submitted.fetch_add(1, Ordering::Relaxed);
    let res = sh.rpc.call(
        "eth_submitWork",
        json!([format!("0x{}", hex(&nonce)), format!("0x{}", hex(&t.seal)), format!("0x{}", hex(hash))]),
    );
    let (sent, accepted, code) = match &res {
        Ok(Value::Bool(b)) => (true, *b, if *b { "ok".to_string() } else { "refused".to_string() }),
        Ok(other) => (true, false, format!("unexpected answer {other}")),
        Err(e) => (false, false, e.clone()),
    };
    // The block id is the block hash; find it at the template height (the node inserts it there).
    let mut id = format!("0x{}", hex(hash));
    let mut height = t.height;
    if !accepted {
        t.won.store(false, Ordering::Release); // let a later solution on this template try again
    }
    if accepted {
        for h in [t.height, t.height.saturating_sub(1), t.height + 1] {
            let b = sh.rpc.call("eth_getBlockByNumber", json!([format!("0x{h:x}"), false])).unwrap_or(Value::Null);
            if b.get("mixHash").and_then(|v| v.as_str()) == Some(&format!("0x{}", hex(hash))) {
                if let Some(bh) = b.get("hash").and_then(|v| v.as_str()) {
                    id = bh.to_ascii_lowercase();
                    height = h;
                }
                break;
            }
        }
        sh.counters.blocks_accepted.fetch_add(1, Ordering::Relaxed);
    }
    let fee_bp = if mode == "solo" { sh.solo_fee_bp.load(Ordering::Relaxed) } else { sh.fee_bp.load(Ordering::Relaxed) };
    sh.log(&format!("BLOCK found by {finder} at height {height} id {id}: node {code}"));
    sh.record(1, json!({"ts": now(), "event": "found", "height": height, "id": id, "pow_hash": format!("0x{}", hex(hash)),
                        "seal_hash": format!("0x{}", hex(&t.seal)), "nonce": format!("0x{}", hex(&nonce)),
                        "finder": finder, "sent": sent, "block_work": t.difficulty as f64, "mode": mode,
                        "effort": effort, "fee_bp": fee_bp}));
    sh.record(1, json!({"ts": now(), "event": "result", "height": height, "id": id, "finder": finder,
                        "accepted": accepted, "code": code}));
}

// ------------------------------------------------------------------------------------- miners

fn err(id: &Value, code: i32, msg: &str) -> Value {
    json!({"id": id, "jsonrpc": "2.0", "result": null, "error": {"code": code, "message": msg}})
}

fn ok(id: &Value, result: Value) -> Value {
    json!({"id": id, "jsonrpc": "2.0", "result": result, "error": null})
}

/// "0xADDR" or "0xADDR.worker"; a missing worker falls back to XMRig's rig-id, then "default".
fn parse_login(login: &str, rigid: Option<&str>) -> Option<(String, String)> {
    let ok_worker =
        |w: &str| !w.is_empty() && w.len() <= 32 && w.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    let (addr, worker) = match login.split_once('.') {
        Some((a, w)) if ok_worker(w) => (a, w.to_string()),
        Some(_) => return None, // a worker name was given but is not valid
        None => (login, rigid.filter(|r| ok_worker(r)).unwrap_or("default").to_string()),
    };
    Some((work::normalize_address(addr)?, worker))
}

fn serve_miner(sh: Arc<Shared>, stream: TcpStream, solo_port: bool) {
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(600)));
    let Ok(out) = stream.try_clone() else { return };
    let id = sh.next_session.fetch_add(1, Ordering::Relaxed);
    let en = sh.next_en.fetch_add(1, Ordering::Relaxed).to_be_bytes();
    let sess = Arc::new(Session {
        id,
        peer: peer.clone(),
        solo_port,
        out: Mutex::new(out),
        alive: AtomicBool::new(true),
        st: Mutex::new(SessState {
            login: None,
            solo: solo_port,
            en,
            e: sh.cfg.start_e,
            jobs: VecDeque::new(),
            next_job: 0,
            window_start: Instant::now(),
            window_count: 0,
            second: (Instant::now(), 0),
            bad_in_a_row: 0,
        }),
    });
    sh.sessions.lock().unwrap().push(sess.clone());
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match (&mut reader).take(MAX_LINE as u64 + 1).read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(n) if n > MAX_LINE => break,
            Ok(_) => {}
        }
        if line.trim().is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(&line) else { break };
        let rid = req.get("id").cloned().unwrap_or(Value::Null);
        let p = req.get("params").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let reply = match method {
            "login" => login(&sh, &sess, &rid, &p),
            "submit" => submit(&sh, &sess, &rid, &p),
            "keepalived" => ok(&rid, json!({"status": "KEEPALIVED"})),
            "getjob" => match sh.latest().and_then(|t| make_job(&sess, &t)) {
                Some(j) => ok(&rid, j),
                None => err(&rid, -1, "no job"),
            },
            _ => err(&rid, -1, "unknown method"),
        };
        sess.send(&reply);
        if !sess.alive.load(Ordering::Relaxed) {
            break;
        }
    }
    sess.alive.store(false, Ordering::Relaxed);
    sh.sessions.lock().unwrap().retain(|s| s.id != sess.id);
    let who = sess.st.lock().unwrap().login.clone().map(|(a, w)| format!("{a}.{w}")).unwrap_or_default();
    sh.log(&format!("session {} {} closed ({who})", sess.id, sess.peer));
}

fn login(sh: &Shared, sess: &Session, rid: &Value, p: &Value) -> Value {
    let login = p.get("login").and_then(|v| v.as_str()).unwrap_or("");
    let pass = p.get("pass").and_then(|v| v.as_str()).unwrap_or("");
    let rigid = p.get("rigid").and_then(|v| v.as_str());
    let Some((addr, worker)) = parse_login(login, rigid) else {
        return err(rid, -1, "login must be a PeerCash address (0x + 40 hex), optionally .worker");
    };
    if sess.st.lock().unwrap().login.is_some() {
        return err(rid, -1, "already logged in");
    }
    let Some(t) = sh.latest() else { return err(rid, -1, "pool has no work from its node yet") };
    let solo = sess.solo_port || pass.to_ascii_lowercase().contains("solo");
    {
        let mut st = sess.st.lock().unwrap();
        st.login = Some((addr.clone(), worker.clone()));
        st.solo = solo;
    }
    let Some(job) = make_job(sess, &t) else { return err(rid, -1, "internal error") };
    let agent = p.get("agent").and_then(|v| v.as_str()).unwrap_or("?");
    sh.log(&format!("session {} {} logged in {addr}.{worker}{} ({agent})", sess.id, sess.peer, if solo { " (solo)" } else { "" }));
    ok(rid, json!({"id": format!("{:x}", sess.id), "job": job, "extensions": ["algo", "keepalive"], "status": "OK"}))
}

fn reject(sh: &Shared, sess: &Session, rid: &Value, msg: &str) -> Value {
    sh.counters.rejected.fetch_add(1, Ordering::Relaxed);
    let mut st = sess.st.lock().unwrap();
    st.bad_in_a_row += 1;
    if st.bad_in_a_row > MAX_BAD_IN_A_ROW {
        sess.alive.store(false, Ordering::Relaxed);
    }
    err(rid, -1, msg)
}

fn submit(sh: &Shared, sess: &Session, rid: &Value, p: &Value) -> Value {
    let job_id = p.get("job_id").and_then(|v| v.as_str()).unwrap_or("");
    let nonce = p.get("nonce").and_then(|v| v.as_str()).and_then(unhex).and_then(|v| <[u8; 4]>::try_from(v).ok());
    let claimed = p.get("result").and_then(|v| v.as_str()).and_then(unhex32);
    let (addr, worker, solo, en, job) = {
        let mut st = sess.st.lock().unwrap();
        let Some((addr, worker)) = st.login.clone() else { return err(rid, -1, "unauthenticated") };
        let now_i = Instant::now();
        if now_i.duration_since(st.second.0) >= Duration::from_secs(1) {
            st.second = (now_i, 0);
        }
        st.second.1 += 1;
        if st.second.1 > MAX_SUBMITS_PER_SEC {
            sess.alive.store(false, Ordering::Relaxed);
            return err(rid, -1, "too many submits");
        }
        let job = st.jobs.iter().find(|j| j.0 == job_id).cloned();
        (addr, worker, st.solo, st.en, job)
    };
    let Some((_, tid, diff)) = job else { return reject(sh, sess, rid, "Invalid job id") };
    let Some(t) = sh.template(tid) else { return reject(sh, sess, rid, "Stale share") };
    if t.replaced.lock().unwrap().is_some_and(|r| r.elapsed() > STALE_GRACE) {
        return reject(sh, sess, rid, "Stale share");
    }
    let Some(nonce) = nonce else { return reject(sh, sess, rid, "Bad nonce") };
    let input = work::with_nonce(&work::blob(&t.seal, en), nonce);
    let block_nonce = work::block_nonce(&input);
    if !sh.seen.lock().unwrap().entry(t.id).or_default().insert(block_nonce) {
        return reject(sh, sess, rid, "Duplicate share");
    }
    let hash = sh.verifier.hash(&t.cache, &input);
    if claimed.is_some_and(|c| c != hash) {
        return reject(sh, sess, rid, "Bad hash");
    }
    if !work::meets(&hash, diff) {
        return reject(sh, sess, rid, "Low difficulty share");
    }
    // A block solution counts only on the current, not yet solved template; a late one (on a
    // replaced or already solved template) is still a valid share.
    let block = work::hash_value(&hash) <= t.target
        && t.replaced.lock().unwrap().is_none()
        && !t.won.swap(true, Ordering::AcqRel);
    let work_done = diff as f64;
    let mode = if solo { "solo" } else { "pplns" };
    sh.counters.accepted.fetch_add(1, Ordering::Relaxed);
    sh.record(0, json!({"ts": now(), "addr": addr, "worker": worker, "work": work_done, "height": t.height,
                        "block": block, "mode": mode}));
    sh.recent.lock().unwrap().push_back((Instant::now(), addr.clone(), worker.clone(), work_done, solo));
    let effort = {
        let mut rw = sh.round_work.lock().unwrap();
        *rw += work_done;
        let e = *rw / t.difficulty.max(1) as f64;
        if block {
            *rw = 0.0;
        }
        e
    };
    if block {
        submit_block(sh, &t, &input, &hash, &format!("{addr}.{worker}"), mode, effort);
    }
    vardiff(sh, sess);
    ok(rid, json!({"status": "OK"}))
}

fn vardiff(sh: &Shared, sess: &Session) {
    let changed = {
        let mut st = sess.st.lock().unwrap();
        st.bad_in_a_row = 0;
        st.window_count += 1;
        let elapsed = st.window_start.elapsed();
        let mut changed = false;
        if st.window_count > RATE_HIGH * 2 || elapsed >= VARDIFF_WINDOW {
            let scaled = st.window_count as f64 * VARDIFF_WINDOW.as_secs_f64() / elapsed.as_secs_f64().max(1.0);
            let old = st.e;
            if scaled > RATE_HIGH as f64 && st.e < sh.cfg.max_e {
                // Harder: raise the exponent by log2 of how far above the aim the rate is.
                let steps = ((scaled / 6.0).log2().floor() as u32).clamp(1, sh.cfg.max_e - st.e);
                st.e += steps;
            } else if scaled < RATE_LOW as f64 && st.e > sh.cfg.min_e {
                st.e -= 1;
            }
            changed = st.e != old;
            st.window_start = Instant::now();
            st.window_count = 0;
        }
        changed
    };
    if changed {
        if let Some(t) = sh.latest() {
            push_job(sess, &t);
        }
    }
}

// ------------------------------------------------------------------------------------- stats

/// Reads the fees from <data>/pool-config.json: {"fee_percent": 0.0, "solo_fee_percent": 0.0}.
/// A missing file is created with 0 % for both. A broken file or out-of-range value keeps the
/// current fees and is logged, so a typo never changes the fee by accident.
fn load_fees(sh: &Shared) {
    let path = sh.cfg.data.join("pool-config.json");
    if !path.exists() {
        let _ = std::fs::write(&path, "{\n  \"fee_percent\": 0.0,\n  \"solo_fee_percent\": 0.0\n}\n");
    }
    let parsed = std::fs::read_to_string(&path).ok().and_then(|t| serde_json::from_str::<Value>(&t).ok());
    let Some(cfg) = parsed else {
        sh.log("pool-config.json does not parse; fees unchanged");
        return;
    };
    for (key, slot) in [("fee_percent", &sh.fee_bp), ("solo_fee_percent", &sh.solo_fee_bp)] {
        let Some(pct) = cfg.get(key).and_then(|v| v.as_f64()) else { continue };
        if !(0.0..=10.0).contains(&pct) {
            sh.log(&format!("pool-config.json: {key} {pct} outside 0..10 %; unchanged"));
            continue;
        }
        let bp = (pct * 100.0).round() as u32;
        let old = slot.swap(bp, Ordering::Relaxed);
        if old != bp {
            sh.log(&format!("{key} is now {:.2} % (was {:.2} %)", bp as f64 / 100.0, old as f64 / 100.0));
        }
    }
}

fn stats_loop(sh: Arc<Shared>) {
    loop {
        thread::sleep(Duration::from_secs(10));
        load_fees(&sh);
        let mut by_addr: HashMap<String, (f64, HashMap<String, f64>, bool)> = HashMap::new();
        let total = {
            let mut recent = sh.recent.lock().unwrap();
            while recent.front().is_some_and(|r| r.0.elapsed() > HASHRATE_WINDOW) {
                recent.pop_front();
            }
            let mut total = 0.0;
            for (_, a, w, work, solo) in recent.iter() {
                total += work;
                let e = by_addr.entry(a.clone()).or_default();
                e.0 += work;
                *e.1.entry(w.clone()).or_default() += work;
                e.2 |= *solo;
            }
            total
        };
        // Until the pool has run a full window, divide by its uptime so the first minutes aren't understated.
        let secs = HASHRATE_WINDOW.as_secs_f64().min((now() - sh.started).max(60) as f64);
        let t = sh.latest();
        let miners: Vec<Value> = by_addr
            .iter()
            .map(|(a, (w, ws, solo))| {
                json!({"addr": a, "hashrate": w / secs, "solo": solo,
                       "workers": ws.iter().map(|(n, x)| json!({"name": n, "hashrate": x / secs})).collect::<Vec<_>>()})
            })
            .collect();
        let peers = sh.peers.load(Ordering::Relaxed);
        let st = json!({
            "updated": now(), "started": sh.started, "name": sh.cfg.name,
            "node_online": sh.node_up.load(Ordering::Relaxed) && sh.work_up.load(Ordering::Relaxed),
            "peer_online": peers > 0, "peers": peers,
            "sessions": sh.sessions.lock().unwrap().len(),
            "hashrate": total / secs, "unit": "H/s",
            "fee_percent": sh.fee_bp.load(Ordering::Relaxed) as f64 / 100.0,
            "solo_fee_percent": sh.solo_fee_bp.load(Ordering::Relaxed) as f64 / 100.0,
            "height": t.as_ref().map(|t| t.height),
            "difficulty": t.as_ref().map(|t| t.difficulty),
            "network_work_per_block": t.as_ref().map(|t| t.difficulty as f64),
            "round_effort": t.as_ref().map(|t| *sh.round_work.lock().unwrap() / t.difficulty.max(1) as f64),
            "seed_hash": t.as_ref().map(|t| hex(&t.seed)),
            "accepted": sh.counters.accepted.load(Ordering::Relaxed),
            "rejected": sh.counters.rejected.load(Ordering::Relaxed),
            "blocks_submitted": sh.counters.blocks_submitted.load(Ordering::Relaxed),
            "blocks_accepted": sh.counters.blocks_accepted.load(Ordering::Relaxed),
            "miners": miners,
            "job_age_s": t.as_ref().map(|t| t.created.elapsed().as_secs()),
        });
        let tmp = sh.cfg.data.join("stats.json.tmp");
        if std::fs::write(&tmp, serde_json::to_vec_pretty(&st).unwrap()).is_ok() {
            let _ = std::fs::rename(&tmp, sh.cfg.data.join("stats.json"));
        }
    }
}

/// The round's work so far, rebuilt from the share log so a restart does not reset the round effort: the work of
/// every share after the last block share (the same rule the live count uses).
fn round_work_from_log(path: &std::path::Path) -> f64 {
    let Ok(f) = File::open(path) else { return 0.0 };
    let mut work = 0.0;
    for line in BufReader::new(f).lines().map_while(Result::ok) {
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        work += v.get("work").and_then(Value::as_f64).unwrap_or(0.0);
        if v.get("block").and_then(Value::as_bool) == Some(true) {
            work = 0.0;
        }
    }
    work
}

fn main() {
    let cfg = config();
    std::fs::create_dir_all(&cfg.data).expect("create the data directory");
    let open = |n: &str| OpenOptions::new().create(true).append(true).open(cfg.data.join(n)).expect("open log file");
    let files = (open("shares.jsonl"), open("blocks.jsonl"), open("pool.log"));
    let rpc = Rpc::new(&cfg.node_rpc).unwrap_or_else(|e| panic!("{e}"));
    let verifier = Verifier::new(cfg.verify_threads);
    let seed_en = (now() as u32).wrapping_mul(2_654_435_761) ^ std::process::id();
    let sh = Arc::new(Shared {
        rpc,
        verifier,
        templates: Mutex::new(VecDeque::new()),
        caches: Mutex::new(Vec::new()),
        seen: Mutex::new(HashMap::new()),
        sessions: Mutex::new(Vec::new()),
        recent: Mutex::new(VecDeque::new()),
        next_session: AtomicU64::new(1),
        next_template: AtomicU64::new(1),
        next_en: AtomicU32::new(seed_en),
        node_up: AtomicBool::new(false),
        work_up: AtomicBool::new(false),
        peers: AtomicU64::new(0),
        fee_bp: AtomicU32::new(0),
        solo_fee_bp: AtomicU32::new(0),
        round_work: Mutex::new(round_work_from_log(&cfg.data.join("shares.jsonl"))),
        counters: Counters::default(),
        files: Mutex::new(files),
        started: now(),
        cfg,
    });
    load_fees(&sh);
    sh.log(&format!(
        "{} {} starting: node {}, share difficulty 2^{} (2^{}..2^{}), {} verifier threads{}",
        sh.cfg.name,
        env!("CARGO_PKG_VERSION"),
        sh.cfg.node_rpc,
        sh.cfg.start_e,
        sh.cfg.min_e,
        sh.cfg.max_e,
        sh.cfg.verify_threads,
        if sh.cfg.require_peers { "" } else { ", serving work without peers (test chain)" }
    ));
    {
        let s = sh.clone();
        thread::spawn(move || node_loop(s));
    }
    {
        let s = sh.clone();
        thread::spawn(move || stats_loop(s));
    }
    let accept = |addr: String, solo: bool, sh: Arc<Shared>| {
        let listener = TcpListener::bind(&addr).unwrap_or_else(|e| panic!("listen {addr}: {e}"));
        sh.log(&format!("stratum (rx/0) listening on {addr} ({})", if solo { "solo" } else { "PPLNS" }));
        for conn in listener.incoming() {
            match conn {
                Ok(s) => {
                    let sh2 = sh.clone();
                    thread::spawn(move || serve_miner(sh2, s, solo));
                }
                Err(e) => sh.log(&format!("accept failed: {e}")),
            }
        }
    };
    {
        let (sh2, addr) = (sh.clone(), sh.cfg.listen_solo.clone());
        thread::spawn(move || accept(addr, true, sh2));
    }
    accept(sh.cfg.listen.clone(), false, sh.clone());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_work_restarts_after_the_last_block() {
        let path = std::env::temp_dir().join(format!("peer-round-work-{}.jsonl", std::process::id()));
        std::fs::write(
            &path,
            concat!(
                "{\"work\":2.0,\"block\":false}\n",
                "{\"work\":3.0,\"block\":true}\n",
                "{\"work\":1.5,\"block\":false}\n",
                "not json\n",
                "{\"work\":4.0,\"block\":false}\n",
            ),
        )
        .unwrap();
        let work = round_work_from_log(&path);
        let _ = std::fs::remove_file(&path);
        assert_eq!(work, 5.5);
        assert_eq!(round_work_from_log(&path), 0.0);
    }
}
