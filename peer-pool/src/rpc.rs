//! A small HTTP/1.1 JSON-RPC client for the node's local RPC (plain http only: the node's RPC is
//! bound to 127.0.0.1 and never public).

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct Rpc {
    host: String, // host:port
    path: String,
}

impl Rpc {
    /// `url` like http://127.0.0.1:8545 or http://127.0.0.1:8545/path.
    pub fn new(url: &str) -> Result<Rpc, String> {
        let rest = url.strip_prefix("http://").ok_or("node RPC URL must start with http://")?;
        let (host, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        Ok(Rpc { host: host.to_string(), path: path.to_string() })
    }

    pub fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
        let mut s = TcpStream::connect(&self.host).map_err(|e| format!("connect {}: {e}", self.host))?;
        let _ = s.set_read_timeout(Some(Duration::from_secs(20)));
        let _ = s.set_write_timeout(Some(Duration::from_secs(20)));
        let host_name = self.host.split(':').next().unwrap_or("localhost");
        let req = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.path,
            if host_name == "127.0.0.1" { "localhost" } else { host_name },
            body.len(),
            body
        );
        s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        s.read_to_end(&mut raw).map_err(|e| e.to_string())?;
        let split = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("bad HTTP response")?;
        let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
        let mut payload = raw[split + 4..].to_vec();
        if !head.starts_with("http/1.1 200") && !head.starts_with("http/1.0 200") {
            return Err(format!("HTTP {}", head.lines().next().unwrap_or("")));
        }
        if head.contains("transfer-encoding: chunked") {
            payload = dechunk(&payload).ok_or("bad chunked body")?;
        }
        let v: Value = serde_json::from_slice(&payload).map_err(|e| format!("bad JSON: {e}"))?;
        if let Some(e) = v.get("error").filter(|e| !e.is_null()) {
            return Err(format!("{method}: {e}"));
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn dechunk(mut b: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let eol = b.windows(2).position(|w| w == b"\r\n")?;
        let size_txt = std::str::from_utf8(&b[..eol]).ok()?;
        let size = usize::from_str_radix(size_txt.split(';').next()?.trim(), 16).ok()?;
        b = &b[eol + 2..];
        if size == 0 {
            return Some(out);
        }
        out.extend_from_slice(b.get(..size)?);
        b = b.get(size + 2..)?;
    }
}

/// Parses a 0x-prefixed hex quantity.
pub fn quantity(v: &Value) -> Option<u64> {
    u64::from_str_radix(v.as_str()?.strip_prefix("0x")?, 16).ok()
}

/// Parses 0x + 64 hex digits.
pub fn word(v: &Value) -> Option<[u8; 32]> {
    crate::common::unhex32(v.as_str()?.strip_prefix("0x")?)
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunks() {
        assert_eq!(super::dechunk(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n").unwrap(), b"abcde");
    }
}
