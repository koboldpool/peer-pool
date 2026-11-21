#!/usr/bin/env python3
"""PPLNS payouts for peer-pool: credits pool blocks still on the best chain after --confirmations (cross-checked with
--ref-rpc) over the last WINDOW x block difficulty of shares, and with --pay sends balances above --min as transfers
signed offline by peercash-signer (password from PEER_WALLET_PASSWORD). Ledger: <data>/payouts.json (wei, lowercase).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

WEI = 10 ** 18                # wei per PEER
BASE_REWARD = WEI             # 1 PEER per block (consensus/randomx calcBlockReward)
HALVING = 10_500_000
PAYMENT_CONFIRMATIONS = 30
REPLACE_AFTER = 20            # blocks a payment may wait unmined before it is re-signed with a higher tip
GAS = 21_000


def rpc(url: str, method: str, *params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json",
                                                          "user-agent": "peer-pool-payout"})
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read())
    if out.get("error"):
        raise RuntimeError(f"{method}: {out['error']}")
    return out["result"]


def block_reward(height: int) -> int:
    era = height // HALVING
    return 0 if era >= 64 else BASE_REWARD >> era


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # a line cut by a crash
    return out


def fmt(wei: int) -> str:
    return f"{wei / WEI:.8f} PEER"


class Wallet:
    def __init__(self, args):
        self.args = args
        ks = sorted(Path(args.keystore_dir).glob("UTC--*")) if args.keystore_dir else []
        self.keystore = args.keystore or (str(ks[0]) if ks else "")
        if not self.keystore:
            raise SystemExit("no keystore file (--keystore or --keystore-dir)")
        p = subprocess.run([args.signer, "address", self.keystore], capture_output=True, text=True, timeout=60)
        if p.returncode != 0:
            raise SystemExit(f"peercash-signer address failed: {p.stderr.strip()}")
        self.address = p.stdout.strip().lower()

    def sign(self, nonce: int, to: str, value: int, tip: int, max_fee: int) -> dict:
        p = subprocess.run([self.args.signer, "sign", self.keystore, str(self.args.chain_id), str(nonce), to,
                            str(value), str(tip), str(max_fee), str(GAS)],
                           capture_output=True, text=True, timeout=120)
        if p.returncode != 0:
            raise RuntimeError(f"peercash-signer: {p.stderr.strip()[-300:]}")
        out = json.loads(p.stdout)
        if out["from"].lower() != self.address:
            raise RuntimeError("signer used a different key than the pool wallet")
        return out


def fees(args) -> tuple[int, int]:
    """(tip, max fee per gas) in wei: tip from --tip-wei or the node's suggestion, max fee = 2 x base fee + tip."""
    latest = rpc(args.rpc, "eth_getBlockByNumber", "latest", False)
    base = int(latest.get("baseFeePerGas") or "0x0", 16)
    if args.tip_wei is not None:
        tip = args.tip_wei
    else:
        try:
            tip = int(rpc(args.rpc, "eth_maxPriorityFeePerGas"), 16)
        except Exception:
            tip = 1_000_000
    tip = max(tip, 1)
    return tip, 2 * base + tip


def receipt(args, txid: str):
    return rpc(args.rpc, "eth_getTransactionReceipt", txid)


def reconcile(args, ledger: dict, tip_height: int, wallet: Wallet | None) -> None:
    """Moves pending payments to confirmed / failed, and re-signs stuck ones (only with --pay)."""
    pending = [p for p in ledger["payments"] if p.get("status", "pending") == "pending"]
    if not pending:
        return
    used = int(rpc(args.rpc, "eth_getTransactionCount", pending[0]["from"], "latest"), 16)
    for p in pending:
        hashes = [p["txid"]] + p.get("replaced", [])
        rc = next((r for r in (receipt(args, h) for h in hashes) if r), None)
        if rc:
            mined = int(rc["blockNumber"], 16)
            p["txid"] = rc["transactionHash"]
            p["mined_height"] = mined
            if int(rc.get("status", "0x1"), 16) != 1:
                fail(ledger, p, "reverted")
            elif tip_height - mined + 1 >= PAYMENT_CONFIRMATIONS:
                p["status"] = "confirmed"
                print(f"payment {p['txid'][:18]} to {p['addr']} confirmed at height {mined}")
            continue
        if used > p["nonce"]:
            # The nonce was used, but by none of our transactions for this payment.
            fail(ledger, p, f"nonce {p['nonce']} used by another transaction")
            continue
        if wallet and tip_height - p.get("height_sent", tip_height) >= REPLACE_AFTER:
            tip, max_fee = fees(args)
            tip = max(tip, p.get("tip", 1) * 2)
            max_fee = max(max_fee, p.get("max_fee", 0) * 2, 2 * tip)
            tx = wallet.sign(p["nonce"], p["addr"], p["amount"], tip, max_fee)
            try:
                rpc(args.rpc, "eth_sendRawTransaction", tx["raw"])
            except RuntimeError as e:
                print(f"replacement for {p['txid'][:18]} not accepted: {e}")
                continue
            p.setdefault("replaced", []).append(p["txid"])
            p.update(txid=tx["hash"], tip=tip, max_fee=max_fee, height_sent=tip_height)
            print(f"payment to {p['addr']} re-sent at nonce {p['nonce']} with tip {tip} wei: {tx['hash']}")


def fail(ledger: dict, p: dict, why: str) -> None:
    p["status"] = "failed"
    p["failed_reason"] = why
    ledger["balances"][p["addr"]] = ledger["balances"].get(p["addr"], 0) + p["amount"]
    ledger["paid"][p["addr"]] = ledger["paid"].get(p["addr"], 0) - p["amount"]
    print(f"PAYMENT FAILED ({why}): {fmt(p['amount'])} to {p['addr']}; credited back to the balance")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="pool-data")
    ap.add_argument("--rpc", default="http://127.0.0.1:8545")
    ap.add_argument("--ref-rpc", default="", help="an independent node; a block is credited only if it has the same hash")
    ap.add_argument("--signer", default="peercash-signer")
    ap.add_argument("--keystore", default="", help="pool wallet keystore file")
    ap.add_argument("--keystore-dir", default="", help="directory holding the pool keystore (first UTC--* file)")
    ap.add_argument("--chain-id", type=int, default=620156)
    ap.add_argument("--confirmations", type=int, default=200)
    ap.add_argument("--fee", type=float, default=0.0, help="fee in percent for block records without fee_bp")
    ap.add_argument("--window", type=float, default=2.0, help="PPLNS window in block difficulties")
    ap.add_argument("--min", type=float, default=0.1, help="minimum payout in PEER (a transfer costs about 0.00000003 PEER)")
    ap.add_argument("--reserve", type=float, default=0.01, help="PEER always left in the wallet for fees")
    ap.add_argument("--tip-wei", type=int, default=None, help="priority fee per gas (default: the node's suggestion)")
    ap.add_argument("--pay", action="store_true", help="actually send payments")
    args = ap.parse_args()

    data = Path(args.data)
    ledger_path = data / "payouts.json"
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {
        "credited": {}, "orphaned": {}, "balances": {}, "paid": {}, "payments": [], "pool_fees": 0}

    def save():
        tmp = ledger_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(ledger, indent=1))
        tmp.replace(ledger_path)

    wallet = Wallet(args) if (args.keystore or args.keystore_dir) else None
    tip_height = int(rpc(args.rpc, "eth_blockNumber"), 16)
    reconcile(args, ledger, tip_height, wallet if args.pay else None)
    save()

    events = read_jsonl(data / "blocks.jsonl")
    found = {e["id"]: e for e in events if e.get("event") == "found"}
    accepted = {e["id"] for e in events if e.get("event") == "result" and e.get("accepted")}
    shares = read_jsonl(data / "shares.jsonl")

    for bid, blk in sorted(found.items(), key=lambda kv: kv[1]["height"]):
        if bid in ledger["credited"] or bid in ledger["orphaned"] or bid not in accepted:
            continue
        h = blk["height"]
        if tip_height - h < args.confirmations:
            continue  # not deep enough yet
        onchain = rpc(args.rpc, "eth_getBlockByNumber", hex(h), False) or {}
        if (onchain.get("hash") or "").lower() != bid.lower():
            ledger["orphaned"][bid] = {"height": h, "ts": blk["ts"]}
            print(f"block {h} {bid[:18]} is not on the best chain: orphaned, nothing credited")
            save()
            continue
        if wallet and (onchain.get("miner") or "").lower() != wallet.address:
            print(f"block {h} {bid[:18]} pays {onchain.get('miner')}, not the pool wallet: skipped")
            continue
        if args.ref_rpc:
            try:
                ref = rpc(args.ref_rpc, "eth_getBlockByNumber", hex(h), False) or {}
            except Exception as e:
                print(f"reference node not reachable ({e}); nothing credited this run")
                break
            if (ref.get("hash") or "").lower() != bid.lower():
                print(f"block {h} {bid[:18]}: the reference node has {ref.get('hash')} at this height; "
                      f"not credited (our node may be on a fork)")
                continue
        reward = block_reward(h)
        fee_bp = int(blk["fee_bp"]) if "fee_bp" in blk else int(round(args.fee * 100))
        fee = reward * fee_bp // 10_000
        to_split = reward - fee
        mode = blk.get("mode", "pplns")
        work_by: dict[str, float] = {}
        total = 0.0
        if mode == "solo":
            work_by[blk["finder"].split(".", 1)[0].lower()] = 1.0
            total = 1.0
        else:
            window = args.window * float(blk.get("block_work") or 0)
            for s in reversed(shares):
                if s["ts"] > blk["ts"] or s.get("mode", "pplns") == "solo":
                    continue
                if window and total >= window:
                    break
                a = s["addr"].lower()
                work_by[a] = work_by.get(a, 0.0) + s["work"]
                total += s["work"]
        if total <= 0:
            print(f"block {h}: no shares in the window, kept as pool income")
            ledger["pool_fees"] += reward
        else:
            given = 0
            for addr, w in work_by.items():
                part = int(to_split * w / total)
                ledger["balances"][addr] = ledger["balances"].get(addr, 0) + part
                given += part
            ledger["pool_fees"] += reward - given
        ledger["credited"][bid] = {"height": h, "reward": reward, "fee": fee, "fee_bp": fee_bp, "mode": mode,
                                   "miners": len(work_by), "ts": blk["ts"]}
        print(f"block {h} credited ({mode}, fee {fee_bp / 100:.2f} %): {fmt(reward)} over {len(work_by)} miners")
        save()

    min_wei = int(args.min * WEI)
    due = {a: b for a, b in ledger["balances"].items() if b >= min_wei}
    print(f"tip {tip_height}; {len(ledger['credited'])} blocks credited, {len(ledger['orphaned'])} orphaned; "
          f"{len(due)} balances due (>= {args.min} PEER); owed in total {fmt(sum(ledger['balances'].values()))}")
    for a, b in sorted(ledger["balances"].items(), key=lambda kv: -kv[1]):
        print(f"  {a}  {fmt(b)}{'  (due)' if a in due else ''}")
    if not args.pay or not due:
        save()
        return 0
    if not wallet:
        print("--pay needs --keystore or --keystore-dir")
        return 1

    balance = int(rpc(args.rpc, "eth_getBalance", wallet.address, "latest"), 16)
    in_flight = sum(p["amount"] for p in ledger["payments"] if p.get("status") == "pending")
    tip, max_fee = fees(args)
    available = balance - in_flight - int(args.reserve * WEI)
    nonce = int(rpc(args.rpc, "eth_getTransactionCount", wallet.address, "pending"), 16)
    for addr, amount in sorted(due.items(), key=lambda kv: -kv[1]):
        cost = amount + GAS * max_fee
        if cost > available:
            print(f"wallet can spend {fmt(max(available, 0))}, not enough for {fmt(amount)} to {addr}; stopping")
            break
        tx = wallet.sign(nonce, addr, amount, tip, max_fee)
        try:
            sent = rpc(args.rpc, "eth_sendRawTransaction", tx["raw"])
        except RuntimeError as e:
            print(f"payment to {addr} not accepted by the node: {e}")
            break
        ledger["balances"][addr] -= amount
        ledger["paid"][addr] = ledger["paid"].get(addr, 0) + amount
        ledger["payments"].append({"ts": int(time.time()), "addr": addr, "amount": amount, "txid": sent,
                                   "from": wallet.address, "nonce": nonce, "tip": tip, "max_fee": max_fee,
                                   "status": "pending", "height_sent": tip_height})
        save()
        print(f"paid {fmt(amount)} to {addr} (nonce {nonce}): {sent}")
        available -= cost
        nonce += 1
    save()
    return 0


if __name__ == "__main__":
    sys.exit(main())
