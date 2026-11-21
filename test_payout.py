"""Exact checks of payout.py against a fake node: crediting (per-block fees, solo blocks, PPLNS without
solo shares, orphans, refused blocks, depth, wrong coinbase, reference-node mismatch), paying
(sequential nonces, wallet limit) and reconciling (confirmed, nonce taken by another transaction,
stuck transaction re-signed at the same nonce).

    python -m unittest test_payout
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import payout

WEI = 10 ** 18
POOL = "0x" + "c5" * 20
ALICE, BOB, CAROL = "0x" + "a1" * 20, "0x" + "b0" * 20, "0x" + "ca" * 20


class FakeNode:
    def __init__(self, tip, chain, miner=POOL):
        self.tip, self.chain, self.miner = tip, chain, miner  # chain: height -> block hash
        self.balance = 100 * WEI
        self.receipts = {}        # tx hash -> receipt
        self.nonce_latest = 0
        self.nonce_pending = 0
        self.sent = []
        self.ref = None           # reference node chain (height -> hash) for --ref-rpc

    def rpc(self, url, method, *params):
        chain = self.ref if url == "ref" else self.chain
        if method == "eth_blockNumber":
            return hex(self.tip)
        if method == "eth_getBlockByNumber":
            if params[0] == "latest":
                return {"baseFeePerGas": hex(7), "hash": "0x" + "ee" * 32}
            h = int(params[0], 16)
            return {"hash": chain.get(h, "0x" + "f" * 64), "miner": self.miner, "number": hex(h)}
        if method == "eth_maxPriorityFeePerGas":
            return hex(1000)
        if method == "eth_getBalance":
            return hex(self.balance)
        if method == "eth_getTransactionCount":
            return hex(self.nonce_pending if params[1] == "pending" else self.nonce_latest)
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        if method == "eth_sendRawTransaction":
            self.sent.append(params[0])
            return "0x" + params[0][2:].rjust(64, "0")[-64:]
        raise AssertionError(f"unexpected RPC {method}")


class FakeWallet:
    address = POOL

    def __init__(self, args):
        self.signed = []

    def sign(self, nonce, to, value, tip, max_fee):
        self.signed.append((nonce, to, value, tip, max_fee))
        raw = f"0x{nonce:04x}{tip:08x}"
        return {"from": POOL, "hash": "0x" + raw[2:].rjust(64, "0"), "raw": raw}


def run_payout(data: Path, node: FakeNode, *extra):
    argv = ["payout.py", "--data", str(data), "--window", "2", "--keystore", "k.json", "--chain-id", "563399", *extra]
    with mock.patch.object(sys, "argv", argv), mock.patch.object(payout, "rpc", node.rpc), \
            mock.patch.object(payout, "Wallet", FakeWallet):
        assert payout.main() == 0
    return json.loads((data / "payouts.json").read_text())


def found(bid, h, ts, finder, **kw):
    return {"event": "found", "id": bid, "height": h, "ts": ts, "finder": finder, "block_work": 1000.0, **kw}


def result(bid, ok=True):
    return {"event": "result", "id": bid, "accepted": ok}


def share(ts, addr, work, mode="pplns"):
    return {"ts": ts, "addr": addr, "worker": "w", "work": work, "mode": mode}


H = lambda c: "0x" + c * 64


class CreditTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.data = Path(self.tmp.name)
        blocks = [
            found(H("a"), 10, 1000, ALICE + ".rig", mode="pplns", fee_bp=0), result(H("a")),
            found(H("b"), 20, 2000, CAROL + ".gpu", mode="pplns", fee_bp=200), result(H("b")),
            found(H("c"), 25, 2500, BOB + ".solo1", mode="solo", fee_bp=150), result(H("c")),
            found(H("d"), 30, 3000, ALICE + ".rig"), result(H("d")),  # old record: no mode/fee_bp
            found(H("e"), 40, 3500, ALICE + ".rig", mode="pplns", fee_bp=200), result(H("e")),  # orphaned
            found(H("9"), 45, 3600, CAROL + ".gpu", mode="pplns", fee_bp=200), result(H("9"), False),  # refused
            found(H("8"), 300, 9000, CAROL + ".gpu", mode="pplns", fee_bp=0), result(H("8")),  # not deep yet
        ]
        (d / "blocks.jsonl").write_text("\n".join(json.dumps(b) for b in blocks) + "\n")
        shares = [share(900, ALICE, 300), share(950, CAROL, 100), share(1900, BOB, 1000, "solo"),
                  share(1900, ALICE, 100), share(1950, CAROL, 300)]
        (d / "shares.jsonl").write_text("\n".join(json.dumps(s) for s in shares) + "\n")
        self.node = FakeNode(400, {10: H("a"), 20: H("b"), 25: H("c"), 30: H("d"), 40: H("0"), 45: H("9"),
                                   300: H("8")})

    def tearDown(self):
        self.tmp.cleanup()

    def test_credit(self):
        led = run_payout(self.data, self.node)
        self.assertEqual(set(led["credited"]), {H("a"), H("b"), H("c"), H("d")})
        self.assertEqual(set(led["orphaned"]), {H("e")})
        # Block a (fee 0): ALICE 300, CAROL 100 of 400.
        # Block b (2 %): 0.98 PEER over ALICE 100+300, CAROL 300+100 (solo share left out) -> half each.
        # Block c (solo, 1.5 %): BOB gets 0.985.  Block d (no fee_bp, --fee 0): same window as b -> half each.
        b = led["balances"]
        self.assertEqual(b[ALICE], WEI * 3 // 4 + (WEI * 98 // 100) // 2 + WEI // 2)
        self.assertEqual(b[CAROL], WEI // 4 + (WEI * 98 // 100) // 2 + WEI // 2)
        self.assertEqual(b[BOB], WEI * 985 // 1000)
        self.assertEqual(led["credited"][H("b")]["fee"], WEI * 2 // 100)
        # Run again: nothing is credited twice.
        led2 = run_payout(self.data, self.node)
        self.assertEqual(led2["balances"], b)

    def test_depth(self):
        led = run_payout(self.data, self.node, "--confirmations", "380")
        self.assertEqual(set(led["credited"]), {H("a"), H("b")})  # tip 400: only heights <= 20 are 380 deep
        self.assertEqual(led["orphaned"], {})

    def test_wrong_coinbase(self):
        self.node.miner = "0x" + "11" * 20
        led = run_payout(self.data, self.node)
        self.assertEqual(led["credited"], {})
        self.assertEqual(set(led["orphaned"]), {H("e")})

    def test_reference_node(self):
        self.node.ref = dict(self.node.chain)
        self.node.ref[20] = H("7")  # the reference node has another block at 20
        led = run_payout(self.data, self.node, "--ref-rpc", "ref")
        self.assertEqual(set(led["credited"]), {H("a"), H("c"), H("d")})


class PayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = Path(self.tmp.name)
        ledger = {"credited": {}, "orphaned": {}, "balances": {ALICE: 3 * WEI, BOB: 2 * WEI, CAROL: WEI // 2},
                  "paid": {}, "payments": [], "pool_fees": 0}
        (self.data / "payouts.json").write_text(json.dumps(ledger))
        self.node = FakeNode(1000, {})
        self.node.nonce_pending = self.node.nonce_latest = 5

    def tearDown(self):
        self.tmp.cleanup()

    def test_pay_sequential_nonces(self):
        led = run_payout(self.data, self.node, "--pay", "--min", "1")
        pays = led["payments"]
        self.assertEqual([(p["addr"], p["amount"], p["nonce"]) for p in pays], [(ALICE, 3 * WEI, 5), (BOB, 2 * WEI, 6)])
        self.assertEqual(led["balances"], {ALICE: 0, BOB: 0, CAROL: WEI // 2})  # CAROL below --min 1 (0.5)
        self.assertEqual(led["paid"], {ALICE: 3 * WEI, BOB: 2 * WEI})
        self.assertTrue(all(p["max_fee"] == 2 * 7 + 1000 and p["tip"] == 1000 for p in pays))

    def test_wallet_limit(self):
        self.node.balance = 4 * WEI  # minus 0.01 reserve: ALICE fits, BOB does not
        led = run_payout(self.data, self.node, "--pay", "--min", "1")
        self.assertEqual([p["addr"] for p in led["payments"]], [ALICE])
        self.assertEqual(led["balances"][BOB], 2 * WEI)

    def test_reconcile(self):
        led = run_payout(self.data, self.node, "--pay", "--min", "1")
        a, b = led["payments"]
        # ALICE's payment mined at 1000; BOB's never mined, and nonce 6 got used by another transaction.
        self.node.receipts[a["txid"]] = {"blockNumber": hex(1000), "status": "0x1", "transactionHash": a["txid"]}
        self.node.nonce_latest = 7
        self.node.tip = 1010
        led = run_payout(self.data, self.node)
        self.assertEqual(led["payments"][0]["status"], "pending")  # 11 deep, needs 30
        self.assertEqual(led["payments"][1]["status"], "failed")
        self.assertEqual(led["balances"][BOB], 2 * WEI)
        self.assertEqual(led["paid"][BOB], 0)
        self.node.tip = 1029
        led = run_payout(self.data, self.node)
        self.assertEqual(led["payments"][0]["status"], "confirmed")

    def test_replace_stuck(self):
        led = run_payout(self.data, self.node, "--pay", "--min", "2.5")  # only ALICE, nonce 5
        p = led["payments"][0]
        self.node.tip = 1000 + payout.REPLACE_AFTER
        led = run_payout(self.data, self.node, "--pay", "--min", "2.5")
        q = led["payments"][0]
        self.assertEqual(q["nonce"], 5)
        self.assertEqual(q["replaced"], [p["txid"]])
        self.assertEqual(q["tip"], 2000)
        self.assertNotEqual(q["txid"], p["txid"])
        # The original transaction gets mined after all: the payment follows it.
        self.node.receipts[p["txid"]] = {"blockNumber": hex(1001), "status": "0x1", "transactionHash": p["txid"]}
        self.node.tip = 1100
        led = run_payout(self.data, self.node)
        self.assertEqual(led["payments"][0]["status"], "confirmed")
        self.assertEqual(led["payments"][0]["txid"], p["txid"])


if __name__ == "__main__":
    unittest.main()
