# peer-pool

The mining pool software behind [Kobold Pool](https://koboldpool.com/peer/) for **PeerCash (PEER)**, a RandomX (rx/0)
coin ([github.com/PeerCashChain/PeerCashChain](https://github.com/PeerCashChain/PeerCashChain)). Published so miners can
check how their shares are counted and paid.

| Part | What it does |
|---|---|
| `peer-pool/` | the pool server (Rust): Monero-style stratum for stock XMRig, work from the node's `eth_getWork`, every share re-checked with RandomX |
| `payout.py` | PPLNS payouts; blocks cross-checked with a second node before they are credited |
| `signer/` | `peercash-signer` (Go): signs payout transfers offline from the pool's keystore (the node has no personal API) |
| `test/` | test vectors made with the PeerCash node's own hasher, including real mainnet blocks |
| `deploy/` | example systemd units for node, pool and payouts |

## Mine on Kobold Pool

Any RandomX miner that speaks the Monero stratum works, e.g. stock XMRig:

```
xmrig -a rx/0 -o peer.koboldpool.com:3500 -u 0xYOUR_PEERCASH_ADDRESS.rig1
```

Port 3500 is PPLNS, 3510 solo (or the password `solo`). PeerCash addresses are Ethereum-style (`0x…`); any
Ethereum wallet works (network: RPC `https://rpc.peercash.io`, chain ID 620156, symbol PEER).

## How shares and payouts work

- **Work.** The pool takes `eth_getWork` (seal hash, epoch seed, target) from its own node and gives every connection
  its own job; the miner's nonce goes into the RandomX input exactly as the node builds it.
- **Share check.** Every submit is recomputed with RandomX (light mode, keyed by the epoch seed) and counted only if it
  meets the connection's target. A result that also meets the network target goes to the node with `eth_submitWork`.
- **Difficulty.** Per-connection vardiff aims at 3–12 shares a minute; shares count by their **work**.
- **No peers, no work.** The pool holds jobs while its node has no peers, so it never mines a private fork.
- **PPLNS.** A block is credited once it is `--confirmations` deep (default 200) and, with `--ref-rpc`, the same block is
  on a second, independent node. Its reward (minus that block's fee) is split over the pool shares in the last
  2 × (block difficulty) before it, by their work. Solo blocks go to their finder, minus the solo fee.
- **Payments.** Balances at or above `--min` (default 0.1 PEER) are sent as EIP-1559 transfers signed by
  `peercash-signer` and written to the ledger (`<data>/payouts.json`) before the next one; one that disappears is
  credited back.
- **Fees.** `<data>/pool-config.json`, `{"fee_percent": 0.0, "solo_fee_percent": 0.0}`.

## Build

RandomX first (the pool links `librandomx.a`), then the pool and the signer:

```
git clone https://github.com/tevador/RandomX && cd RandomX && git checkout v1.2.3
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
cd ../peer-pool && RANDOMX_LIB_DIR=../RandomX/build cargo build --release --locked
RANDOMX_LIB_DIR=../RandomX/build cargo test --release --locked     # the vectors in test/
cd .. && python3 -m unittest test_payout
cd signer && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o peercash-signer .
```

GitHub Actions (`.github/workflows/build.yml`) does all of this on every push and attaches the Linux binaries with
`SHA256SUMS` to each release (tag `v*`).

## Run

1. A PeerCash node with its HTTP RPC on 127.0.0.1:8545 and `--miner.etherbase` set to the pool address. Its built-in
   bootnode is the **testnet** node: give it the mainnet peers from the official wallet (`--networkid 620156`,
   `--bootnodes` and `--staticnodes`, see `deploy/peercash-node.service`). Let it sync.
2. The pool wallet: `peercash account new --datadir <wallet dir> --password <file>`; its address is the etherbase.
3. The pool:

   ```
   peer-pool --node-rpc http://127.0.0.1:8545 --data pool-data --listen 0.0.0.0:3500 --listen-solo 0.0.0.0:3510 \
             --verify-threads 2
   ```

4. Payouts every 30 minutes or so; without `--pay` it only reports:

   ```
   PEER_WALLET_PASSWORD=... python3 -I payout.py --data pool-data --rpc http://127.0.0.1:8545 \
       --ref-rpc https://rpc.peercash.io --signer peercash-signer --keystore-dir <wallet dir>/keystore \
       --chain-id 620156 --confirmations 200 --min 0.1 --pay
   ```

## License

Apache License 2.0 (`LICENSE`). RandomX is BSD 3-Clause; `peercash-signer` contains go-ethereum (LGPL v3). See `NOTICE`.
