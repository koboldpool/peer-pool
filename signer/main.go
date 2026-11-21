// peercash-signer: signs PeerCash (EIP-1559) transfers offline for payout.py (the node has no personal API).
//	peercash-signer address <keystore-file>
//	peercash-signer sign <keystore-file> <chain-id> <nonce> <to> <value-wei> <tip-wei> <max-fee-wei> [gas]
// Password from PEER_WALLET_PASSWORD; sign prints {"from", "hash", "raw"} as one JSON line.
package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

func fail(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}

func big10(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok || v.Sign() < 0 {
		fail("not a non-negative integer: %q", s)
	}
	return v
}

func main() {
	if len(os.Args) < 3 {
		fail("usage: peercash-signer address <keystore> | sign <keystore> <chain-id> <nonce> <to> <value-wei> <tip-wei> <max-fee-wei> [gas]")
	}
	blob, err := os.ReadFile(os.Args[2])
	if err != nil {
		fail("read keystore: %v", err)
	}
	switch os.Args[1] {
	case "address":
		var k struct {
			Address string `json:"address"`
		}
		if json.Unmarshal(blob, &k) != nil || !common.IsHexAddress(k.Address) {
			fail("keystore has no address")
		}
		fmt.Println(common.HexToAddress(k.Address).Hex())
	case "sign":
		if len(os.Args) < 9 {
			fail("sign needs: <keystore> <chain-id> <nonce> <to> <value-wei> <tip-wei> <max-fee-wei> [gas]")
		}
		key, err := keystore.DecryptKey(blob, os.Getenv("PEER_WALLET_PASSWORD"))
		if err != nil {
			fail("unlock keystore: %v", err)
		}
		if !common.IsHexAddress(os.Args[5]) {
			fail("bad recipient %q", os.Args[5])
		}
		nonce, err := strconv.ParseUint(os.Args[4], 10, 64)
		if err != nil {
			fail("bad nonce")
		}
		gas := uint64(21000)
		if len(os.Args) > 9 {
			if gas, err = strconv.ParseUint(os.Args[9], 10, 64); err != nil {
				fail("bad gas")
			}
		}
		chainID := big10(os.Args[3])
		to := common.HexToAddress(os.Args[5])
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID: chainID, Nonce: nonce, GasTipCap: big10(os.Args[7]), GasFeeCap: big10(os.Args[8]),
			Gas: gas, To: &to, Value: big10(os.Args[6]),
		})
		signed, err := types.SignTx(tx, types.NewLondonSigner(chainID), key.PrivateKey)
		if err != nil {
			fail("sign: %v", err)
		}
		raw, _ := signed.MarshalBinary()
		out, _ := json.Marshal(map[string]string{"from": key.Address.Hex(), "hash": signed.Hash().Hex(), "raw": hexutil.Encode(raw)})
		fmt.Println(string(out))
	default:
		fail("unknown command %q", os.Args[1])
	}
}
