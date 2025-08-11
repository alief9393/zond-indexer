package utils

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"

	"zond-indexer/internal/config"
	"zond-indexer/internal/models"
	"zond-indexer/internal/node"

	"github.com/theQRL/go-zond/core/types"
)

// CORRECT AND FINAL STRUCTURE - Mirrors the Beacon Node JSON
type BeaconBlockResponse struct {
	Version string `json:"version"` // <<< ADD THIS FIELD TO MATCH THE JSON
	Data    struct {
		Message struct {
			Slot          string `json:"slot"`
			ProposerIndex string `json:"proposer_index"`
			ParentRoot    string `json:"parent_root"`
			StateRoot     string `json:"state_root"`
			Body          struct {
				RandaoReveal string `json:"randao_reveal"`
				Graffiti     string `json:"graffiti"`
				Eth1Data     struct {
					DepositCount string `json:"deposit_count"`
				} `json:"eth1_data"`
				Deposits []struct {
					Index          string `json:"index"`
					ValidatorIndex string `json:"validator_index"`
					FromAddress    string `json:"from_address"`
					Amount         string `json:"amount"`
					Timestamp      string `json:"timestamp"`
					TxHash         string `json:"tx_hash"`
					PubKey         string `json:"pubkey"`
					Signature      string `json:"signature"`
				} `json:"deposits"`
				ExecutionPayload struct {
					ParentHash       string `json:"parent_hash"`
					FeeRecipient     string `json:"fee_recipient"`
					StateRoot        string `json:"state_root"`
					ReceiptsRoot     string `json:"receipts_root"`
					LogsBloom        string `json:"logs_bloom"`
					PrevRandao       string `json:"prev_randao"`
					BlockNumber      string `json:"block_number"`
					GasLimit         string `json:"gas_limit"`
					GasUsed          string `json:"gas_used"`
					Timestamp        string `json:"timestamp"`
					ExtraData        string `json:"extra_data"`
					BaseFeePerGas    string `json:"base_fee_per_gas"`
					BlockHash        string `json:"block_hash"`
					TransactionsRoot string `json:"transactions_root"`
					PayloadHash      string `json:"payload_hash"`
					Withdrawals      []struct {
						Index          string `json:"index"`
						ValidatorIndex string `json:"validator_index"`
						Address        string `json:"address"`
						Amount         string `json:"amount"`
					} `json:"withdrawals"`
				} `json:"execution_payload"`
			} `json:"body"`
		} `json:"message"`
	} `json:"data"`
}

func BytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func FetchBeaconBlockBySlot(cfg config.Config, slot uint64) (*BeaconBlockResponse, error) {
	url := fmt.Sprintf("%s/zond/v1/beacon/blocks/%d", cfg.BeaconURL(), slot)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to GET beacon block: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read beacon block response: %w", err)
	}

	var beaconResp BeaconBlockResponse
	if err := json.Unmarshal(body, &beaconResp); err != nil {
		return nil, fmt.Errorf("failed to decode beacon response: %w", err)
	}

	return &beaconResp, nil
}

func FindMEVTxHashFromTransactions(baseFee *big.Int, txs types.Transactions) []byte {
	var maxTip = big.NewInt(0)
	var mevTxHash []byte

	for _, tx := range txs {
		tip := new(big.Int).Sub(tx.GasPrice(), baseFee)
		if tip.Sign() < 0 {
			tip = big.NewInt(0)
		}
		tipAmount := new(big.Int).Mul(tip, new(big.Int).SetUint64(tx.Gas()))
		if tipAmount.Cmp(maxTip) > 0 {
			maxTip = tipAmount
			mevTxHash = tx.Hash().Bytes()
		}
	}
	return mevTxHash
}

func HexToBytes(str string) []byte {
	str = strings.TrimPrefix(str, "0x")
	bytes, _ := hex.DecodeString(str)
	return bytes
}

func AddressToBytes(address string) ([]byte, error) {
	clean := strings.TrimPrefix(address, "0x")
	return hex.DecodeString(clean)
}

func HexStringToBytes(hexStr string) ([]byte, error) {
	cleaned := strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(cleaned)
}

func SortSlice[T any](slice []T, less func(i, j int) bool) {
	for i := 1; i < len(slice); i++ {
		for j := i; j > 0 && less(j, j-1); j-- {
			slice[j], slice[j-1] = slice[j-1], slice[j]
		}
	}
}

func FlattenTraceCalls(txHash string, blockNumber uint64, calls []node.TraceCall, depth int) []models.InternalTransaction {
	var result []models.InternalTransaction

	for _, call := range calls {
		itx := models.InternalTransaction{
			TxHash:      txHash,
			From:        call.From,
			To:          call.To,
			Value:       call.Value,
			Input:       call.Input,
			Output:      call.Output,
			Type:        call.Type,
			Gas:         call.Gas,
			GasUsed:     call.GasUsed,
			BlockNumber: blockNumber,
			Depth:       depth,
		}
		result = append(result, itx)

		if len(call.Calls) > 0 {
			nested := FlattenTraceCalls(txHash, blockNumber, call.Calls, depth+1)
			result = append(result, nested...)
		}
	}

	return result
}

func SanitizeString(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}
