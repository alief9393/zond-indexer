package node

import (
	"context"
	"fmt"
	"math/big"
	"zond-indexer/internal/models"

	"github.com/rs/zerolog/log"
	"github.com/theQRL/go-zond/rpc"
)

// StructLog represents one EVM execution step from structLogs
type StructLog struct {
	PC      uint64   `json:"pc"`
	Op      string   `json:"op"`
	Gas     uint64   `json:"gas"`
	GasCost uint64   `json:"gasCost"`
	Depth   int      `json:"depth"`
	Stack   []string `json:"stack"`
}

// TraceCall represents a single internal call if available
type TraceCall struct {
	Type    string      `json:"type"`
	From    string      `json:"from"`
	To      string      `json:"to"`
	Value   string      `json:"value"`
	Gas     string      `json:"gas"`
	GasUsed string      `json:"gasUsed"`
	Input   string      `json:"input"`
	Output  string      `json:"output"`
	Calls   []TraceCall `json:"calls,omitempty"`
}

// TraceTransactionResult is the full result from debug_traceTransaction
type TraceTransactionResult struct {
	Gas         uint64           `json:"gas"`
	Failed      bool             `json:"failed"`
	ReturnValue string           `json:"returnValue"`
	StructLogs  []TraceStructLog `json:"structLogs,omitempty"`
	Calls       []TraceCall      `json:"calls,omitempty"`
}

type TraceStructLog struct {
	Pc      uint64   `json:"pc"`
	Op      string   `json:"op"`
	Gas     uint64   `json:"gas"`
	GasCost uint64   `json:"gasCost"`
	Depth   int      `json:"depth"`
	Stack   []string `json:"stack"`
	// You can extend this if needed (e.g. memory, storage)
}

func parseBigIntHex(hexStr string) *big.Int {
	val := new(big.Int)
	val.SetString(hexStr[2:], 16)
	return val
}

func parseAddress(hexStr string) string {
	if len(hexStr) >= 42 {
		return "0x" + hexStr[len(hexStr)-40:]
	}
	return ""
}

func ConvertStructLogsToInternalTxs(txHash string, blockNumber uint64, logs []TraceStructLog) []models.InternalTransaction {
	var result []models.InternalTransaction

	var current models.InternalTransaction
	var inCall bool

	for _, log := range logs {
		switch log.Op {
		case "CALL", "DELEGATECALL", "STATICCALL", "CALLCODE":
			// This is a call start — we assume stack has enough info
			if len(log.Stack) >= 7 {
				inCall = true
				current = models.InternalTransaction{
					TxHash:      txHash,
					BlockNumber: blockNumber,
					Type:        log.Op,
					From:        parseAddress(log.Stack[len(log.Stack)-2]),
					To:          parseAddress(log.Stack[len(log.Stack)-3]),
					Value:       parseBigIntHex(log.Stack[len(log.Stack)-4]).String(),
					Gas:         parseBigIntHex(log.Stack[len(log.Stack)-1]).String(),
					Depth:       log.Depth,
				}
			}

		case "STOP", "RETURN", "REVERT", "INVALID", "SELFDESTRUCT":
			if inCall {
				current.GasUsed = current.Gas // fallback
				result = append(result, current)
				inCall = false
			}
		}
	}

	return result
}

// TraceTransaction calls debug_traceTransaction on the node
func TraceTransaction(ctx context.Context, rpcClient *rpc.Client, txHash string) (*TraceTransactionResult, error) {
	var result TraceTransactionResult
	err := rpcClient.CallContext(ctx, &result, "debug_traceTransaction", txHash, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("trace call failed: %w", err)
	}

	log.Debug().
		Str("tx", txHash).
		Int("structLogSteps", len(result.StructLogs)).
		Msg("Fetched internal trace")

	return &result, nil
}
