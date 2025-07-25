package zond

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"zond-indexer/internal/models" // Make sure this path is correct for your indexer
)

// JSONRPCRequest defines the structure for a standard JSON-RPC request.
type JSONRPCRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// JSONRPCResponse defines the structure for a standard JSON-RPC response.
type JSONRPCResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	ID      int             `json:"id"`
}

// JSONRPCError defines the structure of a JSON-RPC error object.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *JSONRPCError) Error() string {
	return fmt.Sprintf("RPC Error: code %d, message: %s", e.Code, e.Message)
}

// NodeClient now uses a standard, robust HTTP client.
type NodeClient struct {
	httpClient *http.Client
	rpcURL     string
}

// NewNodeClient creates a new client instance using the HTTP RPC URL.
func NewNodeClient(rpcURL string) (*NodeClient, error) {
	return &NodeClient{
		httpClient: &http.Client{Timeout: 15 * time.Second},
		rpcURL:     rpcURL,
	}, nil
}

// call is a private helper method to perform the actual JSON-RPC call over HTTP.
func (c *NodeClient) call(ctx context.Context, method string, result interface{}, params ...interface{}) error {
	request := JSONRPCRequest{
		Jsonrpc: "2.0", Method: method, Params: params, ID: 1,
	}
	reqBytes, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to perform HTTP request: %w", err)
	}
	defer resp.Body.Close()

	var rpcResponse JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResponse); err != nil {
		return fmt.Errorf("failed to decode JSON-RPC response body: %w", err)
	}
	if rpcResponse.Error != nil {
		return rpcResponse.Error
	}
	if err := json.Unmarshal(rpcResponse.Result, result); err != nil {
		return fmt.Errorf("failed to unmarshal RPC result into target struct: %w", err)
	}
	return nil
}

// --- This section solves the unmarshaling error definitively ---

// tempPendingTx is a private struct that perfectly matches the node's JSON output.
type tempPendingTx struct {
	Hash     string  `json:"hash"`
	From     string  `json:"from"`
	To       *string `json:"to"`
	Value    string  `json:"value"`
	Gas      string  `json:"gas"`
	GasPrice string  `json:"gasPrice"`
	Nonce    string  `json:"nonce"`
	Input    string  `json:"input"`
	// We only define fields we need from the raw response.
}

// TxPoolContentResponse uses the temporary struct.
type TxPoolContentResponse struct {
	Pending map[string]map[string]*tempPendingTx `json:"pending"`
}

// GetPendingTransactions now uses the two-step decoding process.
func (n *NodeClient) GetPendingTransactions(ctx context.Context) ([]models.Transaction, error) {
	var response TxPoolContentResponse
	// Use the node's correct RPC method name for mempool content
	err := n.call(ctx, "txpool_content", &response)
	if err != nil {
		return nil, err
	}

	var transactions []models.Transaction
	for _, txsByNonce := range response.Pending {
		for _, rawTx := range txsByNonce {
			if rawTx == nil {
				continue
			}

			// Manually convert from the raw struct to your final, clean model.Transaction
			txHashBytes, _ := hex.DecodeString(strings.TrimPrefix(rawTx.Hash, "0x"))
			fromBytes, _ := hex.DecodeString(strings.TrimPrefix(rawTx.From, "Z"))

			var toBytes []byte
			if rawTx.To != nil {
				toBytes, _ = hex.DecodeString(strings.TrimPrefix(*rawTx.To, "Z"))
			}

			inputBytes, _ := hex.DecodeString(strings.TrimPrefix(rawTx.Input, "0x"))

			tx := models.Transaction{
				TxHash:      txHashBytes,
				FromAddress: fromBytes,
				ToAddress:   toBytes,
				Value:       rawTx.Value,
				Gas:         rawTx.Gas,
				GasPrice:    rawTx.GasPrice,
				Nonce:       rawTx.Nonce,
				Input:       inputBytes,
			}
			transactions = append(transactions, tx)
		}
	}
	return transactions, nil
}
