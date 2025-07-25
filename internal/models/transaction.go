package models

import (
	"time"
)

// Transaction represents the structure of a transaction in your `transactions` table.
// The `db` tags should perfectly match your PostgreSQL column names.
type Transaction struct {
	TxHash               []byte    `db:"tx_hash"`
	BlockNumber          int64     `db:"block_number"`
	FromAddress          []byte    `db:"from_address"`
	ToAddress            []byte    `db:"to_address"`
	Value                string    `db:"value"`
	Gas                  string    `db:"gas"`       // Received as a hex string, stored as a string
	GasPrice             string    `db:"gas_price"` // Received as a hex string, stored as a string
	Nonce                string    `db:"nonce"`     // Received as a hex string, stored as a string
	Input                []byte    `db:"input"`
	Type                 int       `db:"type"`
	ChainID              int64     `db:"chain_id"`
	AccessList           []byte    `db:"access_list"`
	MaxFeePerGas         *string   `db:"max_fee_per_gas"`
	MaxPriorityFeePerGas *string   `db:"max_priority_fee_per_gas"`
	TransactionIndex     int       `db:"transaction_index"`
	CumulativeGasUsed    int64     `db:"cumulative_gas_used"`
	IsSuccessful         bool      `db:"is_successful"`
	RetrievedFrom        string    `db:"retrieved_from"`
	IsCanonical          bool      `db:"is_canonical"`
	Timestamp            time.Time `db:"timestamp"`
	Method               string    `db:"method"`
}

// PendingTransaction represents the structure of a pending transaction in your `pending_transactions` table.
// It is a simplified version of a full transaction.
type PendingTransaction struct {
	Hash     string    `json:"hash"`
	From     string    `json:"from"`
	To       *string   `json:"to"`
	Value    string    `json:"value"`
	Gas      string    `json:"gas"`
	GasPrice string    `json:"gasPrice"`
	Nonce    string    `json:"nonce"`
	Input    string    `json:"input"`
	Method   *string   `db:"method"`
	LastSeen time.Time `db:"last_seen"`
}
