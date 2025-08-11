package utils

import "time"

type Deposit struct {
	Index          string    `json:"index"`
	ValidatorIndex string    `json:"validator_index"`
	Amount         string    `json:"amount"`
	TxHash         string    `json:"tx_hash"`
	FromAddress    string    `json:"from_address"`
	PubKey         string    `json:"pubkey"`
	Signature      string    `json:"signature"`
	Timestamp      time.Time `json:"timestamp"`
}
