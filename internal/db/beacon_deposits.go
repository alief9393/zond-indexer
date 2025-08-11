package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
)

// BeaconDeposit represents the structure of a single beacon deposit.
type BeaconDeposit struct {
	Index          string    `json:"index"`
	ValidatorIndex string    `json:"validator_index"`
	Amount         string    `json:"amount"`
	TxHash         string    `json:"tx_hash"`
	FromAddress    string    `json:"from_address"`
	PubKey         string    `json:"pubkey"`
	Signature      string    `json:"signature"`
	Timestamp      time.Time `json:"timestamp"`
}

// InsertBeaconDeposits inserts a slice of beacon deposits into the database.
func InsertBeaconDeposits(ctx context.Context, tx pgx.Tx, blockNumber uint64, deposits []BeaconDeposit) error {
	if len(deposits) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, d := range deposits {
		index, _ := strconv.ParseInt(d.Index, 10, 64)
		validatorIndex, _ := strconv.Atoi(d.ValidatorIndex)
		fromBytes := utils.MustHexToAddressBytes(d.FromAddress)
		txHashBytes, _ := hex.DecodeString(strings.TrimPrefix(d.TxHash, "0x"))
		pubKeyBytes, _ := hex.DecodeString(strings.TrimPrefix(d.PubKey, "0x"))
		sigBytes, _ := hex.DecodeString(strings.TrimPrefix(d.Signature, "0x"))

		const insertDepositSQL = `
			INSERT INTO beacon_deposits (index, validator_index, block_number, from_address, amount, timestamp, tx_hash, pubkey, signature)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (block_number, index) DO NOTHING;
		`
		batch.Queue(insertDepositSQL, index, validatorIndex, blockNumber, fromBytes, d.Amount, d.Timestamp, txHashBytes, pubKeyBytes, sigBytes)
	}

	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < len(deposits); i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute beacon deposit insert in batch: %w", err)
		}
	}

	return nil
}
