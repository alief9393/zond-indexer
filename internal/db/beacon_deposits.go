package db

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
)

// BeaconDeposit represents the structure of a single beacon deposit.
type BeaconDeposit struct {
	Index          string `json:"index"`
	ValidatorIndex string `json:"validator_index"`
	FromAddress    string `json:"from_address"`
	Amount         string `json:"amount"`
	Timestamp      string `json:"timestamp"`
	TxHash         string `json:"tx_hash"`
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
		txHashBytes := utils.MustHexToAddressBytes(d.TxHash)
		ts, _ := strconv.ParseInt(d.Timestamp, 10, 64)
		timestamp := time.Unix(ts, 0)

		const insertDepositSQL = `
			INSERT INTO beacon_deposits (index, validator_index, block_number, from_address, amount, timestamp, tx_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (block_number, index) DO NOTHING;
		`
		batch.Queue(insertDepositSQL, index, validatorIndex, blockNumber, fromBytes, d.Amount, timestamp, txHashBytes)
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
