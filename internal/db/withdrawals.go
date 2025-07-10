package db

import (
	"context"
	"fmt"
	"strconv"

	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
)

// Withdrawal represents the structure of a single withdrawal from the beacon block response.
type Withdrawal struct {
	Index          string `json:"index"`
	ValidatorIndex string `json:"validator_index"`
	Address        string `json:"address"`
	Amount         string `json:"amount"` // Amount is in Gwei (10^9 Wei)
}

// InsertWithdrawals inserts a slice of withdrawals for a specific block into the database.
func InsertWithdrawals(ctx context.Context, tx pgx.Tx, blockNumber uint64, withdrawals []Withdrawal) error {
	if len(withdrawals) == 0 {
		return nil
	}

	// Prepare a batch insert for efficiency.
	batch := &pgx.Batch{}
	for _, w := range withdrawals {
		index, err := strconv.ParseInt(w.Index, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid withdrawal index '%s': %w", w.Index, err)
		}

		validatorIndex, err := strconv.Atoi(w.ValidatorIndex)
		if err != nil {
			return fmt.Errorf("invalid withdrawal validator_index '%s': %w", w.ValidatorIndex, err)
		}

		// Convert address from "0x..." hex string to bytea
		addrBytes := utils.MustHexToAddressBytes(w.Address)

		// The withdrawal amount from the beacon API is in Gwei.
		// To store it consistently with other values, we convert it to Wei (Planck).
		// 1 Gwei = 1,000,000,000 Wei.
		amountInGwei := w.Amount
		amountInWei := amountInGwei + "000000000" // Append 9 zeros

		const insertWithdrawalSQL = `
			INSERT INTO Withdrawals (index, validator_index, block_number, address, amount)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (block_number, index) DO NOTHING;
		`
		batch.Queue(insertWithdrawalSQL, index, validatorIndex, blockNumber, addrBytes, amountInWei)
	}

	// Execute the batch
	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	// Check for errors in the batch execution
	for i := 0; i < len(withdrawals); i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute withdrawal insert in batch: %w", err)
		}
	}

	return nil
}
