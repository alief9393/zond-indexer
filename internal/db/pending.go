package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"zond-indexer/internal/models"

	"github.com/jackc/pgx/v5"
)

// UpsertPendingTransactions now uses a simple loop instead of a batch for easier debugging.
func UpsertPendingTransactions(ctx context.Context, tx pgx.Tx, txs []models.PendingTransaction) error {
	if len(txs) == 0 {
		return nil
	}

	query := `
		INSERT INTO pending_transactions (tx_hash, from_address, to_address, nonce, gas, gas_price, value, method, last_seen)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (tx_hash) DO UPDATE SET last_seen = NOW();
	`

	// --- THE DEFINITIVE FIX: Use a simple loop to expose any hidden errors ---
	for _, ptx := range txs {
		txHashBytes, _ := hex.DecodeString(strings.TrimPrefix(ptx.Hash, "0x"))
		fromBytes, _ := hex.DecodeString(strings.TrimPrefix(ptx.From, "Z"))
		var toBytes []byte
		if ptx.To != nil {
			toBytes, _ = hex.DecodeString(strings.TrimPrefix(*ptx.To, "Z"))
		}
		nonce, _ := new(big.Int).SetString(strings.TrimPrefix(ptx.Nonce, "0x"), 16)
		gas, _ := new(big.Int).SetString(strings.TrimPrefix(ptx.Gas, "0x"), 16)
		var method *string
		if len(ptx.Input) >= 10 {
			methodHex := ptx.Input[:10]
			method = &methodHex
		}

		// Execute each insert individually. If any single one fails, we will get an error immediately.
		_, err := tx.Exec(ctx, query,
			txHashBytes,
			fromBytes,
			toBytes,
			nonce.Int64(),
			gas.Int64(),
			ptx.GasPrice,
			ptx.Value,
			method,
		)
		if err != nil {
			// This will now catch the error that was being hidden by the batch.
			return fmt.Errorf("failed on upsert for tx %s: %w", ptx.Hash, err)
		}
	}
	// --- END OF FIX ---

	return nil // The commit is handled by the calling function
}

func DeletePendingTransactionByHash(ctx context.Context, tx pgx.Tx, txHash []byte) error {
	_, err := tx.Exec(ctx, `DELETE FROM pending_transactions WHERE tx_hash = $1`, txHash)
	return err
}
