package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"zond-indexer/internal/models"
	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
)

func InsertInternalTransactions(ctx context.Context, tx pgx.Tx, internalTxs []models.InternalTransaction, canonical bool) error {
	if len(internalTxs) == 0 {
		return nil
	}
	now := time.Now()
	log.Printf("Inserting %d internal transactions", len(internalTxs))
	for _, itx := range internalTxs {
		_, err := tx.Exec(ctx, `
				INSERT INTO InternalTransactions (
					tx_hash, from_address, to_address, value, input, output, type,
					gas, gas_used, block_number, depth,
					retrieved_at, retrieved_from, is_canonical, reverted_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7,
						$8, $9, $10, $11,
						$12, $13, $14, $15)`,
			itx.TxHash,
			utils.MustHexToAddressBytes(itx.From),
			utils.MustHexToAddressBytes(itx.To),
			itx.Value,
			itx.Input,
			itx.Output,
			itx.Type,
			itx.Gas,
			itx.GasUsed,
			itx.BlockNumber,
			itx.Depth,
			now,
			"zond_node",
			canonical,
			nil,
		)
		if err != nil {
			log.Printf("Insert failed: %v", err)
			return fmt.Errorf("insert internal transaction: %w", err)
		}
	}
	return nil
}
