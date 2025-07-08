package indexer

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/zondclient"
)

func indexGasPrices(ctx context.Context, _ *zondclient.Client, tx pgx.Tx, block *types.Block, blockNum uint64) error {
	var gasPrices []*big.Int
	for _, transaction := range block.Transactions() {
		gasPrice := transaction.GasPrice()
		if transaction.Type() >= types.DynamicFeeTxType {
			gasPrice = transaction.GasFeeCap()
		}
		gasPrices = append(gasPrices, gasPrice)
	}

	if len(gasPrices) == 0 {
		gasPrices = append(gasPrices, big.NewInt(0))
	}

	utils.SortGasPrices(gasPrices)

	lowPrice := gasPrices[0]
	highPrice := gasPrices[len(gasPrices)-1]
	averagePrice := utils.CalculateAverageGasPrice(gasPrices)

	timestamp := time.Unix(int64(block.Time()), 0)
	_, err := tx.Exec(ctx,
		`INSERT INTO GasPrices (
			timestamp, low_price, average_price, high_price, block_number,
			retrieved_at, retrieved_from, reverted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (timestamp, block_number) DO UPDATE
		SET low_price = EXCLUDED.low_price,
			average_price = EXCLUDED.average_price,
			high_price = EXCLUDED.high_price,
			retrieved_at = EXCLUDED.retrieved_at,
			retrieved_from = EXCLUDED.retrieved_from,
			reverted_at = EXCLUDED.reverted_at`,
		timestamp,
		lowPrice.Int64(),
		averagePrice.Int64(),
		highPrice.Int64(),
		blockNum,
		time.Now(),
		"zond_node",
		nil,
	)
	if err != nil {
		return fmt.Errorf("insert gas prices: %w", err)
	}

	return nil
}
