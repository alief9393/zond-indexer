package indexer

import (
	"context"
	"fmt"
	"log"

	"zond-indexer/internal/config"
	"zond-indexer/internal/models"

	"github.com/theQRL/go-zond/zondclient"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ValidatorIndexer struct {
	Client               *zondclient.Client
	DB                   *pgxpool.Pool
	Config               config.Config
	EpochLength          uint64
	LastValidatorIndexed uint64
}

func (v *ValidatorIndexer) IndexPeriodically(ctx context.Context, blockNum uint64) error {
	if blockNum == 0 || (blockNum > 0 && blockNum%v.EpochLength == 0 && blockNum > v.LastValidatorIndexed) {
		tx, err := v.DB.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin transaction for validator indexing: %w", err)
		}
		defer tx.Rollback(ctx)

		if err := models.IndexValidators(ctx, v.Client, tx, v.Config); err != nil {
			log.Printf("Failed to index Validators at block %d: %v", blockNum, err)
			return nil // Not critical, continue indexing blocks
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit transaction for validator indexing: %w", err)
		}

		v.LastValidatorIndexed = blockNum
		log.Printf("Indexed validators at block %d", blockNum)
	}
	return nil
}
