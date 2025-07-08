package indexer

import (
	"context"
	"fmt"
	"log"

	"zond-indexer/internal/config"
	"zond-indexer/internal/utils"

	"github.com/theQRL/go-zond/core/types"
)

func (i *Indexer) handleReorg(ctx context.Context, newHead *types.Block) error {
	newChain := make(map[int64][]byte)
	currentBlock := newHead
	for currentBlock != nil {
		newChain[currentBlock.Number().Int64()] = currentBlock.Hash().Bytes()
		parent, err := i.client.BlockByHash(ctx, currentBlock.ParentHash())
		if err != nil {
			return fmt.Errorf("fetch parent block: %w", err)
		}
		currentBlock = parent
		if len(newChain) > 10 {
			break
		}
	}

	// Fetch the old chain from the database
	oldChain := make(map[int64][]byte)
	rows, err := i.db.Query(ctx, `
		SELECT block_number, block_hash
		FROM Blocks
		WHERE block_number >= $1 AND block_number <= $2 AND canonical = TRUE
	`, newHead.Number().Int64()-int64(len(newChain)), newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("fetch old chain: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var blockNumber int64
		var blockHash []byte
		if err := rows.Scan(&blockNumber, &blockHash); err != nil {
			return fmt.Errorf("scan old chain block: %w", err)
		}
		oldChain[blockNumber] = blockHash
	}

	// Find the fork point
	forkPoint := newHead.Number().Int64()
	for blockNumber := newHead.Number().Int64(); blockNumber >= newHead.Number().Int64()-int64(len(newChain)); blockNumber-- {
		newHash, inNew := newChain[blockNumber]
		oldHash, inOld := oldChain[blockNumber]
		if inNew && inOld && utils.BytesEqual(newHash, oldHash) {
			forkPoint = blockNumber
			break
		}
	}

	// Start transaction
	tx, err := i.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction for reorg: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update canonical flags
	updates := []struct {
		table string
		field string
	}{
		{"Blocks", "canonical"},
		{"Transactions", "is_canonical"},
		{"TokenTransactions", "is_canonical"},
		{"NFTs", "is_canonical"},
	}

	for _, upd := range updates {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s
			SET %s = FALSE
			WHERE block_number > $1 AND block_number <= $2 AND %s = TRUE
		`, upd.table, upd.field, upd.field), forkPoint, newHead.Number().Int64())
		if err != nil {
			return fmt.Errorf("mark %s as non-canonical: %w", upd.table, err)
		}
	}

	// Reindex blocks
	for blockNumber := forkPoint + 1; blockNumber <= newHead.Number().Int64(); blockNumber++ {
		if _, ok := newChain[blockNumber]; !ok {
			continue
		}
		cfg, _ := config.LoadConfig()
		if err := indexBlock(ctx, cfg, i.client, i.rpcClient, i.db, uint64(blockNumber), i.chainID, true); err != nil {
			return fmt.Errorf("reindex block %d: %w", blockNumber, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reorg transaction: %w", err)
	}

	log.Printf("Reorg handled up to block %d (fork at %d)", newHead.Number().Int64(), forkPoint)
	return nil
}
