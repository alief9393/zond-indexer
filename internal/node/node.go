package node

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/zondclient"
)

// IndexZondNodes fetches node information and indexes it
func IndexZondNodes(ctx context.Context, client *zondclient.Client, tx pgx.Tx) error {
	// Use static version since go-zond doesn't expose version info
	nodeVersion := "unknown"

	// Fetch peer count
	peerCount, err := client.PeerCount(ctx)
	if err != nil {
		return fmt.Errorf("fetch peer count: %w", err)
	}

	// Placeholder values
	latency := 50.0
	location := "Unknown"
	nodeID := "node-" + nodeVersion
	retrievedAt := time.Now()

	_, err = tx.Exec(ctx,
		`INSERT INTO ZondNodes (
			node_id, version, location, last_seen, latency, peers,
			retrieved_at, retrieved_from, reverted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (node_id) DO UPDATE
		SET version = EXCLUDED.version,
			location = EXCLUDED.location,
			last_seen = EXCLUDED.last_seen,
			latency = EXCLUDED.latency,
			peers = EXCLUDED.peers,
			retrieved_at = EXCLUDED.retrieved_at,
			retrieved_from = EXCLUDED.retrieved_from,
			reverted_at = EXCLUDED.reverted_at`,
		nodeID,
		nodeVersion,
		location,
		retrievedAt,
		latency,
		int(peerCount),
		retrievedAt,
		"zond_node",
		nil,
	)
	if err != nil {
		return fmt.Errorf("insert ZondNodes: %w", err)
	}

	return nil
}
