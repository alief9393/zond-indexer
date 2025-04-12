package node

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/zondclient"
)

// IndexZondNodes fetches node information
func IndexZondNodes(ctx context.Context, client *zondclient.Client, tx pgx.Tx) error {
	// Skip node version fetching since ClientVersion is not available in go-zond
	nodeInfo := "unknown"

	// Fetch peer count
	peerCount, err := client.PeerCount(ctx)
	if err != nil {
		return fmt.Errorf("fetch peer count: %w", err)
	}

	// Placeholder for latency and location
	latency := 50.0 // Example latency in milliseconds
	location := "Unknown"

	nodeID := "node-" + nodeInfo // Simplified node ID
	_, err = tx.Exec(ctx,
		`INSERT INTO ZondNodes (
            node_id, version, location, last_seen, latency, peers,
            retrieved_at, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (node_id) DO UPDATE
        SET version = $2, location = $3, last_seen = $4, latency = $5, peers = $6`,
		nodeID,
		nodeInfo,
		location,
		time.Now(),
		latency,
		int(peerCount),
		time.Now(),
		"zond_node")
	if err != nil {
		return fmt.Errorf("insert ZondNodes: %w", err)
	}

	return nil
}
