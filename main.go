package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/zondclient"

	_ "github.com/lib/pq"
)

type Indexer struct {
	db     *sql.DB
	client *zondclient.Client
	config Config
}

func NewIndexer(cfg Config) (*Indexer, error) {
	// First, connect to the 'postgres' database to check/create zond_indexer_db
	postgresConn := "user=" + cfg.PostgresUser + " password=" + cfg.PostgresPassword + " dbname=postgres sslmode=disable host=" + cfg.PostgresHost + " port=" + cfg.PostgresPort
	postgresDB, err := sql.Open("postgres", postgresConn)
	if err != nil {
		return nil, err
	}
	defer postgresDB.Close()

	// Check if zond_indexer_db exists
	var dbExists bool
	err = postgresDB.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'zond_indexer_db')").Scan(&dbExists)
	if err != nil {
		return nil, err
	}

	// Create zond_indexer_db if it doesn't exist
	if !dbExists {
		_, err = postgresDB.Exec("CREATE DATABASE zond_indexer_db")
		if err != nil {
			return nil, err
		}
		log.Println("Created database zond_indexer_db")
	}

	// Now connect to zond_indexer_db
	db, err := sql.Open("postgres", cfg.PostgresConn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	// Use WebSocket endpoint for real-time subscriptions
	client, err := zondclient.Dial(cfg.RPCEndpoint)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Indexer{db: db, client: client, config: cfg}, nil
}

func (idx *Indexer) Run(ctx context.Context) error {
	// Run database migrations
	if err := Migrate(idx.db); err != nil {
		return err
	}

	// Get chain ID
	chainID, err := idx.client.ChainID(ctx)
	if err != nil {
		return err
	}

	// Check sync progress
	syncProgress, err := idx.client.SyncProgress(ctx)
	if err != nil {
		return err
	}
	if syncProgress != nil {
		log.Printf("Node syncing: Start %d, Current %d, Highest %d",
			syncProgress.StartingBlock, syncProgress.CurrentBlock, syncProgress.HighestBlock)
	} else {
		log.Println("Node is fully synced")
	}

	// Index historical blocks (from 0 to latest at startup)
	latestBlock, err := idx.client.BlockNumber(ctx)
	if err != nil {
		return err
	}
	log.Printf("Indexing historical blocks up to block number: %d", latestBlock)

	var wg sync.WaitGroup
	errChan := make(chan error, latestBlock+1)

	for i := uint64(0); i <= latestBlock; i++ {
		wg.Add(1)
		go func(blockNum uint64) {
			defer wg.Done()
			if err := indexBlock(ctx, idx.client, idx.db, blockNum, chainID); err != nil {
				errChan <- err
			}
			time.Sleep(idx.config.RateLimit)
		}(i)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			log.Printf("Indexing error: %v", err)
		}
	}

	log.Println("Historical indexing complete! Starting real-time subscription...")

	// Subscribe to new block headers for real-time updates
	for {
		// Check if context is canceled before attempting subscription
		select {
		case <-ctx.Done():
			log.Println("Shutting down real-time subscription...")
			return nil
		default:
			// Proceed with subscription
		}

		headers := make(chan *types.Header)
		sub, err := idx.client.SubscribeNewHead(ctx, headers)
		if err != nil {
			log.Printf("Failed to subscribe to new headers: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Process headers until subscription fails or context is canceled
		for {
			select {
			case <-ctx.Done():
				log.Println("Shutting down real-time subscription...")
				sub.Unsubscribe()
				return nil
			case err := <-sub.Err():
				log.Printf("Subscription error: %v. Attempting to reconnect...", err)
				sub.Unsubscribe()
				break // Break the inner loop to retry subscription
			case header := <-headers:
				blockNum := header.Number.Uint64()
				log.Printf("New block received: %d", blockNum)
				if err := indexBlock(ctx, idx.client, idx.db, blockNum, chainID); err != nil {
					log.Printf("Error indexing new block %d: %v", blockNum, err)
				}
			}
		}
	}
}

func main() {
	cfg := defaultConfig
	indexer, err := NewIndexer(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize indexer: %v", err)
	}
	defer indexer.db.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := indexer.Run(ctx); err != nil {
		log.Fatalf("Indexer failed: %v", err)
	}
}
