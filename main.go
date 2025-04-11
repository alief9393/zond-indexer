package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

// Indexer holds the state for indexing Zond blocks
type Indexer struct {
	config     Config
	client     *zondclient.Client
	rpcClient  *rpc.Client
	db         *pgxpool.Pool // Updated to use pgxpool.Pool
	chainID    *big.Int
	rateLimit  time.Duration
	latest     uint64
	historical uint64
}

// NewIndexer creates a new Indexer instance
func NewIndexer(config Config) (*Indexer, error) {
	// Connect to the Zond node
	rpcClient, err := rpc.Dial(config.RPCEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC endpoint: %w", err)
	}
	client := zondclient.NewClient(rpcClient)

	// Fetch the chain ID
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain ID: %w", err)
	}

	// Connect to the database using pgxpool
	dbConfig, err := pgxpool.ParseConfig(config.PostgresConn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Postgres connection string: %w", err)
	}
	db, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the database connection
	err = db.Ping(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Fetch the latest block number
	latest, err := client.BlockNumber(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to fetch latest block number: %w", err)
	}

	return &Indexer{
		config:     config,
		client:     client,
		rpcClient:  rpcClient,
		db:         db,
		chainID:    chainID,
		rateLimit:  config.RateLimit,
		latest:     latest,
		historical: latest,
	}, nil
}

// Run starts the indexing process
func (i *Indexer) Run(ctx context.Context) error {
	// Drop the database if requested
	dropDB := false
	flag.BoolVar(&dropDB, "drop-db", false, "Drop and recreate the database")
	flag.Parse()

	if dropDB {
		// Connect to the default database (e.g., "postgres") to drop and create the target database
		defaultConnStr := "host=" + i.config.PostgresHost + " port=" + i.config.PostgresPort + " user=" + i.config.PostgresUser + " dbname=postgres password=" + i.config.PostgresPassword + " sslmode=disable"
		defaultDBConfig, err := pgxpool.ParseConfig(defaultConnStr)
		if err != nil {
			return fmt.Errorf("failed to parse default Postgres connection string: %w", err)
		}
		defaultDB, err := pgxpool.NewWithConfig(context.Background(), defaultDBConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to default database: %w", err)
		}
		defer defaultDB.Close()

		_, err = defaultDB.Exec(context.Background(), "DROP DATABASE IF EXISTS zond_indexer_db")
		if err != nil {
			return fmt.Errorf("failed to drop database: %w", err)
		}
		log.Println("Dropped database zond_indexer_db")

		_, err = defaultDB.Exec(context.Background(), "CREATE DATABASE zond_indexer_db")
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		log.Println("Created database zond_indexer_db")

		// Reconnect to the new database
		i.db.Close()
		newConnStr := "host=" + i.config.PostgresHost + " port=" + i.config.PostgresPort + " user=" + i.config.PostgresUser + " dbname=zond_indexer_db password=" + i.config.PostgresPassword + " sslmode=disable"
		dbConfig, err := pgxpool.ParseConfig(newConnStr)
		if err != nil {
			return fmt.Errorf("failed to parse new Postgres connection string: %w", err)
		}
		i.db, err = pgxpool.NewWithConfig(context.Background(), dbConfig)
		if err != nil {
			return fmt.Errorf("failed to reconnect to database: %w", err)
		}
	}

	// Apply database migrations
	if err := Migrate(i.db); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	// Check if the node is fully synced
	syncing, err := i.client.SyncProgress(ctx)
	if err != nil {
		return fmt.Errorf("failed to check sync progress: %w", err)
	}
	if syncing != nil {
		return fmt.Errorf("node is not fully synced: current block %d, highest block %d", syncing.CurrentBlock, syncing.HighestBlock)
	}
	log.Println("Node is fully synced")

	// Start indexing historical blocks
	log.Printf("Indexing historical blocks up to block number: %d", i.historical)
	for blockNum := uint64(0); blockNum <= i.historical; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Printf("Processing block %d", blockNum)
			if err := indexBlock(ctx, i.client, i.rpcClient, i.db, blockNum, i.chainID); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}
			time.Sleep(i.rateLimit)
		}
	}

	// Start listening for new blocks
	headers := make(chan *types.Header)
	sub, err := i.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new headers: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case header := <-headers:
			blockNum := header.Number.Uint64()
			log.Printf("New block received: %d", blockNum)
			if err := indexBlock(ctx, i.client, i.rpcClient, i.db, blockNum, i.chainID); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}
			i.latest = blockNum
		}
	}
}

func main() {
	// Initialize the indexer with the default config
	idx, err := NewIndexer(defaultConfig)
	if err != nil {
		log.Fatalf("Failed to create indexer: %v", err)
	}
	defer idx.db.Close()

	// Set up context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping indexer...")
		cancel()
	}()

	// Run the indexer
	if err := idx.Run(ctx); err != nil {
		log.Fatalf("Indexer failed: %v", err)
	}
}
