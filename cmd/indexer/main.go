package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"zond-indexer/internal/config"
	"zond-indexer/internal/db"
	"zond-indexer/internal/indexer"

	"github.com/joho/godotenv"
)

func main() {
	// Load the .env file
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Failed to load .env file: %v", err)
	}

	// Load the configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize the indexer with the loaded config
	idx, err := indexer.NewIndexer(cfg)
	if err != nil {
		log.Fatalf("Failed to create indexer: %v", err)
	}
	defer idx.Close()

	// Apply database migrations
	if err := db.Migrate(idx.DB()); err != nil {
		log.Fatalf("Failed to apply migrations: %v", err)
	}

	// Set up context for shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for shutdown
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
