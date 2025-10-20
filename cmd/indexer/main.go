package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"zond-indexer/internal/config"
	"zond-indexer/internal/indexer"

	"github.com/joho/godotenv"
)

func main() {
	mode := flag.String("mode", "consumer", "Set the run mode: 'publisher', 'consumer', or 'sync'")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Failed to load .env file: %v", err)
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping...")
		cancel()
	}()

	log.Printf("Starting Zond Indexer in '%s' mode...", *mode)

	switch *mode {
	case "publisher":
		if err := indexer.StartPublisher(ctx, cfg); err != nil {
			log.Fatalf("Publisher failed: %v", err)
		}
		log.Println("Publisher shut down gracefully.")

	case "consumer":
		idx, err := indexer.NewIndexer(cfg)
		if err != nil {
			log.Fatalf("Failed to create indexer: %v", err)
		}
		defer idx.Close()

		if err := idx.RunConsumer(ctx); err != nil {
			log.Fatalf("Consumer failed: %v", err)
		}
		log.Println("Consumer shut down gracefully.")

	case "sync":
		idx, err := indexer.NewIndexer(cfg)
		if err != nil {
			log.Fatalf("Failed to create indexer: %v", err)
		}
		defer idx.Close()

		if err := idx.Run(ctx); err != nil {
			log.Fatalf("Sync mode failed: %v", err)
		}
		log.Println("Sync mode shut down gracefully.")

	default:
		log.Fatalf("Invalid mode specified: %s. Use 'publisher', 'consumer', or 'sync'.", *mode)
	}
}
