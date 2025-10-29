package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		log.Println("Publisher mode starting...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Publisher shut down gracefully.")
				return
			default:
				err := indexer.StartPublisher(ctx, cfg)
				if err != nil {
					if ctx.Err() != nil {
						log.Println("Publisher shutting down...")
						return
					}
					log.Printf("Publisher error: %v. Reconnecting in 5 seconds...", err)
					time.Sleep(5 * time.Second)
				} else {
					log.Println("Publisher exited. Restarting in 5 seconds...")
					time.Sleep(5 * time.Second)
				}
			}
		}

	case "consumer":
		log.Println("Consumer mode starting...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Consumer shut down gracefully.")
				return
			default:
				if err := runConsumerOnce(ctx, cfg); err != nil {
					if ctx.Err() != nil {
						log.Println("Consumer shutting down...")
						return
					}
					log.Printf("Consumer error: %v. Reconnecting in 5 seconds...", err)
					time.Sleep(5 * time.Second)
				} else {
					log.Println("Consumer exited. Restarting in 5 seconds...")
					time.Sleep(5 * time.Second)
				}
			}
		}

	case "sync":
		log.Println("Sync mode starting...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Sync shut down gracefully.")
				return
			default:
				if err := runSyncOnce(ctx, cfg); err != nil {
					if ctx.Err() != nil {
						log.Println("Sync shutting down...")
						return
					}
					log.Printf("Sync error: %v. Reconnecting in 5 seconds...", err)
					time.Sleep(5 * time.Second)
				} else {
					log.Println("Sync exited. Restarting in 5 seconds...")
					time.Sleep(5 * time.Second)
				}
			}
		}
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

func runConsumerOnce(ctx context.Context, cfg config.Config) error {
	idx, err := indexer.NewIndexer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create indexer: %w", err)
	}
	defer idx.Close()
	if err := idx.RunConsumer(ctx); err != nil {
		return fmt.Errorf("consumer run failed: %w", err)
	}
	return nil
}

func runSyncOnce(ctx context.Context, cfg config.Config) error {
	idx, err := indexer.NewIndexer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create indexer: %w", err)
	}
	defer idx.Close()

	if err := idx.Run(ctx); err != nil {
		return fmt.Errorf("sync run failed: %w", err)
	}

	return nil
}
