package indexer

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
	"zond-indexer/internal/db"
	"zond-indexer/internal/models"
)

// The URL of our Node.js microservice translator
const watcherServiceURL = "http://localhost:3001/pending"

// StartPendingTxWatcher is the standalone background task.
func (i *Indexer) StartPendingTxWatcher(ctx context.Context) {
	log.Println("✅ [Watcher] Starting Pending Transaction Watcher...")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: 4 * time.Second}

	for {
		select {
		case <-ctx.Done():
			log.Println("[Watcher] Shutting down.")
			return
		case <-ticker.C:
			log.Println("[Watcher] Fetching pending transactions from Node.js service...")

			resp, err := httpClient.Get(watcherServiceURL)
			if err != nil {
				log.Printf("❌ [Watcher] Failed to connect to watcher service: %v", err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("❌ [Watcher] Watcher service returned non-200 status: %s", resp.Status)
				resp.Body.Close()
				continue
			}

			var pendingTxs []models.PendingTransaction // Assuming this is the model that matches the JSON
			if err := json.NewDecoder(resp.Body).Decode(&pendingTxs); err != nil {
				log.Printf("❌ [Watcher] Failed to unmarshal JSON from watcher service: %v", err)
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			// --- THE DEFINITIVE, CORRECTED LOGIC ---

			// 1. Begin a single database transaction from the main pool.
			tx, err := i.db.Begin(ctx)
			if err != nil {
				log.Printf("❌ [Watcher] Failed to begin database transaction: %v", err)
				continue
			}

			if len(pendingTxs) > 0 {
				log.Printf("✅ [Watcher] Found %d pending transactions. Saving to DB...", len(pendingTxs))
				// Also pass the transaction object `tx` here.
				if err := db.UpsertPendingTransactions(ctx, tx, pendingTxs); err != nil {
					log.Printf("❌ [Watcher] Failed to upsert pending transactions: %v", err)
					tx.Rollback(ctx)
					continue
				}
			}

			// 4. If all database operations were successful, commit the transaction.
			if err := tx.Commit(ctx); err != nil {
				log.Printf("❌ [Watcher] Failed to commit database transaction: %v", err)
			}
			// --- END OF DEFINITIVE LOGIC ---
		}
	}
}
