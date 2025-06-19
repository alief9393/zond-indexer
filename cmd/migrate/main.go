// cmd/migrate/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"zond-indexer/internal/db"
)

func main() {
	drop := flag.Bool("drop", false, "Drop all tables before migrating")
	flag.Parse()
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	connStr := os.Getenv("POSTGRES_CONN")
	if connStr == "" {
		log.Fatal("POSTGRES_CONN is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(pool, *drop); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Println("Migration completed successfully")
}
