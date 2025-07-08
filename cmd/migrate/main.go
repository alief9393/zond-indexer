// cmd/migrate/main.go
package main

import (
	"context"
	"flag"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"zond-indexer/internal/db"
	logger "zond-indexer/internal/log"
)

func main() {
	logger.Init()
	logger.Logger.Info("🚀 Starting migration...")

	drop := flag.Bool("drop", false, "Drop all tables before migrating")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		logger.Logger.Warn("⚠️ No .env file found, proceeding without it")
	}

	connStr := os.Getenv("POSTGRES_CONN")
	if connStr == "" {
		logger.Logger.Fatal("❌ Environment variable POSTGRES_CONN is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		logger.Logger.WithError(err).Fatal("❌ Unable to create connection pool")
	}
	defer pool.Close()

	if err := db.Migrate(pool, *drop); err != nil {
		logger.Logger.WithError(err).Fatal("❌ Migration failed")
	}

	logger.Logger.Info("✅ Migration completed successfully")
}
