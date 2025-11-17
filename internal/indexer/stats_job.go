package indexer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const dailyNetworkStatsQuery = `
WITH daily_token_txs AS (
    SELECT
        tx.timestamp::date AS day,
        COUNT(tt.tx_hash) AS count
    FROM tokentransactions tt
    JOIN transactions tx ON tt.tx_hash = tx.tx_hash
    WHERE tt.token_id IS NULL -- ERC-20 only
      AND tx.timestamp::date = $1::date
    GROUP BY tx.timestamp::date
),
daily_active_addresses AS (
    SELECT
        timestamp::date AS day,
        COUNT(DISTINCT address) AS count
    FROM (
        SELECT timestamp, from_address AS address FROM transactions
        WHERE timestamp::date = $1::date
        UNION
        SELECT timestamp, to_address AS address FROM transactions
        WHERE timestamp::date = $1::date AND to_address IS NOT NULL
    ) AS active_addrs
    GROUP BY timestamp::date
),
daily_active_token_addresses AS (
    SELECT
        tx.timestamp::date AS day,
        COUNT(DISTINCT address) AS count
    FROM (
        SELECT tx.timestamp, tt.from_address AS address FROM tokentransactions tt
        JOIN transactions tx ON tt.tx_hash = tx.tx_hash
        WHERE tx.timestamp::date = $1::date AND tt.token_id IS NULL
        UNION
        SELECT tx.timestamp, tt.to_address AS address FROM tokentransactions tt
        JOIN transactions tx ON tt.tx_hash = tx.tx_hash
        WHERE tx.timestamp::date = $1::date AND tt.token_id IS NULL AND tt.to_address IS NOT NULL
    ) AS active_token_addrs
    GROUP BY tx.timestamp::date
),
daily_fees AS (
    SELECT
        timestamp::date AS day,
        COALESCE(AVG(fee_usd), 0) AS avg_fee_usd,
        COALESCE(AVG(fee_eth), 0) AS avg_fee_qrl,
        COALESCE(SUM(fee_eth), 0) AS total_fee_qrl -- ADDED THIS
    FROM transactions
    WHERE timestamp::date = $1::date
    GROUP BY timestamp::date
)
INSERT INTO daily_network_stats (
    date,
    total_transactions,
    avg_block_time_sec,
    avg_block_size_bytes,
    total_block_count,
    new_addresses,
    total_token_transfers,
    avg_gas_limit,
    total_gas_used,
    active_addresses,
    active_token_addresses,
    avg_transaction_fee_usd,
    avg_transaction_fee_qrl,
    burnt_fees_qrl,
    total_transaction_fee_qrl -- ADDED THIS
    -- avg_difficulty,
    -- hash_rate
)
SELECT
    b.day,
    COALESCE(SUM(b.transaction_count), 0) AS total_transactions,
    CASE 
        WHEN COALESCE(COUNT(b.block_number), 0) > 0 THEN 86400.0 / COALESCE(COUNT(b.block_number), 0)
        ELSE 0 
    END AS avg_block_time_sec,
    COALESCE(AVG(b.size), 0) AS avg_block_size_bytes,
    COALESCE(COUNT(b.block_number), 0) AS total_block_count,
    COALESCE(a.new_addresses, 0) AS new_addresses,
    COALESCE(dtt.count, 0) AS total_token_transfers,
    COALESCE(AVG(b.gas_limit), 0) AS avg_gas_limit,
    COALESCE(SUM(b.gas_used), 0) AS total_gas_used,
    COALESCE(daa.count, 0) AS active_addresses,
    COALESCE(data.count, 0) AS active_token_addresses,
    COALESCE(df.avg_fee_usd, 0) AS avg_transaction_fee_usd,
    COALESCE(df.avg_fee_qrl, 0) AS avg_transaction_fee_qrl,
    COALESCE(SUM(b.burnt_fees_eth), 0) AS burnt_fees_qrl,
    COALESCE(df.total_fee_qrl, 0) AS total_transaction_fee_qrl -- ADDED THIS
    -- COALESCE(AVG(b.difficulty), 0) AS avg_difficulty,
    -- COALESCE(AVG(b.hash_rate), 0) AS hash_rate
FROM (
    SELECT
        timestamp::date AS day,
        transaction_count,
        block_number,
        size,
        gas_limit,
        gas_used,
        burnt_fees_eth
    FROM blocks
    WHERE timestamp::date = $1::date
) b
LEFT JOIN (
    SELECT
        first_seen::date AS day,
        COUNT(*) AS new_addresses
    FROM accounts
    WHERE first_seen::date = $1::date
    GROUP BY day
) a ON b.day = a.day
LEFT JOIN daily_token_txs dtt ON b.day = dtt.day
LEFT JOIN daily_active_addresses daa ON b.day = daa.day
LEFT JOIN daily_active_token_addresses data ON b.day = data.day
LEFT JOIN daily_fees df ON b.day = df.day
GROUP BY b.day, a.new_addresses, dtt.count, daa.count, data.count, df.avg_fee_usd, df.avg_fee_qrl, df.total_fee_qrl -- ADDED total_fee_qrl
ON CONFLICT (date) DO UPDATE SET
    total_transactions = EXCLUDED.total_transactions,
    avg_block_time_sec = EXCLUDED.avg_block_time_sec,
    avg_block_size_bytes = EXCLUDED.avg_block_size_bytes,
    total_block_count = EXCLUDED.total_block_count,
    new_addresses = EXCLUDED.new_addresses,
    total_token_transfers = EXCLUDED.total_token_transfers,
    avg_gas_limit = EXCLUDED.avg_gas_limit,
    total_gas_used = EXCLUDED.total_gas_used,
    active_addresses = EXCLUDED.active_addresses,
    active_token_addresses = EXCLUDED.active_token_addresses,
    avg_transaction_fee_usd = EXCLUDED.avg_transaction_fee_usd,
    avg_transaction_fee_qrl = EXCLUDED.avg_transaction_fee_qrl,
    burnt_fees_qrl = EXCLUDED.burnt_fees_qrl,
    total_transaction_fee_qrl = EXCLUDED.total_transaction_fee_qrl; -- ADDED THIS
    -- avg_difficulty = EXCLUDED.avg_difficulty,
    -- hash_rate = EXCLUDED.hash_rate;
`

// runStatsForDay executes the aggregation query for the given day.
func runStatsForDay(ctx context.Context, db *pgxpool.Pool, day time.Time) error {
	dateStr := day.Format("2006-01-02")
	log.Printf("[StatsJob] Running stats query for %s...", dateStr)

	_, err := db.Exec(ctx, dailyNetworkStatsQuery, dateStr)
	if err != nil {
		return fmt.Errorf("failed to execute daily stats query for %s: %w", dateStr, err)
	}

	log.Printf("[StatsJob] Successfully saved stats for %s.", dateStr)
	return nil
}

// backfillDailyStats checks for any missing days between the last run and yesterday.
func backfillDailyStats(ctx context.Context, db *pgxpool.Pool) {
	var lastIndexedDate time.Time

	// Find the last date we have stats for.
	err := db.QueryRow(ctx, "SELECT COALESCE(MAX(date), '1970-01-01'::date) FROM daily_network_stats").Scan(&lastIndexedDate)
	if err != nil {
		log.Printf("[StatsJob] Error getting last indexed date: %v", err)
		return
	}

	// We only backfill up to yesterday. Today's stats run at midnight.
	yesterday := time.Now().Truncate(24 * time.Hour).Add(-1 * time.Second)

	// Loop from the day after the last indexed day up to yesterday.
	for day := lastIndexedDate.AddDate(0, 0, 1); day.Before(yesterday); day = day.AddDate(0, 0, 1) {
		select {
		case <-ctx.Done():
			log.Printf("[StatsJob] Backfill shutting down.")
			return
		default:
			if err := runStatsForDay(ctx, db, day); err != nil {
				log.Printf("[StatsJob] Error backfilling stats for %s: %v", day.Format("2006-01-02"), err)
				// Don't stop, just try the next day.
			}
		}
	}
	log.Println("[StatsJob] Backfill complete.")
}

// RunDailyStatsScheduler runs a job to populate the daily_network_stats table.
// It backfills any missing days on startup and then runs once daily at midnight.
func RunDailyStatsScheduler(ctx context.Context, db *pgxpool.Pool) {
	log.Println("[StatsJob] Starting daily stats scheduler...")

	// 1. Run backfill on startup to catch any missed days.
	go backfillDailyStats(ctx, db)

	// 2. Loop and run the job at midnight every day.
	for {
		// Calculate duration until next midnight (in local time)
		now := time.Now()
		midnight := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
		durationUntilMidnight := time.Until(midnight)

		log.Printf("[StatsJob] Next run scheduled for %v (in %v)", midnight, durationUntilMidnight)

		select {
		case <-ctx.Done():
			// Context was cancelled, shut down the scheduler.
			log.Println("[StatsJob] Scheduler shutting down.")
			return

		case <-time.After(durationUntilMidnight):
			// It's midnight. Run the stats for "yesterday".
			yesterday := time.Now().Add(-1 * time.Minute) // 1 minute ago to be safe
			if err := runStatsForDay(ctx, db, yesterday); err != nil {
				log.Printf("[StatsJob] Failed to run daily stats job: %v", err)
			}
		}
	}
}
