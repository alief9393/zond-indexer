package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Migrate(db *pgxpool.Pool, dropDatabase bool) error {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if dropDatabase {
		fmt.Println("Dropping all existing tables in the database...")
		_, err := tx.Exec(ctx, `
			DO $$
			DECLARE
				r RECORD;
			BEGIN
				FOR r IN (
					SELECT tablename FROM pg_tables WHERE schemaname = 'public'
				) LOOP
					EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
				END LOOP;
			END $$;
		`)
		if err != nil {
			return fmt.Errorf("failed to drop existing tables: %w", err)
		}
		fmt.Println("All tables dropped.")
	}

	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if the type_address domain exists
	var domainExists bool
	err = tx.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typname = 'type_address' AND n.nspname = 'public'
        );
    `).Scan(&domainExists)
	if err != nil {
		return fmt.Errorf("failed to check if type_address domain exists: %w", err)
	}

	// Create the type_address domain if it doesn't exist
	if !domainExists {
		_, err = tx.Exec(ctx, `
            CREATE DOMAIN type_address AS BYTEA
            CHECK (octet_length(VALUE) = 20);
        `)
		if err != nil {
			return fmt.Errorf("failed to create type_address domain: %w", err)
		}
	}

	// Create Blocks table with address fields as type_address
	_, err = tx.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS Blocks (
            block_number BIGINT PRIMARY KEY,
            block_hash BYTEA NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            miner_address type_address NOT NULL,
            canonical BOOL DEFAULT TRUE,
            parent_hash BYTEA NOT NULL,
            gas_used BIGINT NOT NULL,
            gas_limit BIGINT NOT NULL,
            size INTEGER NOT NULL,
            transaction_count INTEGER NOT NULL,
            extra_data BYTEA NOT NULL,
            base_fee_per_gas BIGINT,
            transactions_root BYTEA NOT NULL,
            state_root BYTEA NOT NULL,
            receipts_root BYTEA NOT NULL,
            logs_bloom BYTEA NOT NULL,
            chain_id BIGINT NOT NULL,
            retrieved_from VARCHAR NOT NULL,
            slot BIGINT,
            reward_eth DOUBLE PRECISION,
            burnt_fees_eth DOUBLE PRECISION,
            reorg_depth INTEGER,
            epoch BIGINT,
            proposer_index INTEGER,
            graffiti TEXT,
            randao_reveal TEXT,
            beacon_deposit_count BIGINT,
            slot_root BYTEA,
            parent_root BYTEA,
            mev_fee_recipient BYTEA,
            mev_reward_eth DOUBLE PRECISION,
            mev_tx_hash BYTEA,
            withdrawals_count INTEGER,
            internal_contract_tx_count INTEGER,
            fee_recipient_seconds INTEGER,
            transaction_fees_eth DOUBLE PRECISION,
            withdrawals_root BYTEA,
            nonce BIGINT,
            blob_data JSONB
        );
    `)
	if err != nil {
		return fmt.Errorf("create Blocks table: %w", err)
	}

	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS Users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            password TEXT NOT NULL,
            is_paid BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `)
	if err != nil {
		return fmt.Errorf("create Users table: %w", err)
	}

	// Create Transactions table with from_address and to_address as type_address
	_, err = tx.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS Transactions (
            tx_hash BYTEA PRIMARY KEY,
            block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
            from_address type_address NOT NULL,
            to_address type_address,
            value TEXT NOT NULL,
            gas BIGINT NOT NULL,
            gas_price TEXT NOT NULL,
            type INTEGER NOT NULL,
            chain_id BIGINT NOT NULL,
            access_list JSONB NOT NULL,
            max_fee_per_gas TEXT,
            max_priority_fee_per_gas TEXT,
            transaction_index INTEGER NOT NULL,
            cumulative_gas_used BIGINT NOT NULL,
            is_successful BOOLEAN NOT NULL,
            retrieved_from VARCHAR NOT NULL,
            is_canonical BOOLEAN NOT NULL DEFAULT TRUE,
            timestamp TIMESTAMPTZ,
            is_pending BOOLEAN DEFAULT FALSE
        );
    `)
	if err != nil {
		return fmt.Errorf("create Transactions table: %w", err)
	}

	// Create Accounts table with address as type_address
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS Accounts (
            address type_address PRIMARY KEY,
            balance TEXT NOT NULL,
            nonce INTEGER NOT NULL,
            is_contract BOOLEAN NOT NULL,
            code TEXT NOT NULL,
            first_seen TIMESTAMP NOT NULL,
            last_seen TIMESTAMP NOT NULL,
            retrieved_from VARCHAR NOT NULL
        );
    `)
	if err != nil {
		return fmt.Errorf("create Accounts table: %w", err)
	}

	// Create Contracts table with address as type_address
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS Contracts (
            address type_address PRIMARY KEY REFERENCES Accounts(address),
            contract_name VARCHAR(255) NOT NULL,
            compiler_version VARCHAR(50) NOT NULL,
            abi JSON NOT NULL,
            source_code TEXT NOT NULL,
            optimization_enabled BOOLEAN NOT NULL,
            runs INT,
            constructor_arguments TEXT,
            verified_date TIMESTAMP,
            license VARCHAR(255),
            is_canonical BOOLEAN DEFAULT TRUE,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create Contracts table: %w", err)
	}

	// Create Tokens table with contract_address as type_address
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS Tokens (
            contract_address type_address PRIMARY KEY REFERENCES Accounts(address),
            token_name VARCHAR(255) NOT NULL,
            token_symbol VARCHAR(50) NOT NULL,
            total_supply BIGINT CHECK (total_supply >= 0) NOT NULL,
            decimals INT NOT NULL,
            token_type VARCHAR(50) NOT NULL,
            website VARCHAR(255),
            logo VARCHAR(255),
            is_canonical BOOLEAN DEFAULT TRUE,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create Tokens table: %w", err)
	}

	// Create TokenTransactions table with addresses as type_address
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS TokenTransactions (
            tx_hash BYTEA NOT NULL REFERENCES Transactions(tx_hash),
            contract_address type_address NOT NULL REFERENCES Tokens(contract_address),
            from_address type_address NOT NULL REFERENCES Accounts(address),
            to_address type_address NOT NULL REFERENCES Accounts(address),
            token_id VARCHAR(255),
            value BIGINT CHECK (value >= 0) NOT NULL,
            PRIMARY KEY (tx_hash, contract_address, from_address, to_address, token_id),
            is_canonical BOOLEAN DEFAULT TRUE,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create TokenTransactions table: %w", err)
	}

	// Create NFTs table with addresses as type_address
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS NFTs (
            contract_address type_address NOT NULL REFERENCES Accounts(address),
            token_id VARCHAR(255) NOT NULL,
            token_uri VARCHAR(255) NOT NULL,
            owner type_address NOT NULL REFERENCES Accounts(address),
            metadata JSON,
            PRIMARY KEY (contract_address, token_id),
            is_canonical BOOLEAN DEFAULT TRUE,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create NFTs table: %w", err)
	}

	// Create Validators table
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS Validators (
            validator_index INT PRIMARY KEY,
            public_key BYTEA NOT NULL,
            deposit_amount BIGINT CHECK (deposit_amount >= 0) NOT NULL,
            withdrawal_credentials BYTEA NOT NULL,
            effective_balance BIGINT CHECK (effective_balance >= 0) NOT NULL,
            status VARCHAR(50) NOT NULL,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create Validators table: %w", err)
	}

	// Create GasPrices table
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS GasPrices (
            timestamp TIMESTAMP NOT NULL,
            low_price BIGINT CHECK (low_price >= 0) NOT NULL,
            average_price BIGINT CHECK (average_price >= 0) NOT NULL,
            high_price BIGINT CHECK (high_price >= 0) NOT NULL,
            block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
            PRIMARY KEY (timestamp, block_number),
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create GasPrices table: %w", err)
	}

	// Create ZondNodes table
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS ZondNodes (
            node_id VARCHAR(255) PRIMARY KEY,
            version VARCHAR(50) NOT NULL,
            location VARCHAR(255) NOT NULL,
            last_seen TIMESTAMP NOT NULL,
            latency NUMERIC(10, 2) NOT NULL,
            peers INT NOT NULL,
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            retrieved_from VARCHAR,
            reverted_at TIMESTAMP WITH TIME ZONE
        );
    `)
	if err != nil {
		return fmt.Errorf("create ZondNodes table: %w", err)
	}

	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS InternalTransactions (
        tx_hash BYTEA NOT NULL,
        block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
        from_address type_address NOT NULL,
        to_address type_address NOT NULL,
        value TEXT NOT NULL,
        input TEXT,
        output TEXT,
        type VARCHAR(50),
        gas BIGINT,
        gas_used BIGINT,
        depth INTEGER NOT NULL,
        retrieved_at TIMESTAMP,
        retrieved_from TEXT,
        is_canonical BOOLEAN,
        reverted_at TIMESTAMP,
        PRIMARY KEY (tx_hash, from_address, to_address, depth)
    );`)

	if err != nil {
		return fmt.Errorf("create InternalTransactions table: %w", err)
	}

	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS Blobs (
        blob_hash BYTEA PRIMARY KEY,
        block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
        proposer_index INT NOT NULL,
        data TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        retrieved_from VARCHAR
    );`)
	if err != nil {
		return fmt.Errorf("create Blobs table: %w", err)
	}

	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS BundleTransactions (
        bundle_id VARCHAR(255) NOT NULL,
        tx_hash BYTEA NOT NULL REFERENCES Transactions(tx_hash),
        timestamp TIMESTAMP NOT NULL,
        block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
        bundler_address BYTEA,
        entry_point_address BYTEA,
        method VARCHAR(100),
        aa_txns_count INTEGER,
        amount TEXT,
        gas_price TEXT,
        PRIMARY KEY (bundle_id, tx_hash)
    );`)
	if err != nil {
		return fmt.Errorf("create BundleTransactions table: %w", err)
	}

	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS AccountAbstraction (
        address type_address PRIMARY KEY,
        nonce BIGINT,
        init_code TEXT,
        validation_data TEXT,
        paymaster_and_data TEXT,
        signature TEXT,
        block_number BIGINT REFERENCES Blocks(block_number),
        retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        retrieved_from VARCHAR
    );`)
	if err != nil {
		return fmt.Errorf("create AccountAbstraction table: %w", err)
	}

	_, err = tx.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS TokenBalances (
        address type_address NOT NULL REFERENCES Accounts(address),
        token_address type_address NOT NULL REFERENCES Tokens(contract_address),
        balance NUMERIC(38, 0) NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (address, token_address)
    );
    `)
	if err != nil {
		return err
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
