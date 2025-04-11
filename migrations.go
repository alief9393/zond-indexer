package main

import (
	"database/sql"
	"fmt"
)

// Migrate applies the database schema migrations.
func Migrate(db *sql.DB) error {
	// Start a transaction for the migration
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create the type_address domain for Ethereum/Zond addresses (20 bytes)
	var domainExists bool
	err = tx.QueryRow(`
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
		_, err = tx.Exec(`
            CREATE DOMAIN type_address AS BYTEA
            CHECK (octet_length(VALUE) = 20);
        `)
		if err != nil {
			return fmt.Errorf("failed to create type_address domain: %w", err)
		}
	}

	// Create Blocks table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Blocks (
            block_number BIGINT PRIMARY KEY,
            block_hash BYTEA NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            miner_address BYTEA NOT NULL,
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
            retrieved_from VARCHAR NOT NULL
        );
    `)
	if err != nil {
		return fmt.Errorf("create Blocks table: %w", err)
	}

	// Create Transactions table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Transactions (
            tx_hash BYTEA PRIMARY KEY,
            block_number BIGINT NOT NULL REFERENCES Blocks(block_number),
            from_address BYTEA NOT NULL,
            to_address BYTEA,
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
            retrieved_from VARCHAR NOT NULL
        );
    `)
	if err != nil {
		return fmt.Errorf("create Transactions table: %w", err)
	}

	// Create Accounts table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Accounts (
            address BYTEA PRIMARY KEY,
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

	// Create Contracts table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Contracts (
            address BYTEA PRIMARY KEY REFERENCES Accounts(address),
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

	// Create Tokens table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Tokens (
            contract_address BYTEA PRIMARY KEY REFERENCES Accounts(address),
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

	// Create TokenTransactions table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS TokenTransactions (
            tx_hash BYTEA NOT NULL REFERENCES Transactions(tx_hash),
            contract_address BYTEA NOT NULL REFERENCES Tokens(contract_address),
            from_address BYTEA NOT NULL REFERENCES Accounts(address),
            to_address BYTEA NOT NULL REFERENCES Accounts(address),
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

	// Create NFTs table
	_, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS NFTs (
            contract_address BYTEA NOT NULL REFERENCES Accounts(address),
            token_id VARCHAR(255) NOT NULL,
            token_uri VARCHAR(255) NOT NULL,
            owner BYTEA NOT NULL REFERENCES Accounts(address),
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
	// Note: This schema is based on Ethereum's validator data and may need adjustment
	// for Zond's consensus mechanism, which might differ (e.g., PoS, PoA, or custom).
	_, err = tx.Exec(`
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
	_, err = tx.Exec(`
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

	// Create ZondNodes table (renamed from EthereumNodes)
	_, err = tx.Exec(`
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

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
