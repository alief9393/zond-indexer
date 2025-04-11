package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/zondclient"
)

func indexBlock(ctx context.Context, client *zondclient.Client, db *sql.DB, blockNum uint64, chainID *big.Int) error {
	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", blockNum, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert Block
	var baseFeePerGas *string
	if baseFee := block.BaseFee(); baseFee != nil {
		bf := baseFee.String()
		baseFeePerGas = &bf
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO Blocks (
            block_number, block_hash, timestamp, miner_address, parent_hash,
            gas_used, gas_limit, size, transaction_count, extra_data,
            base_fee_per_gas, transactions_root, state_root, receipts_root,
            logs_bloom, chain_id, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (block_number) DO NOTHING`,
		block.Number().Int64(), block.Hash().Bytes(), time.Unix(int64(block.Time()), 0),
		block.Coinbase().Bytes(), block.ParentHash().Bytes(),
		new(big.Int).SetUint64(block.GasUsed()).String(),
		new(big.Int).SetUint64(block.GasLimit()).String(),
		int(block.Size()), len(block.Transactions()), block.Extra(),
		baseFeePerGas, block.TxHash().Bytes(), block.Root().Bytes(),
		block.ReceiptHash().Bytes(), block.Bloom().Bytes(), chainID.Int64(), "local-node")
	if err != nil {
		return fmt.Errorf("insert block %d: %w", blockNum, err)
	}

	// Process Transactions and Collect Accounts
	accounts := make(map[string]bool)
	// Add the miner address
	minerAddr := block.Coinbase().Hex()
	log.Printf("Block %d: Miner address: %s", blockNum, minerAddr)
	if isValidHexAddress(minerAddr) {
		accounts[minerAddr] = true
	} else {
		log.Printf("Block %d: Skipping invalid miner address: %s", blockNum, minerAddr)
	}

	for _, transaction := range block.Transactions() {
		receipt, err := client.TransactionReceipt(ctx, transaction.Hash())
		if err != nil {
			return fmt.Errorf("fetch receipt for tx %s: %w", transaction.Hash().Hex(), err)
		}

		from, err := types.Sender(types.LatestSignerForChainID(transaction.ChainId()), transaction)
		if err != nil {
			return fmt.Errorf("get sender for tx %s: %w", transaction.Hash().Hex(), err)
		}

		var toAddress []byte
		var toAddrStr string
		if to := transaction.To(); to != nil {
			toAddress = to.Bytes()
			toAddrStr = to.Hex()
		}

		var maxFeePerGas, maxPriorityFeePerGas *string
		if mfg := transaction.GasFeeCap(); mfg != nil {
			mfgStr := mfg.String()
			maxFeePerGas = &mfgStr
		}
		if mpf := transaction.GasTipCap(); mpf != nil {
			mpfStr := mpf.String()
			maxPriorityFeePerGas = &mpfStr
		}

		// Marshal AccessList to JSON
		var accessList json.RawMessage
		if al := transaction.AccessList(); al != nil {
			accessListBytes, err := json.Marshal(al)
			if err != nil {
				return fmt.Errorf("marshal access list for tx %s: %w", transaction.Hash().Hex(), err)
			}
			accessList = accessListBytes
		} else {
			accessList = json.RawMessage("[]") // Empty list if nil
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO Transactions (
                tx_hash, block_number, from_address, to_address, value, gas,
                gas_price, type, chain_id, access_list, max_fee_per_gas,
                max_priority_fee_per_gas, transaction_index, cumulative_gas_used,
                is_successful, retrieved_from
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (tx_hash) DO NOTHING`,
			transaction.Hash().Bytes(), block.Number().Int64(), from.Bytes(), toAddress,
			transaction.Value().String(), int64(transaction.Gas()),
			transaction.GasPrice().String(), int(transaction.Type()),
			int(transaction.ChainId().Int64()), accessList,
			maxFeePerGas, maxPriorityFeePerGas, int(receipt.TransactionIndex),
			int64(receipt.CumulativeGasUsed), receipt.Status == 1, "local-node")
		if err != nil {
			return fmt.Errorf("insert transaction %s: %w", transaction.Hash().Hex(), err)
		}

		// Add transaction-related addresses to accounts
		fromAddr := from.Hex()
		log.Printf("Block %d: Transaction %s: From address: %s", blockNum, transaction.Hash().Hex(), fromAddr)
		if isValidHexAddress(fromAddr) {
			accounts[fromAddr] = true
		} else {
			log.Printf("Block %d: Skipping invalid from address: %s", blockNum, fromAddr)
		}

		if toAddrStr != "" {
			log.Printf("Block %d: Transaction %s: To address: %s", blockNum, transaction.Hash().Hex(), toAddrStr)
			if isValidHexAddress(toAddrStr) {
				accounts[toAddrStr] = true
			} else {
				log.Printf("Block %d: Skipping invalid to address: %s", blockNum, toAddrStr)
			}
		}

		// Add contract address if this transaction created a contract
		if receipt.ContractAddress != (common.Address{}) {
			contractAddr := receipt.ContractAddress.Hex()
			log.Printf("Block %d: Transaction %s: Contract address: %s", blockNum, transaction.Hash().Hex(), contractAddr)
			if isValidHexAddress(contractAddr) {
				accounts[contractAddr] = true
			} else {
				log.Printf("Block %d: Skipping invalid contract address: %s", blockNum, contractAddr)
			}
		}
	}

	// Process Accounts
	timestamp := time.Unix(int64(block.Time()), 0)
	for address := range accounts {
		addr, err := hexToAddress(address)
		if err != nil {
			return fmt.Errorf("convert address %s: %w", address, err)
		}

		balance, err := client.BalanceAt(ctx, addr, block.Number())
		if err != nil {
			return fmt.Errorf("fetch balance for %s: %w", address, err)
		}
		nonce, err := client.NonceAt(ctx, addr, block.Number())
		if err != nil {
			return fmt.Errorf("fetch nonce for %s: %w", address, err)
		}
		code, err := client.CodeAt(ctx, addr, block.Number())
		if err != nil {
			return fmt.Errorf("fetch code for %s: %w", address, err)
		}

		// Check existing account data to avoid unnecessary updates
		var existingBalance string
		var existingNonce int
		err = tx.QueryRowContext(ctx,
			`SELECT balance, nonce FROM Accounts WHERE address = $1`,
			addr.Bytes()).Scan(&existingBalance, &existingNonce)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("check existing account %s: %w", address, err)
		}

		// Only update if the values have changed or it's a new account
		balanceStr := balance.String()
		nonceInt := int(nonce)
		if err == sql.ErrNoRows || existingBalance != balanceStr || existingNonce != nonceInt {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO Accounts (
                    address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (address) DO UPDATE
                SET balance = $2, nonce = $3, last_seen = $7`,
				addr.Bytes(), balanceStr, nonceInt, len(code) > 0,
				hex.EncodeToString(code), timestamp, timestamp, "local-node")
			if err != nil {
				return fmt.Errorf("insert account %s: %w", address, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Indexed block %d with %d transactions and %d accounts", blockNum, len(block.Transactions()), len(accounts))
	return nil
}

// isValidHexAddress checks if the address is a valid Zond address (Z prefix + 40 hex characters).
func isValidHexAddress(addr string) bool {
	// Check for Z prefix
	if !strings.HasPrefix(addr, "Z") {
		// Also allow standard 0x prefix for compatibility
		if !strings.HasPrefix(addr, "0x") {
			return false
		}
		addr = strings.TrimPrefix(addr, "0x")
	} else {
		addr = strings.TrimPrefix(addr, "Z")
	}

	// Check length (40 characters for 20 bytes)
	if len(addr) != 40 {
		return false
	}

	// Check if all characters are valid hex digits
	for _, char := range addr {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func hexToAddress(s string) (common.Address, error) {
	original := s // For logging
	// Handle Z prefix for Zond addresses
	if strings.HasPrefix(s, "Z") {
		s = strings.TrimPrefix(s, "Z")
	} else {
		// Also handle standard 0x prefix for compatibility
		s = strings.TrimPrefix(s, "0x")
	}

	// Log the transformation
	log.Printf("Converting address: %s -> %s", original, s)

	// Validate length
	if len(s) != 40 {
		return common.Address{}, fmt.Errorf("invalid address length: %d (expected 40 characters)", len(s))
	}

	// Convert to lowercase to ensure consistent decoding
	s = strings.ToLower(s)
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return common.Address{}, err
	}

	var addr common.Address
	copy(addr[:], bytes)
	return addr, nil
}
