package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

// indexBlock processes a single block and indexes its data into the database.
func indexBlock(ctx context.Context, client *zondclient.Client, rpcClient *rpc.Client, db *pgxpool.Pool, blockNum uint64, chainID *big.Int) error {
	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", blockNum, err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert Block
	var baseFeePerGas *int64
	if baseFee := block.BaseFee(); baseFee != nil {
		if baseFee.IsInt64() {
			bf := baseFee.Int64()
			baseFeePerGas = &bf
		} else {
			return fmt.Errorf("base_fee_per_gas for block %d is too large for int64: %s", blockNum, baseFee.String())
		}
	}
	// Convert gas_used and gas_limit to strings safely
	gasUsedStr := strconv.FormatUint(block.GasUsed(), 10)
	gasLimitStr := strconv.FormatUint(block.GasLimit(), 10)

	// Validate miner address
	minerAddrBytes := block.Coinbase().Bytes()
	if len(minerAddrBytes) != 20 {
		return fmt.Errorf("block %d: invalid miner address length: got %d bytes, expected 20", blockNum, len(minerAddrBytes))
	}
	// Additional validation: ensure the hex representation is valid
	minerAddrHex := block.Coinbase().Hex()
	if !isValidHexAddress(minerAddrHex) {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddrHex)
	}
	// Log the miner address bytes to check for invalid bytes like 0x89
	log.Printf("Block %d: Miner address bytes: %v", blockNum, minerAddrBytes)

	// Validate BYTEA fields and log their hex representation for debugging
	blockHashBytes := block.Hash().Bytes()
	parentHashBytes := block.ParentHash().Bytes()
	extraDataBytes := block.Extra()
	// Log extra_data as hex for debugging, but store as BYTEA
	extraDataHex := "0x" + hex.EncodeToString(extraDataBytes)
	txRootBytes := block.TxHash().Bytes()
	stateRootBytes := block.Root().Bytes()
	receiptsRootBytes := block.ReceiptHash().Bytes()
	logsBloomBytes := block.Bloom().Bytes()

	// Log all BYTEA fields as hex strings to debug
	log.Printf("Block %d: block_hash=%s, miner_address=%s, parent_hash=%s, extra_data=%s, transactions_root=%s, state_root=%s, receipts_root=%s, logs_bloom=%s",
		blockNum,
		hex.EncodeToString(blockHashBytes),
		hex.EncodeToString(minerAddrBytes),
		hex.EncodeToString(parentHashBytes),
		extraDataHex,
		hex.EncodeToString(txRootBytes),
		hex.EncodeToString(stateRootBytes),
		hex.EncodeToString(receiptsRootBytes),
		hex.EncodeToString(logsBloomBytes))

	// Log the values being inserted for debugging
	log.Printf("Inserting block %d: gas_used=%s, gas_limit=%s, base_fee_per_gas=%v, retrieved_from=%s",
		block.Number().Int64(),
		gasUsedStr,
		gasLimitStr,
		baseFeePerGas,
		"zond_node")

	// Insert all fields in a single statement
	_, err = tx.Exec(ctx,
		`INSERT INTO Blocks (
            block_number, block_hash, timestamp, miner_address, parent_hash,
            gas_used, gas_limit, size, transaction_count, extra_data,
            base_fee_per_gas, transactions_root, state_root, receipts_root,
            logs_bloom, chain_id, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (block_number) DO NOTHING`,
		block.Number().Int64(),
		blockHashBytes,
		time.Unix(int64(block.Time()), 0),
		minerAddrBytes,
		parentHashBytes,
		gasUsedStr,
		gasLimitStr,
		int(block.Size()),
		len(block.Transactions()),
		extraDataBytes,
		baseFeePerGas,
		txRootBytes,
		stateRootBytes,
		receiptsRootBytes,
		logsBloomBytes,
		chainID.Int64(),
		"zond_node")
	if err != nil {
		return fmt.Errorf("insert block %d: %w", blockNum, err)
	}

	// Process Transactions and Collect Accounts
	accounts := make(map[string]bool)
	contracts := make(map[common.Address]bool)      // Track contract addresses
	tokenContracts := make(map[common.Address]bool) // Track token contracts
	nftContracts := make(map[common.Address]bool)   // Track NFT contracts

	// Add the miner address
	minerAddr := block.Coinbase().Hex()
	log.Printf("Block %d: Miner address: %s", blockNum, minerAddr)
	if isValidHexAddress(minerAddr) {
		accounts[minerAddr] = true
	} else {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddr)
	}

	// Calculate Gas Prices for the block
	if err := indexGasPrices(ctx, client, tx, block, blockNum); err != nil {
		return fmt.Errorf("index gas prices for block %d: %w", blockNum, err)
	}

	// Process ZondNodes (node info)
	if err := indexZondNodes(ctx, client, tx); err != nil {
		log.Printf("Failed to index ZondNodes for block %d: %v", blockNum, err)
		// Not critical, so we continue
	}

	// Process Validators (if supported)
	if err := indexValidators(ctx, client, tx); err != nil {
		log.Printf("Failed to index Validators for block %d: %v", blockNum, err)
		// Not critical, so we continue
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

		// Validate from address
		fromAddrBytes := from.Bytes()
		if len(fromAddrBytes) != 20 {
			return fmt.Errorf("tx %s: invalid from address length: got %d bytes, expected 20", transaction.Hash().Hex(), len(fromAddrBytes))
		}

		var toAddress []byte
		var toAddrStr string
		if to := transaction.To(); to != nil {
			toAddress = to.Bytes()
			if len(toAddress) != 20 {
				return fmt.Errorf("tx %s: invalid to address length: got %d bytes, expected 20", transaction.Hash().Hex(), len(toAddress))
			}
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
			accessList = json.RawMessage("[]")
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO Transactions (
                tx_hash, block_number, from_address, to_address, value, gas,
                gas_price, type, chain_id, access_list, max_fee_per_gas,
                max_priority_fee_per_gas, transaction_index, cumulative_gas_used,
                is_successful, retrieved_from
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (tx_hash) DO NOTHING`,
			transaction.Hash().Bytes(),
			block.Number().Int64(),
			fromAddrBytes,
			toAddress,
			transaction.Value().String(),
			int64(transaction.Gas()),
			transaction.GasPrice().String(),
			int(transaction.Type()),
			transaction.ChainId().Int64(),
			accessList,
			maxFeePerGas,
			maxPriorityFeePerGas,
			int(receipt.TransactionIndex),
			int64(receipt.CumulativeGasUsed),
			receipt.Status == 1,
			"zond_node")
		if err != nil {
			return fmt.Errorf("insert transaction %s: %w", transaction.Hash().Hex(), err)
		}

		// Add transaction-related addresses to accounts
		fromAddr := from.Hex()
		log.Printf("Block %d: Transaction %s: From address: %s", blockNum, transaction.Hash().Hex(), fromAddr)
		if isValidHexAddress(fromAddr) {
			accounts[fromAddr] = true
		} else {
			return fmt.Errorf("block %d: invalid from address format: %s", blockNum, fromAddr)
		}

		if toAddrStr != "" {
			log.Printf("Block %d: Transaction %s: To address: %s", blockNum, transaction.Hash().Hex(), toAddrStr)
			if isValidHexAddress(toAddrStr) {
				accounts[toAddrStr] = true
			} else {
				return fmt.Errorf("block %d: invalid to address format: %s", blockNum, toAddrStr)
			}
		}

		// Handle contract creation
		if receipt.ContractAddress != (common.Address{}) {
			contractAddr := receipt.ContractAddress
			contractAddrBytes := contractAddr.Bytes()
			if len(contractAddrBytes) != 20 {
				return fmt.Errorf("tx %s: invalid contract address length: got %d bytes, expected 20", transaction.Hash().Hex(), len(contractAddrBytes))
			}
			contractAddrStr := contractAddr.Hex()
			log.Printf("Block %d: Transaction %s: Contract address: %s", blockNum, transaction.Hash().Hex(), contractAddrStr)
			if isValidHexAddress(contractAddrStr) {
				accounts[contractAddrStr] = true
				contracts[contractAddr] = true

				// Check if this is a token or NFT contract
				isToken, tokenType, err := detectTokenContract(ctx, rpcClient, contractAddr)
				if err != nil {
					log.Printf("Block %d: Failed to detect token type for contract %s: %v", blockNum, contractAddrStr, err)
				} else if isToken {
					if tokenType == "ERC20" {
						tokenContracts[contractAddr] = true
					} else if tokenType == "ERC721" || tokenType == "ERC1155" {
						nftContracts[contractAddr] = true
					}
				}
			} else {
				return fmt.Errorf("block %d: invalid contract address format: %s", blockNum, contractAddrStr)
			}
		}

		// Process Token Transactions and NFTs (via Transfer events)
		if err := indexTokenTransactionsAndNFTs(ctx, rpcClient, tx, transaction, receipt, blockNum); err != nil {
			return fmt.Errorf("index token transactions and NFTs for tx %s: %w", transaction.Hash().Hex(), err)
		}
	}

	// Process Accounts
	timestamp := time.Unix(int64(block.Time()), 0)
	for address := range accounts {
		addr, err := hexToAddress(address)
		if err != nil {
			return fmt.Errorf("convert address %s: %w", address, err)
		}

		addrBytes := addr.Bytes()
		if len(addrBytes) != 20 {
			return fmt.Errorf("account %s: invalid address length: got %d bytes, expected 20", address, len(addrBytes))
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

		var existingBalance string
		var existingNonce int
		err = tx.QueryRow(ctx,
			`SELECT balance, nonce FROM Accounts WHERE address = $1`,
			addrBytes).Scan(&existingBalance, &existingNonce)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("check existing account %s: %w", address, err)
		}

		balanceStr := balance.String()
		nonceInt := int(nonce)
		if err == pgx.ErrNoRows || existingBalance != balanceStr || existingNonce != nonceInt {
			_, err = tx.Exec(ctx,
				`INSERT INTO Accounts (
                    address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (address) DO UPDATE
                SET balance = $2, nonce = $3, last_seen = $7`,
				addrBytes,
				balanceStr,
				nonceInt,
				len(code) > 0,
				hex.EncodeToString(code),
				timestamp,
				timestamp,
				"zond_node")
			if err != nil {
				return fmt.Errorf("insert account %s: %w", address, err)
			}
		}
	}

	// Process Contracts (basic info for now, extend with verification API later)
	for contractAddr := range contracts {
		contractAddrBytes := contractAddr.Bytes()
		if len(contractAddrBytes) != 20 {
			return fmt.Errorf("contract %s: invalid address length: got %d bytes, expected 20", contractAddr.Hex(), len(contractAddrBytes))
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO Contracts (
                address, contract_name, compiler_version, abi, source_code,
                optimization_enabled, runs, constructor_arguments, verified_date,
                license, is_canonical, retrieved_at, retrieved_from
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (address) DO NOTHING`,
			contractAddrBytes,
			"Unknown",
			"Unknown",
			"[]",
			"",
			false,
			0,
			"",
			nil,
			"",
			true,
			time.Now(),
			"zond_node")
		if err != nil {
			return fmt.Errorf("insert contract %s: %w", contractAddr.Hex(), err)
		}
	}

	// Process Tokens
	for tokenAddr := range tokenContracts {
		if err := indexToken(ctx, rpcClient, tx, tokenAddr, blockNum); err != nil {
			return fmt.Errorf("index token %s: %w", tokenAddr.Hex(), err)
		}
	}

	// Process NFTs
	for nftAddr := range nftContracts {
		if err := indexNFTs(ctx, rpcClient, tx, nftAddr, blockNum); err != nil {
			return fmt.Errorf("index NFTs for contract %s: %w", nftAddr.Hex(), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Indexed block %d with %d transactions, %d accounts, %d contracts, %d tokens, %d NFTs",
		blockNum, len(block.Transactions()), len(accounts), len(contracts), len(tokenContracts), len(nftContracts))
	return nil
}

// isValidHexAddress checks if an address is a valid 40-character hex string (with Z or 0x prefix)
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

// hexToAddress converts a hex string to a common.Address
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
