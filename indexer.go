package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/theQRL/go-zond/accounts/abi"
	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/common/hexutil"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

// indexBlock processes a single block and indexes its data into the database.
func indexBlock(ctx context.Context, client *zondclient.Client, rpcClient *rpc.Client, db *sql.DB, blockNum uint64, chainID *big.Int) error {
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

	// Log the values being inserted for debugging
	log.Printf("Inserting block %d: gas_used=%s, gas_limit=%s, base_fee_per_gas=%v, retrieved_from=%s",
		block.Number().Int64(),
		gasUsedStr,
		gasLimitStr,
		baseFeePerGas,
		"zond_node")
	_, err = tx.ExecContext(ctx,
		`INSERT INTO Blocks (
            block_number, block_hash, timestamp, miner_address, parent_hash,
            gas_used, gas_limit, size, transaction_count, extra_data,
            base_fee_per_gas, transactions_root, state_root, receipts_root,
            logs_bloom, chain_id, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (block_number) DO NOTHING`,
		block.Number().Int64(),
		block.Hash().Bytes(),
		time.Unix(int64(block.Time()), 0),
		block.Coinbase().Bytes(),
		block.ParentHash().Bytes(),
		gasUsedStr,
		gasLimitStr,
		int(block.Size()),
		len(block.Transactions()),
		block.Extra(),
		baseFeePerGas, // Now a BIGINT
		block.TxHash().Bytes(),
		block.Root().Bytes(),
		block.ReceiptHash().Bytes(),
		block.Bloom().Bytes(),
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
		log.Printf("Block %d: Skipping invalid miner address: %s", blockNum, minerAddr)
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
			accessList = json.RawMessage("[]")
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO Transactions (
                tx_hash, block_number, from_address, to_address, value, gas,
                gas_price, type, chain_id, access_list, max_fee_per_gas,
                max_priority_fee_per_gas, transaction_index, cumulative_gas_used,
                is_successful, retrieved_from
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (tx_hash) DO NOTHING`,
			transaction.Hash().Bytes(),
			block.Number().Int64(),
			from.Bytes(),
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

		// Handle contract creation
		if receipt.ContractAddress != (common.Address{}) {
			contractAddr := receipt.ContractAddress
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
				log.Printf("Block %d: Skipping invalid contract address: %s", blockNum, contractAddrStr)
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
		err = tx.QueryRowContext(ctx,
			`SELECT balance, nonce FROM Accounts WHERE address = $1`,
			addr.Bytes()).Scan(&existingBalance, &existingNonce)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("check existing account %s: %w", address, err)
		}

		balanceStr := balance.String()
		nonceInt := int(nonce)
		if err == sql.ErrNoRows || existingBalance != balanceStr || existingNonce != nonceInt {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO Accounts (
                    address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (address) DO UPDATE
                SET balance = $2, nonce = $3, last_seen = $7`,
				addr.Bytes(),
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
		_, err := tx.ExecContext(ctx,
			`INSERT INTO Contracts (
                address, contract_name, compiler_version, abi, source_code,
                optimization_enabled, runs, constructor_arguments, verified_date,
                license, is_canonical, retrieved_at, retrieved_from
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (address) DO NOTHING`,
			contractAddr.Bytes(),
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Indexed block %d with %d transactions, %d accounts, %d contracts, %d tokens, %d NFTs",
		blockNum, len(block.Transactions()), len(accounts), len(contracts), len(tokenContracts), len(nftContracts))
	return nil
}

// Helper Functions

// isValidHexAddress checks if an address is a valid 40-character hex string (with or without 0x prefix)
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

// callContractRaw performs a raw zond_call RPC call to the given address with the provided data
func callContractRaw(ctx context.Context, rpcClient *rpc.Client, addr common.Address, data []byte) ([]byte, error) {
	var hexResult hexutil.Bytes
	err := rpcClient.CallContext(ctx, &hexResult, "zond_call", map[string]interface{}{
		"to":   addr.Hex(),
		"data": hexutil.Encode(data),
	}, "latest")
	if err != nil {
		return nil, fmt.Errorf("zond_call failed: %w", err)
	}
	return hexResult, nil
}

// detectTokenContract checks if a contract is a token (ERC20, ERC721, ERC1155)
func detectTokenContract(ctx context.Context, rpcClient *rpc.Client, addr common.Address) (bool, string, error) {
	// Check for ERC-20 (totalSupply, balanceOf, transfer)
	erc20ABI, _ := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"type":"function"}]`))
	result, err := callContractRaw(ctx, rpcClient, addr, erc20ABI.Methods["totalSupply"].ID)
	if err == nil {
		return true, "ERC20", nil
	}

	// Check for ERC-721 (supportsInterface for 0x80ac58cd)
	erc721InterfaceID := common.HexToHash("0x80ac58cd")
	supportsInterfaceABI, _ := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"name":"","type":"bool"}],"type":"function"}]`))
	data, _ := supportsInterfaceABI.Pack("supportsInterface", erc721InterfaceID)
	result, err = callContractRaw(ctx, rpcClient, addr, data)
	if err == nil && len(result) > 0 {
		var supports bool
		supportsInterfaceABI.UnpackIntoInterface(&supports, "supportsInterface", result)
		if supports {
			return true, "ERC721", nil
		}
	}

	// Check for ERC-1155 (supportsInterface for 0xd9b67a26)
	erc1155InterfaceID := common.HexToHash("0xd9b67a26")
	data, _ = supportsInterfaceABI.Pack("supportsInterface", erc1155InterfaceID)
	result, err = callContractRaw(ctx, rpcClient, addr, data)
	if err == nil && len(result) > 0 {
		var supports bool
		supportsInterfaceABI.UnpackIntoInterface(&supports, "supportsInterface", result)
		if supports {
			return true, "ERC1155", nil
		}
	}

	return false, "", nil
}

// indexToken fetches token metadata and stores it in the Tokens table
func indexToken(ctx context.Context, rpcClient *rpc.Client, tx *sql.Tx, addr common.Address, blockNum uint64) error {
	// ERC-20 ABI for name, symbol, totalSupply, decimals
	erc20ABI, _ := abi.JSON(strings.NewReader(`[
        {"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
        {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
        {"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"type":"function"},
        {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
    ]`))

	// Fetch name
	nameData, _ := erc20ABI.Pack("name")
	nameResult, err := callContractRaw(ctx, rpcClient, addr, nameData)
	if err != nil {
		return fmt.Errorf("fetch name for token %s: %w", addr.Hex(), err)
	}
	var name string
	erc20ABI.UnpackIntoInterface(&name, "name", nameResult)

	// Fetch symbol
	symbolData, _ := erc20ABI.Pack("symbol")
	symbolResult, err := callContractRaw(ctx, rpcClient, addr, symbolData)
	if err != nil {
		return fmt.Errorf("fetch symbol for token %s: %w", addr.Hex(), err)
	}
	var symbol string
	erc20ABI.UnpackIntoInterface(&symbol, "symbol", symbolResult)

	// Fetch totalSupply
	totalSupplyData, _ := erc20ABI.Pack("totalSupply")
	totalSupplyResult, err := callContractRaw(ctx, rpcClient, addr, totalSupplyData)
	if err != nil {
		return fmt.Errorf("fetch totalSupply for token %s: %w", addr.Hex(), err)
	}
	var totalSupply *big.Int
	erc20ABI.UnpackIntoInterface(&totalSupply, "totalSupply", totalSupplyResult)

	// Fetch decimals
	decimalsData, _ := erc20ABI.Pack("decimals")
	decimalsResult, err := callContractRaw(ctx, rpcClient, addr, decimalsData)
	if err != nil {
		return fmt.Errorf("fetch decimals for token %s: %w", addr.Hex(), err)
	}
	var decimals uint8
	erc20ABI.UnpackIntoInterface(&decimals, "decimals", decimalsResult)

	// Insert into Tokens table
	_, err = tx.ExecContext(ctx,
		`INSERT INTO Tokens (
            contract_address, token_name, token_symbol, total_supply, decimals,
            token_type, website, logo, is_canonical, retrieved_at, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (contract_address) DO NOTHING`,
		addr.Bytes(),
		name,
		symbol,
		totalSupply.Int64(),
		int(decimals),
		"ERC20",
		"",
		"",
		true,
		time.Now(),
		"zond_node")
	if err != nil {
		return fmt.Errorf("insert token %s: %w", addr.Hex(), err)
	}

	return nil
}

// indexTokenTransactionsAndNFTs processes Transfer events for tokens and NFTs
func indexTokenTransactionsAndNFTs(ctx context.Context, rpcClient *rpc.Client, tx *sql.Tx, transaction *types.Transaction, receipt *types.Receipt, blockNum uint64) error {
	transferEventSig := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ce") // Transfer event signature
	transferABI, _ := abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`))
	// Note: transferNFTABI might be needed for ERC-1155 events in the future

	for _, logEntry := range receipt.Logs {
		if len(logEntry.Topics) == 0 || logEntry.Topics[0] != transferEventSig {
			continue
		}

		contractAddr := common.BytesToAddress(logEntry.Address.Bytes())
		isToken, tokenType, err := detectTokenContract(ctx, rpcClient, contractAddr)
		if err != nil {
			log.Printf("Failed to detect token type for contract %s: %v", contractAddr.Hex(), err)
			continue
		}
		if !isToken {
			continue
		}

		var fromAddr, toAddr common.Address
		var value *big.Int
		var tokenID *big.Int

		if tokenType == "ERC20" {
			// ERC-20 Transfer event
			if len(logEntry.Topics) != 3 {
				continue
			}
			fromAddr = common.BytesToAddress(logEntry.Topics[1].Bytes())
			toAddr = common.BytesToAddress(logEntry.Topics[2].Bytes())
			err = transferABI.UnpackIntoInterface(&struct {
				Value *big.Int
			}{Value: value}, "Transfer", logEntry.Data)
			if err != nil {
				return fmt.Errorf("unpack ERC-20 Transfer event: %w", err)
			}

			// Insert into TokenTransactions
			_, err = tx.ExecContext(ctx,
				`INSERT INTO TokenTransactions (
                    tx_hash, contract_address, from_address, to_address, token_id,
                    value, is_canonical, retrieved_at, retrieved_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (tx_hash, contract_address, from_address, to_address, token_id) DO NOTHING`,
				transaction.Hash().Bytes(),
				contractAddr.Bytes(),
				fromAddr.Bytes(),
				toAddr.Bytes(),
				nil,
				value.Int64(),
				true,
				time.Now(),
				"zond_node")
			if err != nil {
				return fmt.Errorf("insert token transaction for tx %s: %w", transaction.Hash().Hex(), err)
			}
		} else if tokenType == "ERC721" {
			// ERC-721 Transfer event
			if len(logEntry.Topics) != 4 {
				continue
			}
			fromAddr = common.BytesToAddress(logEntry.Topics[1].Bytes())
			toAddr = common.BytesToAddress(logEntry.Topics[2].Bytes())
			tokenID = logEntry.Topics[3].Big()

			// Fetch tokenURI and metadata
			tokenURI, metadata, err := fetchNFTMetadata(ctx, rpcClient, contractAddr, tokenID)
			if err != nil {
				log.Printf("Failed to fetch NFT metadata for contract %s, tokenID %s: %v", contractAddr.Hex(), tokenID.String(), err)
				continue
			}

			// Insert into NFTs
			var metadataJSON []byte
			if metadata != nil {
				metadataJSON, err = json.Marshal(metadata)
				if err != nil {
					return fmt.Errorf("marshal NFT metadata: %w", err)
				}
			}
			_, err = tx.ExecContext(ctx,
				`INSERT INTO NFTs (
                    contract_address, token_id, token_uri, owner, metadata,
                    is_canonical, retrieved_at, retrieved_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (contract_address, token_id) DO UPDATE
                SET owner = $4, token_uri = $3, metadata = $5`,
				contractAddr.Bytes(),
				tokenID.String(),
				tokenURI,
				toAddr.Bytes(),
				metadataJSON,
				true,
				time.Now(),
				"zond_node")
			if err != nil {
				return fmt.Errorf("insert NFT for contract %s, tokenID %s: %w", contractAddr.Hex(), tokenID.String(), err)
			}
		}
	}

	return nil
}

// fetchNFTMetadata fetches tokenURI and metadata for an NFT
func fetchNFTMetadata(ctx context.Context, rpcClient *rpc.Client, contractAddr common.Address, tokenID *big.Int) (string, map[string]interface{}, error) {
	// ERC-721 ABI for tokenURI
	erc721ABI, _ := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"type":"function"}]`))
	data, _ := erc721ABI.Pack("tokenURI", tokenID)
	result, err := callContractRaw(ctx, rpcClient, contractAddr, data)
	if err != nil {
		return "", nil, fmt.Errorf("fetch tokenURI: %w", err)
	}
	var tokenURI string
	erc721ABI.UnpackIntoInterface(&tokenURI, "tokenURI", result)

	// Placeholder for metadata (in a real implementation, fetch from tokenURI if it's a URL)
	metadata := map[string]interface{}{
		"name": "NFT #" + tokenID.String(),
	}

	return tokenURI, metadata, nil
}

// indexNFTs fetches existing NFTs for a contract (historical data)
func indexNFTs(ctx context.Context, rpcClient *rpc.Client, tx *sql.Tx, contractAddr common.Address, blockNum uint64) error {
	// For simplicity, we're only indexing NFTs via Transfer events in indexTokenTransactionsAndNFTs.
	// To index historical NFTs, you would need to query past Transfer events using a filter,
	// which requires an Ethereum client with event filtering support.
	// This is a placeholder for future implementation.
	return nil
}

// indexGasPrices calculates gas price statistics for a block
func indexGasPrices(ctx context.Context, client *zondclient.Client, tx *sql.Tx, block *types.Block, blockNum uint64) error {
	var gasPrices []*big.Int
	for _, transaction := range block.Transactions() {
		gasPrice := transaction.GasPrice()
		if transaction.Type() >= types.DynamicFeeTxType {
			gasPrice = transaction.GasFeeCap()
		}
		gasPrices = append(gasPrices, gasPrice)
	}

	if len(gasPrices) == 0 {
		gasPrices = append(gasPrices, big.NewInt(0))
	}

	// Sort gas prices to find low, average, high
	sort.Slice(gasPrices, func(i, j int) bool {
		return gasPrices[i].Cmp(gasPrices[j]) < 0
	})
	lowPrice := gasPrices[0]
	highPrice := gasPrices[len(gasPrices)-1]
	var totalPrice big.Int
	for _, price := range gasPrices {
		totalPrice.Add(&totalPrice, price)
	}
	averagePrice := new(big.Int).Div(&totalPrice, big.NewInt(int64(len(gasPrices))))

	timestamp := time.Unix(int64(block.Time()), 0)
	_, err := tx.ExecContext(ctx,
		`INSERT INTO GasPrices (
            timestamp, low_price, average_price, high_price, block_number,
            retrieved_at, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (timestamp, block_number) DO NOTHING`,
		timestamp,
		lowPrice.Int64(),
		averagePrice.Int64(),
		highPrice.Int64(),
		blockNum,
		time.Now(),
		"zond_node")
	if err != nil {
		return fmt.Errorf("insert gas prices: %w", err)
	}

	return nil
}

// indexValidators fetches validator data (placeholder)
func indexValidators(ctx context.Context, client *zondclient.Client, tx *sql.Tx) error {
	// Zond may not expose validator data via go-zond directly.
	// This requires a beacon chain client or API.
	// Placeholder: In a real implementation, you would fetch validator data from a beacon chain API.
	log.Println("Validator indexing not implemented: requires beacon chain API")
	return nil
}

// indexZondNodes fetches node information
func indexZondNodes(ctx context.Context, client *zondclient.Client, tx *sql.Tx) error {
	// Skip node version fetching since ClientVersion is not available in go-zond
	nodeInfo := "unknown"

	// Fetch peer count
	peerCount, err := client.PeerCount(ctx)
	if err != nil {
		return fmt.Errorf("fetch peer count: %w", err)
	}

	// Placeholder for latency and location
	latency := 50.0 // Example latency in milliseconds
	location := "Unknown"

	nodeID := "node-" + nodeInfo // Simplified node ID
	_, err = tx.ExecContext(ctx,
		`INSERT INTO ZondNodes (
            node_id, version, location, last_seen, latency, peers,
            retrieved_at, retrieved_from
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (node_id) DO UPDATE
        SET version = $2, location = $3, last_seen = $4, latency = $5, peers = $6`,
		nodeID,
		nodeInfo,
		location,
		time.Now(),
		latency,
		int(peerCount),
		time.Now(),
		"zond_node")
	if err != nil {
		return fmt.Errorf("insert ZondNodes: %w", err)
	}

	return nil
}
