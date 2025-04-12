package indexer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"time"

	"ZOND-INDEXER/internal/config"
	"ZOND-INDEXER/internal/models"
	"ZOND-INDEXER/internal/node"
	"ZOND-INDEXER/internal/token"
	"ZOND-INDEXER/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

// Indexer holds the state for indexing Zond blocks
type Indexer struct {
	config             config.Config
	client             *zondclient.Client
	rpcClient          *rpc.Client
	db                 *pgxpool.Pool
	chainID            *big.Int
	rateLimit          time.Duration
	latest             uint64
	historical         uint64
	lastValidatorIndex uint64
	epochLength        uint64
}

func NewIndexer(config config.Config) (*Indexer, error) {
	// Connect to the Zond node
	rpcClient, err := rpc.Dial(config.RPCEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC endpoint: %w", err)
	}
	client := zondclient.NewClient(rpcClient)

	// Fetch the chain ID
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain ID: %w", err)
	}

	// Connect to the database
	dbConfig, err := pgxpool.ParseConfig(config.PostgresConn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Postgres connection string: %w", err)
	}
	db, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the db connection
	err = db.Ping(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Fetch the latest block number
	latest, err := client.BlockNumber(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to fetch latest block number: %w", err)
	}

	return &Indexer{
		config:             config,
		client:             client,
		rpcClient:          rpcClient,
		db:                 db,
		chainID:            chainID,
		rateLimit:          config.RateLimit,
		latest:             latest,
		historical:         latest,
		lastValidatorIndex: 0,
		epochLength:        32,
	}, nil
}

func (i *Indexer) Close() {
	i.db.Close()
	i.rpcClient.Close()
}

func (i *Indexer) Config() config.Config {
	return i.config
}

func (i *Indexer) Client() *zondclient.Client {
	return i.client
}

func (i *Indexer) RPCClient() *rpc.Client {
	return i.rpcClient
}

func (i *Indexer) DB() *pgxpool.Pool {
	return i.db
}

func (i *Indexer) ChainID() *big.Int {
	return i.chainID
}

func (i *Indexer) Latest() uint64 {
	return i.latest
}

func (i *Indexer) Historical() uint64 {
	return i.historical
}

// indexValidatorsPeriodically indexes validators if the current block is at an epoch boundary
func (i *Indexer) indexValidatorsPeriodically(ctx context.Context, blockNum uint64) error {
	// Index validators on startup (block 0) or every epoch
	if blockNum == 0 || (blockNum > 0 && blockNum%i.epochLength == 0 && blockNum > i.lastValidatorIndex) {
		tx, err := i.db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin transaction for validator indexing: %w", err)
		}
		defer tx.Rollback(ctx)

		// Pass the Config to IndexValidators
		if err := models.IndexValidators(ctx, i.client, tx, i.config); err != nil {
			log.Printf("Failed to index Validators at block %d: %v", blockNum, err)
			return nil // Not critical, continue indexing blocks
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit transaction for validator indexing: %w", err)
		}

		i.lastValidatorIndex = blockNum
		log.Printf("Indexed validators at block %d", blockNum)
	}
	return nil
}

// indexGasPrices calculates gas price for a block
func indexGasPrices(ctx context.Context, client *zondclient.Client, tx pgx.Tx, block *types.Block, blockNum uint64) error {
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
	gasPrices = sortGasPrices(gasPrices)
	lowPrice := gasPrices[0]
	highPrice := gasPrices[len(gasPrices)-1]
	averagePrice := calculateAverageGasPrice(gasPrices)

	timestamp := time.Unix(int64(block.Time()), 0)
	_, err := tx.Exec(ctx,
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

// sortGasPrices sorts gas prices in ascending order
func sortGasPrices(gasPrices []*big.Int) []*big.Int {
	sorted := make([]*big.Int, len(gasPrices))
	copy(sorted, gasPrices)
	sortSlice(sorted, func(i, j int) bool {
		return sorted[i].Cmp(sorted[j]) < 0
	})
	return sorted
}

// calculateAverageGasPrice calculates the average gas price
func calculateAverageGasPrice(gasPrices []*big.Int) *big.Int {
	var totalPrice big.Int
	for _, price := range gasPrices {
		totalPrice.Add(&totalPrice, price)
	}
	return new(big.Int).Div(&totalPrice, big.NewInt(int64(len(gasPrices))))
}

func sortSlice[T any](slice []T, less func(i, j int) bool) {
	for i := 1; i < len(slice); i++ {
		for j := i; j > 0 && less(j, j-1); j-- {
			slice[j], slice[j-1] = slice[j-1], slice[j]
		}
	}
}

// indexBlock processes a single block and indexes its data into the db.
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
	gasUsedStr := strconv.FormatUint(block.GasUsed(), 10)
	gasLimitStr := strconv.FormatUint(block.GasLimit(), 10)

	minerAddrBytes := block.Coinbase().Bytes()
	if len(minerAddrBytes) != 20 {
		return fmt.Errorf("block %d: invalid miner address length: got %d bytes, expected 20", blockNum, len(minerAddrBytes))
	}
	minerAddrHex := block.Coinbase().Hex()
	if !utils.IsValidHexAddress(minerAddrHex) {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddrHex)
	}

	blockHashBytes := block.Hash().Bytes()
	parentHashBytes := block.ParentHash().Bytes()
	extraDataBytes := block.Extra()
	txRootBytes := block.TxHash().Bytes()
	stateRootBytes := block.Root().Bytes()
	receiptsRootBytes := block.ReceiptHash().Bytes()
	logsBloomBytes := block.Bloom().Bytes()

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
	contracts := make(map[common.Address]bool)
	tokenContracts := make(map[common.Address]bool)
	nftContracts := make(map[common.Address]bool)

	// Add the miner address
	minerAddr := block.Coinbase().Hex()
	if utils.IsValidHexAddress(minerAddr) {
		accounts[minerAddr] = true
	} else {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddr)
	}

	// Calculate Gas Prices for the block
	if err := indexGasPrices(ctx, client, tx, block, blockNum); err != nil {
		return fmt.Errorf("index gas prices for block %d: %w", blockNum, err)
	}

	// Process ZondNodes (node info)
	if err := node.IndexZondNodes(ctx, client, tx); err != nil {
		log.Printf("Failed to index ZondNodes for block %d: %v", blockNum, err)
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

		fromAddr := from.Hex()
		if utils.IsValidHexAddress(fromAddr) {
			accounts[fromAddr] = true
		} else {
			return fmt.Errorf("block %d: invalid from address format: %s", blockNum, fromAddr)
		}

		if toAddrStr != "" {
			if utils.IsValidHexAddress(toAddrStr) {
				accounts[toAddrStr] = true
			} else {
				return fmt.Errorf("block %d: invalid to address format: %s", blockNum, toAddrStr)
			}
		}

		if receipt.ContractAddress != (common.Address{}) {
			contractAddr := receipt.ContractAddress
			contractAddrBytes := contractAddr.Bytes()
			if len(contractAddrBytes) != 20 {
				return fmt.Errorf("tx %s: invalid contract address length: got %d bytes, expected 20", transaction.Hash().Hex(), len(contractAddrBytes))
			}
			contractAddrStr := contractAddr.Hex()
			if utils.IsValidHexAddress(contractAddrStr) {
				accounts[contractAddrStr] = true
				contracts[contractAddr] = true

				isToken, tokenType, err := token.DetectTokenContract(ctx, rpcClient, contractAddr)
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

		if err := token.IndexTokenTransactionsAndNFTs(ctx, rpcClient, tx, transaction, receipt, blockNum); err != nil {
			return fmt.Errorf("index token transactions and NFTs for tx %s: %w", transaction.Hash().Hex(), err)
		}
	}

	timestamp := time.Unix(int64(block.Time()), 0)
	for address := range accounts {
		addr, err := utils.HexToAddress(address)
		if err != nil {
			return fmt.Errorf("convert address %s: %w", address, err)
		}

		addrBytes := addr.Bytes()
		if len(addrBytes) != 20 {
			return fmt.Errorf("account %s: invalid address length: %d bytes, expected 20", address, len(addrBytes))
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

	for contractAddr := range contracts {
		contractAddrBytes := contractAddr.Bytes()
		if len(contractAddrBytes) != 20 {
			return fmt.Errorf("contract %s: invalid address length: %d bytes, expected 20", contractAddr.Hex(), len(contractAddrBytes))
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

	for tokenAddr := range tokenContracts {
		if err := token.IndexToken(ctx, rpcClient, tx, tokenAddr, blockNum); err != nil {
			return fmt.Errorf("index token %s: %w", tokenAddr.Hex(), err)
		}
	}

	for nftAddr := range nftContracts {
		if err := token.IndexNFTs(ctx, rpcClient, tx, nftAddr, blockNum); err != nil {
			return fmt.Errorf("index NFTs for contract %s: %w", nftAddr.Hex(), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Block %d: Miner=%s, Txs=%d, Accounts=%d", blockNum, minerAddr, len(block.Transactions()), len(accounts))
	return nil
}

// Run starts the indexing process
func (i *Indexer) Run(ctx context.Context) error {
	// Check if the node is fully synced
	syncing, err := i.client.SyncProgress(ctx)
	if err != nil {
		return fmt.Errorf("failed to check sync progress: %w", err)
	}
	if syncing != nil {
		return fmt.Errorf("node is not fully synced: current block %d, highest block %d", syncing.CurrentBlock, syncing.HighestBlock)
	}
	log.Println("Node is fully synced")

	// Start indexing historical blocks
	log.Printf("Indexing historical blocks up to block number: %d", i.historical)
	for blockNum := uint64(0); blockNum <= i.historical; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Printf("Processing block %d", blockNum)

			// Index validators periodically
			if err := i.indexValidatorsPeriodically(ctx, blockNum); err != nil {
				log.Printf("Validator indexing error at block %d: %v", blockNum, err)
				continue
			}

			// Index the block
			if err := indexBlock(ctx, i.client, i.rpcClient, i.db, blockNum, i.chainID); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}
			time.Sleep(i.rateLimit)
		}
	}

	// Start listening for new blocks
	headers := make(chan *types.Header)
	sub, err := i.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new headers: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case header := <-headers:
			blockNum := header.Number.Uint64()
			log.Printf("New block received: %d", blockNum)

			// Index validators periodically
			if err := i.indexValidatorsPeriodically(ctx, blockNum); err != nil {
				log.Printf("Validator indexing error at block %d: %v", blockNum, err)
				continue
			}

			// Index the block
			if err := indexBlock(ctx, i.client, i.rpcClient, i.db, blockNum, i.chainID); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}
			i.latest = blockNum
		}
	}
}
