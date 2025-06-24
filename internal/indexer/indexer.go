package indexer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"zond-indexer/internal/config"
	dbpkg "zond-indexer/internal/db"
	"zond-indexer/internal/models"
	"zond-indexer/internal/node"
	"zond-indexer/internal/token"
	"zond-indexer/internal/utils"

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
	lastHead           struct {
		blockNumber int64
		blockHash   []byte
	}
}

type BeaconBlockResponse struct {
	Data struct {
		Message struct {
			Slot          string `json:"slot"`
			ProposerIndex string `json:"proposer_index"`
			Body          struct {
				Graffiti     string `json:"graffiti"`
				RandaoReveal string `json:"randao_reveal"`
				Eth1Data     struct {
					DepositCount string `json:"deposit_count"`
				} `json:"eth1_data"`

				ExecutionPayload struct {
					ParentHash       string `json:"parent_hash"`
					FeeRecipient     string `json:"fee_recipient"`
					StateRoot        string `json:"state_root"`
					ReceiptsRoot     string `json:"receipts_root"`
					LogsBloom        string `json:"logs_bloom"`
					PrevRandao       string `json:"prev_randao"`
					BlockNumber      string `json:"block_number"`
					GasLimit         string `json:"gas_limit"`
					GasUsed          string `json:"gas_used"`
					Timestamp        string `json:"timestamp"`
					ExtraData        string `json:"extra_data"`
					BaseFeePerGas    string `json:"base_fee_per_gas"`
					BlockHash        string `json:"block_hash"`
					TransactionsRoot string `json:"transactions_root"`
					PayloadHash      string `json:"payload_hash"`
					SlotRoot         string `json:"slot_root"`
					ParentRoot       string `json:"parent_root"`
				} `json:"execution_payload"`
			} `json:"body"`
		} `json:"message"`
	} `json:"data"`
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

	// Fetch last indexed block from DB
	var lastIndexedBlock int64
	err = db.QueryRow(context.Background(), "SELECT COALESCE(MAX(block_number), -1) FROM blocks").Scan(&lastIndexedBlock)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to fetch last indexed block: %w", err)
	}

	start := uint64(lastIndexedBlock + 1)

	return &Indexer{
		config:             config,
		client:             client,
		rpcClient:          rpcClient,
		db:                 db,
		chainID:            chainID,
		rateLimit:          config.RateLimit,
		latest:             start,
		historical:         start,
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
func indexGasPrices(ctx context.Context, _ *zondclient.Client, tx pgx.Tx, block *types.Block, blockNum uint64) error {
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
			retrieved_at, retrieved_from, reverted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (timestamp, block_number) DO UPDATE
		SET low_price = EXCLUDED.low_price,
			average_price = EXCLUDED.average_price,
			high_price = EXCLUDED.high_price,
			retrieved_at = EXCLUDED.retrieved_at,
			retrieved_from = EXCLUDED.retrieved_from,
			reverted_at = EXCLUDED.reverted_at`,
		timestamp,
		lowPrice.Int64(),
		averagePrice.Int64(),
		highPrice.Int64(),
		blockNum,
		time.Now(),
		"zond_node",
		nil, // reverted_at
	)
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

func flattenTraceCalls(txHash string, blockNumber uint64, calls []node.TraceCall, depth int) []models.InternalTransaction {
	var result []models.InternalTransaction

	for _, call := range calls {
		itx := models.InternalTransaction{
			TxHash:      txHash,
			From:        call.From,
			To:          call.To,
			Value:       call.Value,
			Input:       call.Input,
			Output:      call.Output,
			Type:        call.Type,
			Gas:         call.Gas,
			GasUsed:     call.GasUsed,
			BlockNumber: blockNumber,
			Depth:       depth,
		}
		result = append(result, itx)

		if len(call.Calls) > 0 {
			nested := flattenTraceCalls(txHash, blockNumber, call.Calls, depth+1)
			result = append(result, nested...)
		}
	}

	return result
}

// indexBlock processes a single block and indexes its data into the db.
func indexBlock(ctx context.Context, cfg config.Config, client *zondclient.Client, rpcClient *rpc.Client, db *pgxpool.Pool, blockNum uint64, chainID *big.Int, canonical bool) error {
	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", blockNum, err)
	}
	slot := block.Number().Uint64()

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction %d: %w", blockNum, err)
	}
	defer tx.Rollback(ctx)

	var baseFeePerGas *int64
	var rewardEth, burntFeesEth float64
	var baseFee *big.Int
	baseFee = block.BaseFee()
	if baseFee := block.BaseFee(); baseFee != nil {
		if baseFee.IsInt64() {
			bf := baseFee.Int64()
			baseFeePerGas = &bf
		} else {
			return fmt.Errorf("base_fee_per_gas for block %d is too large for int64: %s", blockNum, baseFee.String())
		}

		totalTip := big.NewInt(0)
		totalGasUsed := uint64(0)

		for _, tx := range block.Transactions() {
			gasUsed := tx.Gas()
			tip := new(big.Int).Sub(tx.GasPrice(), baseFee)
			if tip.Sign() < 0 {
				tip = big.NewInt(0)
			}
			totalTip.Add(totalTip, new(big.Int).Mul(tip, new(big.Int).SetUint64(gasUsed)))
			totalGasUsed += gasUsed
		}

		burnt := new(big.Int).Mul(baseFee, new(big.Int).SetUint64(totalGasUsed))

		burntFloat, _ := new(big.Float).Quo(new(big.Float).SetInt(burnt), big.NewFloat(1e18)).Float64()
		rewardFloat, _ := new(big.Float).Quo(new(big.Float).SetInt(totalTip), big.NewFloat(1e18)).Float64()
		rewardEth = rewardFloat
		burntFeesEth = burntFloat

	} else {
		return fmt.Errorf("base_fee_per_gas is nil for block %d", blockNum)
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
	beaconResp, err := fetchBeaconBlockBySlot(cfg, slot)
	if err != nil {
		return fmt.Errorf("failed to fetch beacon block: %w", err)
	}

	proposerIndex, _ := strconv.Atoi(beaconResp.Data.Message.ProposerIndex)
	graffitiBytes, err := hexStringToBytes(beaconResp.Data.Message.Body.Graffiti)
	if err != nil {
		return fmt.Errorf("graffiti decode error: %w", err)
	}
	randaoReveal := beaconResp.Data.Message.Body.RandaoReveal
	mevFeeRecipient := beaconResp.Data.Message.Body.ExecutionPayload.FeeRecipient
	mevFeeRecipientBytes, err := addressToBytes(mevFeeRecipient)
	if err != nil {
		return fmt.Errorf("invalid mev_fee_recipient address: %w", err)
	}
	mevRewardEth := rewardEth
	mevTxHash := findMEVTxHashFromTransactions(baseFee, block.Transactions())
	mevTxHashBytes := mevTxHash
	slotRoot := hexToBytes(beaconResp.Data.Message.Body.ExecutionPayload.SlotRoot)
	parentRoot := hexToBytes(beaconResp.Data.Message.Body.ExecutionPayload.ParentRoot)
	beaconDepositCount := beaconResp.Data.Message.Body.Eth1Data.DepositCount
	epoch := int64(slot / 32)

	_, err = tx.Exec(ctx,
		`INSERT INTO Blocks (
		block_number, block_hash, timestamp, miner_address, canonical, parent_hash,
		gas_used, gas_limit, size, transaction_count, extra_data,
		base_fee_per_gas, transactions_root, state_root, receipts_root,
		logs_bloom, chain_id, retrieved_from,
		slot, epoch, proposer_index, graffiti, randao_reveal,
		beacon_deposit_count, slot_root, parent_root,
		mev_fee_recipient, mev_reward_eth, mev_tx_hash,
		reward_eth, burnt_fees_eth
	) VALUES (
		$1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10, $11, $12,
		$13, $14, $15, $16, $17, $18,
		$19, $20, $21, $22, $23,
		$24, $25, $26,
		$27, $28, $29,
		$30, $31
	)
	ON CONFLICT (block_number) DO UPDATE SET
		block_hash = EXCLUDED.block_hash,
		timestamp = EXCLUDED.timestamp,
		canonical = EXCLUDED.canonical,
		parent_hash = EXCLUDED.parent_hash,
		gas_used = EXCLUDED.gas_used,
		gas_limit = EXCLUDED.gas_limit,
		size = EXCLUDED.size,
		transaction_count = EXCLUDED.transaction_count,
		extra_data = EXCLUDED.extra_data,
		base_fee_per_gas = EXCLUDED.base_fee_per_gas,
		transactions_root = EXCLUDED.transactions_root,
		state_root = EXCLUDED.state_root,
		receipts_root = EXCLUDED.receipts_root,
		logs_bloom = EXCLUDED.logs_bloom,
		chain_id = EXCLUDED.chain_id,
		retrieved_from = EXCLUDED.retrieved_from,
		slot = EXCLUDED.slot,
		epoch = EXCLUDED.epoch,
		proposer_index = EXCLUDED.proposer_index,
		graffiti = EXCLUDED.graffiti,
		randao_reveal = EXCLUDED.randao_reveal,
		beacon_deposit_count = EXCLUDED.beacon_deposit_count,
		slot_root = EXCLUDED.slot_root,
		parent_root = EXCLUDED.parent_root,
		mev_fee_recipient = EXCLUDED.mev_fee_recipient,
		mev_reward_eth = EXCLUDED.mev_reward_eth,
		mev_tx_hash = EXCLUDED.mev_tx_hash,
		reward_eth = EXCLUDED.reward_eth,
		burnt_fees_eth = EXCLUDED.burnt_fees_eth`,
		block.Number().Int64(),
		blockHashBytes,
		time.Unix(int64(block.Time()), 0),
		minerAddrBytes,
		canonical,
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
		"zond_node",
		slot,
		epoch,
		proposerIndex,
		graffitiBytes,
		randaoReveal,
		beaconDepositCount,
		slotRoot,
		parentRoot,
		mevFeeRecipientBytes,
		mevRewardEth,
		mevTxHashBytes,
		rewardEth,
		burntFeesEth,
	)
	if err != nil {
		return fmt.Errorf("insert block %d: %w", blockNum, err)
	}

	accounts := make(map[string]bool)
	contracts := make(map[common.Address]bool)
	tokenContracts := make(map[common.Address]bool)
	nftContracts := make(map[common.Address]bool)

	minerAddr := block.Coinbase().Hex()
	if utils.IsValidHexAddress(minerAddr) {
		accounts[minerAddr] = true
	} else {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddr)
	}

	if err := indexGasPrices(ctx, client, tx, block, blockNum); err != nil {
		return fmt.Errorf("index gas prices for block %d: %w", blockNum, err)
	}

	if err := node.IndexZondNodes(ctx, client, tx); err != nil {
		log.Printf("Failed to index ZondNodes for block %d: %v", blockNum, err)
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
                is_successful, retrieved_from, is_canonical, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (tx_hash) DO UPDATE
            SET block_number = EXCLUDED.block_number, from_address = EXCLUDED.from_address,
                to_address = EXCLUDED.to_address, value = EXCLUDED.value, gas = EXCLUDED.gas,
                gas_price = EXCLUDED.gas_price, type = EXCLUDED.type, chain_id = EXCLUDED.chain_id,
                access_list = EXCLUDED.access_list, max_fee_per_gas = EXCLUDED.max_fee_per_gas,
                max_priority_fee_per_gas = EXCLUDED.max_priority_fee_per_gas,
                transaction_index = EXCLUDED.transaction_index, cumulative_gas_used = EXCLUDED.cumulative_gas_used,
                is_successful = EXCLUDED.is_successful, retrieved_from = EXCLUDED.retrieved_from,
                is_canonical = EXCLUDED.is_canonical, timestamp = EXCLUDED.timestamp`,
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
			"zond_node",
			canonical,
			block.Time())
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

		if err := token.IndexTokenTransactionsAndNFTs(ctx, rpcClient, tx, transaction, receipt, blockNum, canonical); err != nil {
			return fmt.Errorf("index token transactions and NFTs for tx %s: %w", transaction.Hash().Hex(), err)
		}
		trace, err := node.TraceTransaction(ctx, rpcClient, transaction.Hash().Hex())
		if err != nil {
			log.Printf("Block %d: Failed to trace internal txs for %s: %v", blockNum, transaction.Hash().Hex(), err)
		} else {
			var internalTxs []models.InternalTransaction

			if len(trace.Calls) > 0 {
				internalTxs = flattenTraceCalls(transaction.Hash().Hex(), blockNum, trace.Calls, 0)
			} else if len(trace.StructLogs) > 0 {
				internalTxs = node.ConvertStructLogsToInternalTxs(transaction.Hash().Hex(), blockNum, trace.StructLogs)
			}

			log.Printf("InsertInternalTransactions called with %d txs", len(internalTxs))
			if len(internalTxs) == 0 {
				log.Println("No internal transactions to insert")
			} else {
				if err := dbpkg.InsertInternalTransactions(ctx, tx, internalTxs, canonical); err != nil {
					return fmt.Errorf("insert internal txs for %s: %w", transaction.Hash().Hex(), err)
				}
			}
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
			ON CONFLICT (address) DO UPDATE
			SET contract_name = EXCLUDED.contract_name, compiler_version = EXCLUDED.compiler_version,
				abi = EXCLUDED.abi, source_code = EXCLUDED.source_code,
				optimization_enabled = EXCLUDED.optimization_enabled, runs = EXCLUDED.runs,
				constructor_arguments = EXCLUDED.constructor_arguments, verified_date = EXCLUDED.verified_date,
				license = EXCLUDED.license, is_canonical = EXCLUDED.is_canonical,
				retrieved_at = EXCLUDED.retrieved_at, retrieved_from = EXCLUDED.retrieved_from`,
			contractAddrBytes,
			"Unknown",
			"Unknown",
			"[]",
			"",
			false,
			0,
			"",
			nil,         // verified_date
			"",          // license
			canonical,   // is_canonical
			time.Now(),  // retrieved_at
			"zond_node") // retrieved_from
		if err != nil {
			return fmt.Errorf("insert contract %s: %w", contractAddr.Hex(), err)
		}
	}

	for tokenAddr := range tokenContracts {
		ok, tokenType, err := token.DetectTokenContract(ctx, client.Client(), tokenAddr)
		if err != nil {
			log.Printf("failed to detect token %s: %v", tokenAddr.Hex(), err)
			continue
		}
		if !ok {
			continue
		}

		err = token.IndexToken(ctx, client.Client(), tx, tokenAddr, blockNum, true, tokenType)
		if err != nil {
			return fmt.Errorf("index token %s: %w", tokenAddr.Hex(), err)
		}
	}

	for nftAddr := range nftContracts {
		if err := token.IndexNFTs(ctx, rpcClient, tx, nftAddr, blockNum, canonical); err != nil {
			return fmt.Errorf("index NFTs for contract %s: %w", nftAddr.Hex(), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Block %d: Miner=%s, Txs=%d, Accounts=%d, Canonical=%t", blockNum, minerAddr, len(block.Transactions()), len(accounts), canonical)
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

			cfg, err := config.LoadConfig()

			// Index the block as canonical initially
			if err := indexBlock(ctx, cfg, i.client, i.rpcClient, i.db, blockNum, i.chainID, true); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}

			// Update last head
			block, err := i.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
			if err != nil {
				log.Printf("Failed to fetch block %d for head tracking: %v", blockNum, err)
				continue
			}
			i.lastHead.blockNumber = block.Number().Int64()
			i.lastHead.blockHash = block.Hash().Bytes()

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

			// Fetch the full block to check for reorgs
			block, err := i.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
			if err != nil {
				log.Printf("Failed to fetch block %d: %v", blockNum, err)
				continue
			}

			// Check for reorg
			parentHash := block.ParentHash().Bytes()
			if i.lastHead.blockNumber > 0 && i.lastHead.blockNumber == int64(blockNum)-1 {
				if !bytesEqual(parentHash, i.lastHead.blockHash) {
					log.Printf("Reorg detected at block %d", blockNum)
					if err := i.handleReorg(ctx, block); err != nil {
						log.Printf("Failed to handle reorg at block %d: %v", blockNum, err)
						continue
					}
				}
			}

			// Index validators periodically
			if err := i.indexValidatorsPeriodically(ctx, blockNum); err != nil {
				log.Printf("Validator indexing error at block %d: %v", blockNum, err)
				continue
			}
			cfg, err := config.LoadConfig()
			// Index the block as canonical
			if err := indexBlock(ctx, cfg, i.client, i.rpcClient, i.db, blockNum, i.chainID, true); err != nil {
				log.Printf("Indexing error: insert block %d: %v", blockNum, err)
				continue
			}

			// Update last head
			i.lastHead.blockNumber = block.Number().Int64()
			i.lastHead.blockHash = block.Hash().Bytes()
			i.latest = blockNum
		}
	}
}

func (i *Indexer) handleReorg(ctx context.Context, newHead *types.Block) error {
	// Traverse backwards from the new head to find the common ancestor
	newChain := make(map[int64][]byte)
	currentBlock := newHead
	for currentBlock != nil {
		newChain[currentBlock.Number().Int64()] = currentBlock.Hash().Bytes()
		parent, err := i.client.BlockByHash(ctx, currentBlock.ParentHash())
		if err != nil {
			return fmt.Errorf("fetch parent block: %w", err)
		}
		currentBlock = parent
		// Stop at a reasonable depth (e.g., 10 blocks) to avoid excessive traversal
		if len(newChain) > 10 {
			break
		}
	}

	// Fetch the old chain from the database
	oldChain := make(map[int64][]byte)
	rows, err := i.db.Query(ctx, `
        SELECT block_number, block_hash
        FROM Blocks
        WHERE block_number >= $1 AND block_number <= $2 AND canonical = TRUE
    `, newHead.Number().Int64()-int64(len(newChain)), newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("fetch old chain: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var blockNumber int64
		var blockHash []byte
		if err := rows.Scan(&blockNumber, &blockHash); err != nil {
			return fmt.Errorf("scan old chain block: %w", err)
		}
		oldChain[blockNumber] = blockHash
	}

	// Find the fork point
	forkPoint := newHead.Number().Int64()
	for blockNumber := newHead.Number().Int64(); blockNumber >= newHead.Number().Int64()-int64(len(newChain)); blockNumber-- {
		newHash, inNew := newChain[blockNumber]
		oldHash, inOld := oldChain[blockNumber]
		if inNew && inOld && bytesEqual(newHash, oldHash) {
			forkPoint = blockNumber
			break
		}
	}

	// Mark blocks in the old chain as non-canonical
	tx, err := i.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction for reorg: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
        UPDATE Blocks
        SET canonical = FALSE
        WHERE block_number > $1 AND block_number <= $2 AND canonical = TRUE
    `, forkPoint, newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("mark old chain as non-canonical: %w", err)
	}

	// Mark related data as non-canonical
	_, err = tx.Exec(ctx, `
        UPDATE Transactions
        SET is_canonical = FALSE
        WHERE block_number > $1 AND block_number <= $2 AND is_canonical = TRUE
    `, forkPoint, newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("mark transactions as non-canonical: %w", err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE TokenTransactions
        SET is_canonical = FALSE
        WHERE block_number > $1 AND block_number <= $2 AND is_canonical = TRUE
    `, forkPoint, newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("mark token transactions as non-canonical: %w", err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE NFTs
        SET is_canonical = FALSE
        WHERE block_number > $1 AND block_number <= $2 AND is_canonical = TRUE
    `, forkPoint, newHead.Number().Int64())
	if err != nil {
		return fmt.Errorf("mark NFTs as non-canonical: %w", err)
	}

	// Reindex the new chain
	for blockNumber := forkPoint + 1; blockNumber <= newHead.Number().Int64(); blockNumber++ {
		if _, ok := newChain[blockNumber]; !ok {
			continue // Skip if we don't have the block in the new chain
		}
		cfg, _ := config.LoadConfig()
		if err := indexBlock(ctx, cfg, i.client, i.rpcClient, i.db, uint64(blockNumber), i.chainID, true); err != nil {
			return fmt.Errorf("reindex block %d: %w", blockNumber, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reorg transaction: %w", err)
	}

	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fetchBeaconBlockBySlot(cfg config.Config, slot uint64) (*BeaconBlockResponse, error) {
	url := fmt.Sprintf("%s/zond/v1/beacon/blocks/%d", cfg.BeaconURL(), slot)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to GET beacon block: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read beacon block response: %w", err)
	}

	log.Println("BEACON RAW RESPONSE", string(body))

	var beaconResp BeaconBlockResponse
	if err := json.Unmarshal(body, &beaconResp); err != nil {
		return nil, fmt.Errorf("failed to decode beacon response: %w", err)
	}

	return &beaconResp, nil
}

func findMEVTxHashFromTransactions(baseFee *big.Int, txs types.Transactions) []byte {
	var maxTip = big.NewInt(0)
	var mevTxHash []byte

	for _, tx := range txs {
		tip := new(big.Int).Sub(tx.GasPrice(), baseFee)
		if tip.Sign() < 0 {
			tip = big.NewInt(0)
		}
		tipAmount := new(big.Int).Mul(tip, new(big.Int).SetUint64(tx.Gas()))
		if tipAmount.Cmp(maxTip) > 0 {
			maxTip = tipAmount
			mevTxHash = tx.Hash().Bytes()
		}
	}
	return mevTxHash
}

func hexToBytes(str string) []byte {
	str = strings.TrimPrefix(str, "0x")
	bytes, _ := hex.DecodeString(str)
	return bytes
}

func addressToBytes(address string) ([]byte, error) {
	clean := strings.TrimPrefix(address, "0x")
	return hex.DecodeString(clean)
}

func hexStringToBytes(hexStr string) ([]byte, error) {
	cleaned := strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(cleaned)
}
