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
	"github.com/theQRL/go-zond/rlp"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

type accountUpdateData struct {
	address     string
	txHash      []byte
	isSender    bool
	isReceiver  bool
	fromAddress []byte
}

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
	baseFee := block.BaseFee()
	if baseFee := block.BaseFee(); baseFee != nil {
		if baseFee.IsInt64() {
			bf := baseFee.Int64()
			baseFeePerGas = &bf
		} else {
			log.Printf("Warning: base_fee_per_gas for block %d is too large for int64: %s. Storing as nil.", blockNum, baseFee.String())
			baseFeePerGas = nil
		}

		totalGasUsed := uint64(0)
		for _, tx := range block.Transactions() {
			totalGasUsed += tx.Gas()
		}
		burnt := new(big.Int).Mul(baseFee, new(big.Int).SetUint64(totalGasUsed))
		burntFloat, _ := new(big.Float).Quo(new(big.Float).SetInt(burnt), new(big.Float).SetInt64(1e18)).Float64()
		burntFeesEth = burntFloat

		totalTip := big.NewInt(0)

		for _, tx := range block.Transactions() {
			gasUsed := tx.Gas()
			tip := new(big.Int).Sub(tx.GasPrice(), baseFee)
			if tip.Sign() < 0 {
				tip = big.NewInt(0)
			}
			totalTip.Add(totalTip, new(big.Int).Mul(tip, new(big.Int).SetUint64(gasUsed)))
			totalGasUsed += gasUsed
		}

		rewardFloat, _ := new(big.Float).Quo(new(big.Float).SetInt(totalTip), new(big.Float).SetInt64(1e18)).Float64()
		rewardEth = rewardFloat
	} else {
		log.Printf("Block %d: base_fee_per_gas is nil (pre-London block). Defaulting values to 0.", blockNum)
		bf := int64(0)
		baseFeePerGas = &bf
		burntFeesEth = 0
		rewardEth = 0
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
	beaconResp, err := utils.FetchBeaconBlockBySlot(cfg, slot)
	if err != nil {
		return fmt.Errorf("failed to fetch beacon block: %w", err)
	}

	proposerIndex, _ := strconv.Atoi(beaconResp.Data.Message.ProposerIndex)
	graffitiBytes, err := utils.HexStringToBytes(beaconResp.Data.Message.Body.Graffiti)
	if err != nil {
		return fmt.Errorf("graffiti decode error: %w", err)
	}
	randaoReveal := beaconResp.Data.Message.Body.RandaoReveal
	mevFeeRecipient := beaconResp.Data.Message.Body.ExecutionPayload.FeeRecipient
	mevFeeRecipientBytes, err := utils.AddressToBytes(mevFeeRecipient)
	if err != nil {
		return fmt.Errorf("invalid mev_fee_recipient address: %w", err)
	}
	mevRewardEth := rewardEth
	mevTxHash := utils.FindMEVTxHashFromTransactions(baseFee, block.Transactions())
	mevTxHashBytes := mevTxHash
	slotRoot := utils.HexToBytes(beaconResp.Data.Message.StateRoot)
	parentRoot := utils.HexToBytes(beaconResp.Data.Message.ParentRoot)
	epoch := int64(slot / 32)

	var beaconDepositCountInt64 int64
	if beaconResp.Data.Message.Body.Eth1Data.DepositCount != "" {
		parsedVal, err := strconv.ParseInt(beaconResp.Data.Message.Body.Eth1Data.DepositCount, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing beacon_deposit_count '%s': %w", beaconResp.Data.Message.Body.Eth1Data.DepositCount, err)
		}
		beaconDepositCountInt64 = parsedVal
	}

	const insertBlockSQL = `
	INSERT INTO Blocks (
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
		burnt_fees_eth = EXCLUDED.burnt_fees_eth
	`

	args := []interface{}{
		block.Number().Int64(), blockHashBytes, time.Unix(int64(block.Time()), 0), minerAddrBytes, canonical, parentHashBytes,
		gasUsedStr, gasLimitStr, int(block.Size()), len(block.Transactions()), extraDataBytes,
		baseFeePerGas, txRootBytes, stateRootBytes, receiptsRootBytes,
		logsBloomBytes, chainID.Int64(), "zond_node",
		slot, epoch, proposerIndex, graffitiBytes, randaoReveal,
		beaconDepositCountInt64, slotRoot, parentRoot,
		mevFeeRecipientBytes, mevRewardEth, mevTxHashBytes,
		rewardEth, burntFeesEth,
	}

	_, err = tx.Exec(ctx, insertBlockSQL, args...)
	if err != nil {
		return fmt.Errorf("insert block %d: %w", block.Number().Int64(), err)
	}

	if beaconResp != nil {
		withdrawals := beaconResp.Data.Message.Body.ExecutionPayload.Withdrawals
		if len(withdrawals) > 0 {
			dbWithdrawals := make([]dbpkg.Withdrawal, len(withdrawals))
			for i, w := range withdrawals {
				dbWithdrawals[i] = dbpkg.Withdrawal{
					Index:          w.Index,
					ValidatorIndex: w.ValidatorIndex,
					Address:        w.Address,
					Amount:         w.Amount,
				}
			}
			if err := dbpkg.InsertWithdrawals(ctx, tx, blockNum, dbWithdrawals); err != nil {
				return fmt.Errorf("insert withdrawals for block %d: %w", blockNum, err)
			}
		}

		// --- Process Beacon Deposits ---
		// Use the correct path to the deposits array
		deposits := beaconResp.Data.Message.Body.Deposits
		if len(deposits) > 0 {
			dbDeposits := make([]dbpkg.BeaconDeposit, len(deposits))
			for i, d := range deposits {
				timestampUnix, err := strconv.ParseInt(d.Timestamp, 10, 64)
				if err != nil {
					log.Printf("Could not parse timestamp for deposit: %v", err)
					continue
				}
				parsedTimestamp := time.Unix(timestampUnix, 0)
				dbDeposits[i] = dbpkg.BeaconDeposit{
					Index:          d.Index,
					ValidatorIndex: d.ValidatorIndex,
					FromAddress:    d.FromAddress,
					Amount:         d.Amount,
					Timestamp:      parsedTimestamp,
					TxHash:         d.TxHash,
					PubKey:         d.PubKey,
					Signature:      d.Signature,
				}
			}
			if err := dbpkg.InsertBeaconDeposits(ctx, tx, blockNum, dbDeposits); err != nil {
				return fmt.Errorf("insert beacon deposits for block %d: %w", blockNum, err)
			}
		}
	}

	accountsToUpdate := make(map[string]accountUpdateData)
	contracts := make(map[common.Address]bool)
	tokenContracts := make(map[common.Address]bool)
	nftContracts := make(map[common.Address]bool)

	minerAddr := block.Coinbase().Hex()
	if utils.IsValidHexAddress(minerAddr) {
		accountsToUpdate[minerAddr] = accountUpdateData{address: minerAddr, isReceiver: true}
	} else {
		return fmt.Errorf("block %d: invalid miner address format: %s", blockNum, minerAddr)
	}

	if err := indexGasPrices(ctx, client, tx, block, blockNum); err != nil {
		return fmt.Errorf("index gas prices for block %d: %w", blockNum, err)
	}

	if err := node.IndexZondNodes(ctx, client, tx); err != nil {
		log.Printf("Failed to index ZondNodes for block %d: %v", blockNum, err)
	}

	if err := insertTransactions(ctx, client, rpcClient, tx, block, blockNum, canonical, accountsToUpdate, contracts, tokenContracts, nftContracts); err != nil {
		return fmt.Errorf("insert transactions for block %d: %w", blockNum, err)
	}

	timestamp := time.Unix(int64(block.Time()), 0)
	if err := insertAccounts(ctx, client, tx, accountsToUpdate, timestamp); err != nil {
		return fmt.Errorf("insert accounts: %w", err)
	}

	if err := insertContracts(ctx, tx, contracts, canonical); err != nil {
		return fmt.Errorf("insert contracts: %w", err)
	}

	if err := indexTokens(ctx, client, tx, tokenContracts, blockNum); err != nil {
		return fmt.Errorf("index tokens: %w", err)
	}

	if err := indexNFTContracts(ctx, rpcClient, tx, nftContracts, blockNum, canonical); err != nil {
		return fmt.Errorf("index NFT contracts: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Block %d: Miner=%s, Txs=%d, Accounts=%d, Canonical=%t", blockNum, minerAddr, len(block.Transactions()), len(accountsToUpdate), canonical)
	return nil
}

func insertTransactions(
	ctx context.Context,
	client *zondclient.Client,
	rpcClient *rpc.Client,
	tx pgx.Tx,
	block *types.Block,
	blockNum uint64,
	canonical bool,
	accountsToUpdate map[string]accountUpdateData,
	contracts map[common.Address]bool,
	tokenContracts map[common.Address]bool,
	nftContracts map[common.Address]bool,
) error {
	for _, transaction := range block.Transactions() {
		receipt, err := client.TransactionReceipt(ctx, transaction.Hash())
		if err != nil {
			return fmt.Errorf("fetch receipt for tx %s: %w", transaction.Hash().Hex(), err)
		}

		rawTxBytes, err := rlp.EncodeToBytes(transaction)
		if err != nil {
			return fmt.Errorf("failed to rlp encode tx %s: %w", transaction.Hash().Hex(), err)
		}
		rawTxHex := "0x" + hex.EncodeToString(rawTxBytes)

		var logsJSON json.RawMessage
		if receipt.Logs != nil && len(receipt.Logs) > 0 {
			logsJSON, err = json.Marshal(receipt.Logs)
			if err != nil {
				return fmt.Errorf("failed to marshal logs for tx %s: %w", transaction.Hash().Hex(), err)
			}
		} else {
			logsJSON = json.RawMessage("[]")
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

		isContract := false
		if to := transaction.To(); to != nil {
			if contracts[*to] {
				isContract = true
			}
		}

		inputData := transaction.Data()
		method := ""
		if len(inputData) >= 4 {
			method = "0x" + hex.EncodeToString(inputData[:4])
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO Transactions (
                tx_hash, block_number, from_address, to_address, value, gas,
                gas_price, gas_used, type, chain_id, access_list, max_fee_per_gas,
                max_priority_fee_per_gas, transaction_index, cumulative_gas_used,
                is_successful, retrieved_from, is_canonical, timestamp, is_contract, method, logs, raw_tx_hex
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (tx_hash) DO UPDATE
            SET block_number = EXCLUDED.block_number, from_address = EXCLUDED.from_address,
                to_address = EXCLUDED.to_address, value = EXCLUDED.value, gas = EXCLUDED.gas,
                gas_price = EXCLUDED.gas_price, gas_used = EXCLUDED.gas_used, type = EXCLUDED.type, chain_id = EXCLUDED.chain_id,
                access_list = EXCLUDED.access_list, max_fee_per_gas = EXCLUDED.max_fee_per_gas,
                max_priority_fee_per_gas = EXCLUDED.max_priority_fee_per_gas,
                transaction_index = EXCLUDED.transaction_index, cumulative_gas_used = EXCLUDED.cumulative_gas_used,
                is_successful = EXCLUDED.is_successful, retrieved_from = EXCLUDED.retrieved_from,
                is_canonical = EXCLUDED.is_canonical, timestamp = EXCLUDED.timestamp, is_contract = EXCLUDED.is_contract, method = EXCLUDED.method, logs = EXCLUDED.logs, raw_tx_hex = EXCLUDED.raw_tx_hex`,
			transaction.Hash().Bytes(),
			block.Number().Int64(),
			fromAddrBytes,
			toAddress,
			transaction.Value().String(),
			int64(transaction.Gas()),
			transaction.GasPrice().String(),
			int64(receipt.GasUsed),
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
			time.Unix(int64(block.Time()), 0),
			isContract,
			method,
			logsJSON,
			rawTxHex,
		)
		if err != nil {
			return fmt.Errorf("insert transaction %s: %w", transaction.Hash().Hex(), err)
		}

		log.Printf("[Block %d] Transaction %s confirmed. Removing from pending table.", blockNum, transaction.Hash().Hex())
		// We use the main database transaction `tx` that the block processor is using.
		if err := dbpkg.DeletePendingTransactionByHash(ctx, tx, transaction.Hash().Bytes()); err != nil {
			// This is not a critical error, so we just log it and continue.
			log.Printf("⚠️ [Block %d] Failed to delete pending tx %s: %v", blockNum, transaction.Hash().Hex(), err)
		}

		fromAddr := from.Hex()
		txHashBytes := transaction.Hash().Bytes()
		if utils.IsValidHexAddress(fromAddr) {
			accountsToUpdate[fromAddr] = accountUpdateData{
				address:     fromAddr,
				txHash:      txHashBytes,
				isSender:    true,
				fromAddress: fromAddrBytes,
			}
		} else {
			return fmt.Errorf("block %d: invalid from address format: %s", blockNum, fromAddr)
		}

		if toAddrStr != "" {
			if utils.IsValidHexAddress(toAddrStr) {
				data := accountsToUpdate[toAddrStr]
				data.address = toAddrStr
				data.txHash = txHashBytes
				data.isReceiver = true
				data.fromAddress = fromAddrBytes
				accountsToUpdate[toAddrStr] = data
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
				accountsToUpdate[contractAddrStr] = accountUpdateData{
					address:     contractAddrStr,
					txHash:      txHashBytes,
					isReceiver:  true,
					fromAddress: fromAddrBytes,
				}
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
			continue
		}

		var internalTxs []models.InternalTransaction
		if len(trace.Calls) > 0 {
			internalTxs = utils.FlattenTraceCalls(transaction.Hash().Hex(), blockNum, trace.Calls, 0)
		} else if len(trace.StructLogs) > 0 {
			internalTxs = node.ConvertStructLogsToInternalTxs(transaction.Hash().Hex(), blockNum, trace.StructLogs)
		}

		if len(internalTxs) > 0 {
			if err := dbpkg.InsertInternalTransactions(ctx, tx, internalTxs, canonical); err != nil {
				return fmt.Errorf("insert internal txs for %s: %w", transaction.Hash().Hex(), err)
			}
		}
	}
	return nil
}

func insertAccounts(
	ctx context.Context,
	client *zondclient.Client,
	tx pgx.Tx,
	accountsToUpdate map[string]accountUpdateData,
	timestamp time.Time,
) error {
	var qrlPrice float64
	err := tx.QueryRow(ctx, `SELECT price_usd FROM cmc_data WHERE symbol = 'QRL' ORDER BY retrieved_at DESC LIMIT 1`).Scan(&qrlPrice)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("fetch qrl price: %w", err)
	}

	for address, data := range accountsToUpdate {
		addr, err := utils.HexToAddress(address)
		if err != nil {
			return fmt.Errorf("convert address %s: %w", address, err)
		}

		addrBytes := addr.Bytes()
		if len(addrBytes) != 20 {
			return fmt.Errorf("account %s: invalid address length: %d bytes", address, len(addrBytes))
		}

		balance, err := client.BalanceAt(ctx, addr, nil)
		if err != nil {
			return fmt.Errorf("fetch balance for %s: %w", address, err)
		}
		nonce, err := client.NonceAt(ctx, addr, nil)
		if err != nil {
			return fmt.Errorf("fetch nonce for %s: %w", address, err)
		}
		code, err := client.CodeAt(ctx, addr, nil)
		if err != nil {
			return fmt.Errorf("fetch code for %s: %w", address, err)
		}

		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM Accounts WHERE address = $1)`, addrBytes).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check existing account %s: %w", address, err)
		}

		balanceStr := balance.String()
		nonceInt := int(nonce)

		if !exists {
			funderAddress := []byte(nil)
			fundingTxHash := []byte(nil)
			firstTxSentTimestamp := (*time.Time)(nil)

			if data.isReceiver {
				funderAddress = data.fromAddress
				fundingTxHash = data.txHash
			}
			if data.isSender {
				firstTxSentTimestamp = &timestamp
			}

			_, err = tx.Exec(ctx,
				`INSERT INTO Accounts (
                    address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from,
                    funder_address, funding_tx_hash, first_tx_sent_timestamp, last_tx_sent_timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				addrBytes,
				balanceStr,
				nonceInt,
				len(code) > 0,
				hex.EncodeToString(code),
				timestamp,
				timestamp,
				"zond_node",
				funderAddress,
				fundingTxHash,
				firstTxSentTimestamp,
				&timestamp,
			)
			if err != nil {
				return fmt.Errorf("insert new account %s: %w", address, err)
			}
		} else {
			var updateQuery string
			if data.isSender {
				updateQuery = `
                    UPDATE Accounts SET
                        balance = $1,
                        nonce = $2,
                        last_seen = $3,
                        last_tx_sent_timestamp = $3,
                        first_tx_sent_timestamp = COALESCE(first_tx_sent_timestamp, $3)
                    WHERE address = $4`
			} else {
				updateQuery = `
                    UPDATE Accounts SET
                        balance = $1,
                        nonce = $2,
                        last_seen = $3
                    WHERE address = $4`
			}

			_, err = tx.Exec(ctx, updateQuery, balanceStr, nonceInt, timestamp, addrBytes)
			if err != nil {
				return fmt.Errorf("update existing account %s: %w", address, err)
			}
		}

		var balanceUsd float64
		if qrlPrice > 0 {
			balanceBigInt, _ := new(big.Int).SetString(balanceStr, 10)
			if balanceBigInt != nil {
				balanceFloat := new(big.Float).SetInt(balanceBigInt)
				divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
				realBalance := new(big.Float).Quo(balanceFloat, divisor)
				value := new(big.Float).Mul(realBalance, big.NewFloat(qrlPrice))
				balanceUsd, _ = value.Float64()
			}
		}

		_, err = tx.Exec(ctx, `
            INSERT INTO address_daily_balances (address, date, balance_planck, balance_usd)
            VALUES ($1, $2::date, $3, $4)
            ON CONFLICT (address, date) DO UPDATE SET
                balance_planck = EXCLUDED.balance_planck,
                balance_usd = EXCLUDED.balance_usd
        `, addrBytes, timestamp, balanceStr, balanceUsd)
		if err != nil {
			return fmt.Errorf("upsert daily balance for %s: %w", address, err)
		}
	}
	return nil
}

func insertContracts(
	ctx context.Context,
	tx pgx.Tx,
	contracts map[common.Address]bool,
	canonical bool,
) error {
	for contractAddr := range contracts {
		contractAddrBytes := contractAddr.Bytes()
		if len(contractAddrBytes) != 20 {
			return fmt.Errorf("contract %s: invalid address length", contractAddr.Hex())
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO Contracts (
				address, contract_name, compiler_version, abi, source_code,
				optimization_enabled, runs, constructor_arguments, verified_date,
				license, is_canonical, retrieved_at, retrieved_from
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (address) DO UPDATE
			SET contract_name = EXCLUDED.contract_name,
				compiler_version = EXCLUDED.compiler_version,
				abi = EXCLUDED.abi,
				source_code = EXCLUDED.source_code,
				optimization_enabled = EXCLUDED.optimization_enabled,
				runs = EXCLUDED.runs,
				constructor_arguments = EXCLUDED.constructor_arguments,
				verified_date = EXCLUDED.verified_date,
				license = EXCLUDED.license,
				is_canonical = EXCLUDED.is_canonical,
				retrieved_at = EXCLUDED.retrieved_at,
				retrieved_from = EXCLUDED.retrieved_from`,
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
			canonical,
			time.Now(),
			"zond_node")
		if err != nil {
			return fmt.Errorf("insert contract %s: %w", contractAddr.Hex(), err)
		}
	}
	return nil
}

func indexTokens(
	ctx context.Context,
	client *zondclient.Client,
	tx pgx.Tx,
	tokenContracts map[common.Address]bool,
	blockNum uint64,
) error {
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
	return nil
}

func indexNFTContracts(
	ctx context.Context,
	rpcClient *rpc.Client,
	tx pgx.Tx,
	nftContracts map[common.Address]bool,
	blockNum uint64,
	canonical bool,
) error {
	for nftAddr := range nftContracts {
		if err := token.IndexNFTs(ctx, rpcClient, tx, nftAddr, blockNum, canonical); err != nil {
			return fmt.Errorf("index NFTs for contract %s: %w", nftAddr.Hex(), err)
		}
	}
	return nil
}
