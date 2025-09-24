package token

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	"zond-indexer/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
)

// IndexToken indexes a token contract based on its type
func IndexToken(ctx context.Context, client *rpc.Client, tx pgx.Tx, addr common.Address, blockNum uint64, canonical bool, tokenType string) error {
	// --- THE FINAL, DEFINITIVE FIX ---
	// Before we can insert into the `tokens` table, we MUST ensure the contract's address
	// exists in the `accounts` table to satisfy the foreign key constraint.
	_, err := tx.Exec(ctx,
		`INSERT INTO accounts (address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from)
		 VALUES ($1, '0', 0, true, '', NOW(), NOW(), 'zond-indexer-placeholder')
		 ON CONFLICT (address) DO NOTHING`,
		addr.Bytes())
	if err != nil {
		return fmt.Errorf("ensure token contract account %s exists: %w", addr.Hex(), err)
	}
	// --- END OF FIX ---

	// Now that the account is guaranteed to exist, we can proceed with fetching token details.
	nameBytes, err := callContractRaw(ctx, client, addr, "name()")
	if err != nil {
		log.Printf("Could not fetch name for token %s: %v", addr.Hex(), err)
		nameBytes = []byte{}
	}
	nameStr := utils.SanitizeString(string(nameBytes))

	symbolBytes, err := callContractRaw(ctx, client, addr, "symbol()")
	if err != nil {
		log.Printf("Could not fetch symbol for token %s: %v", addr.Hex(), err)
		symbolBytes = []byte{}
	}
	symbolStr := utils.SanitizeString(string(symbolBytes))

	decimals := 0
	if tokenType == "ERC20" {
		decBytes, err := callContractRaw(ctx, client, addr, "decimals()")
		if err == nil && len(decBytes) > 0 {
			decBig := new(big.Int).SetBytes(decBytes)
			if decBig.IsInt64() {
				decimals = int(decBig.Int64())
			}
		}
	}

	totalSupply := "0"
	if tokenType == "ERC20" {
		supplyBytes, err := callContractRaw(ctx, client, addr, "totalSupply()")
		if err == nil && len(supplyBytes) > 0 {
			supplyBig := new(big.Int).SetBytes(supplyBytes)
			totalSupply = supplyBig.String()
		}
	}

	// This INSERT will now succeed because the foreign key dependency has been met.
	_, err = tx.Exec(ctx,
		`INSERT INTO Tokens (
			contract_address, token_name, token_symbol, total_supply, decimals, token_type,
			website, logo, is_canonical, retrieved_at, retrieved_from, reverted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (contract_address) DO UPDATE
		SET token_name = EXCLUDED.token_name, token_symbol = EXCLUDED.token_symbol,
			total_supply = EXCLUDED.total_supply, decimals = EXCLUDED.decimals,
			token_type = EXCLUDED.token_type, website = EXCLUDED.website,
			logo = EXCLUDED.logo, is_canonical = EXCLUDED.is_canonical,
			retrieved_at = EXCLUDED.retrieved_at, retrieved_from = EXCLUDED.retrieved_from,
			reverted_at = EXCLUDED.reverted_at`,
		addr.Bytes(), nameStr, symbolStr, totalSupply, decimals, tokenType, "", "", canonical, time.Now(), "zond_node", nil,
	)
	if err != nil {
		return fmt.Errorf("insert token %s (name: %q, symbol: %q): %w", addr.Hex(), nameStr, symbolStr, err)
	}

	return nil
}

// IndexTokenTransactionsAndNFTs indexes token transactions and NFTs for a transaction
func IndexTokenTransactionsAndNFTs(ctx context.Context, client *rpc.Client, tx pgx.Tx, transaction *types.Transaction, receipt *types.Receipt, blockNum uint64, canonical bool) error {
	transferEventSig := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	for _, txLog := range receipt.Logs {
		if len(txLog.Topics) < 3 || txLog.Topics[0] != transferEventSig {
			continue
		}

		tokenAddress := txLog.Address
		fromAddr := common.BytesToAddress(txLog.Topics[1][:])
		toAddr := common.BytesToAddress(txLog.Topics[2][:])

		// Ensure 'from' and 'to' accounts exist in the main accounts table.
		accountsToEnsure := []common.Address{fromAddr, toAddr}
		for _, acc := range accountsToEnsure {
			if acc != (common.Address{}) {
				_, err := tx.Exec(ctx,
					`INSERT INTO accounts (address, balance, nonce, is_contract, code, first_seen, last_seen, retrieved_from)
					 VALUES ($1, '0', 0, false, '', NOW(), NOW(), 'zond-indexer-placeholder') ON CONFLICT (address) DO NOTHING`,
					acc.Bytes())
				if err != nil {
					return fmt.Errorf("ensure account %s exists: %w", acc.Hex(), err)
				}
			}
		}

		if len(txLog.Topics) == 3 {
			err := IndexToken(ctx, client, tx, tokenAddress, blockNum, canonical, "ERC20")
			if err != nil {
				return fmt.Errorf("failed to ensure ERC20 token %s exists: %w", tokenAddress.Hex(), err)
			}

			if len(txLog.Data) != 32 {
				log.Printf("Warning: Invalid ERC-20 Transfer data length in tx %s", transaction.Hash().Hex())
				continue
			}

			value := new(big.Int).SetBytes(txLog.Data).String()

			// Update ERC-20 balances...
			if fromAddr != (common.Address{}) {
				_, err := tx.Exec(ctx, `INSERT INTO tokenbalances (address, token_address, balance) VALUES ($1, $2, '0') ON CONFLICT DO NOTHING`, fromAddr.Bytes(), tokenAddress.Bytes())
				if err != nil {
					return fmt.Errorf("ensure from_account exists in tokenbalances: %w", err)
				}
				_, err = tx.Exec(ctx, `UPDATE tokenbalances SET balance = balance - CAST($1 AS NUMERIC) WHERE address = $2 AND token_address = $3`, value, fromAddr.Bytes(), tokenAddress.Bytes())
				if err != nil {
					return fmt.Errorf("update from_address balance: %w", err)
				}
			}
			if toAddr != (common.Address{}) {
				_, err := tx.Exec(ctx, `INSERT INTO tokenbalances (address, token_address, balance) VALUES ($1, $2, CAST($3 AS NUMERIC)) ON CONFLICT (address, token_address) DO UPDATE SET balance = tokenbalances.balance + EXCLUDED.balance`, toAddr.Bytes(), tokenAddress.Bytes(), value)
				if err != nil {
					return fmt.Errorf("update to_address balance: %w", err)
				}
			}

			// Insert the ERC-20 transfer event.
			_, err = tx.Exec(ctx, `INSERT INTO tokentransactions (tx_hash, log_index, contract_address, from_address, to_address, value) VALUES ($1, $2, $3, $4, $5, $6)`, transaction.Hash().Bytes(), txLog.Index, tokenAddress.Bytes(), fromAddr.Bytes(), toAddr.Bytes(), value)
			if err != nil {
				return fmt.Errorf("insert ERC20 tokentransaction: %w", err)
			}
		}

		if len(txLog.Topics) == 4 {
			// This is an NFT transfer.
			err := IndexToken(ctx, client, tx, tokenAddress, blockNum, canonical, "ERC-721")
			if err != nil {
				return fmt.Errorf("failed to ensure ERC721 token %s exists: %w", tokenAddress.Hex(), err)
			}

			tokenID := new(big.Int).SetBytes(txLog.Topics[3][:]).String()

			// For an NFT, the "value" of the transfer is always 1 token.
			const nftValue = "1"

			// Insert the NFT transfer event.
			_, err = tx.Exec(ctx, `INSERT INTO tokentransactions (tx_hash, log_index, contract_address, from_address, to_address, value, token_id) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				transaction.Hash().Bytes(), txLog.Index, tokenAddress.Bytes(), fromAddr.Bytes(), toAddr.Bytes(), nftValue, tokenID)
			if err != nil {
				return fmt.Errorf("insert ERC721 tokentransaction: %w", err)
			}

			// Update ownership in the `nfts` table.
			_, err = tx.Exec(ctx,
				`INSERT INTO nfts (contract_address, token_id, owner, token_uri, metadata)
				 VALUES ($1, $2, $3, '', '{}')
				 ON CONFLICT (contract_address, token_id) DO UPDATE SET owner = EXCLUDED.owner`,
				tokenAddress.Bytes(), tokenID, toAddr.Bytes())
			if err != nil {
				return fmt.Errorf("upsert nft ownership for token %s id %s: %w", tokenAddress.Hex(), tokenID, err)
			}
		}
	}
	return nil
}

func IndexNFTs(ctx context.Context, client *rpc.Client, tx pgx.Tx, addr common.Address, blockNum uint64, canonical bool) error {
	// For simplicity, NFTs are indexed via Transfer events in IndexTokenTransactionsAndNFTs
	return nil
}

func fetchNFTMetadata(ctx context.Context, client *rpc.Client, addr common.Address, tokenID *big.Int) (NFTMetadata, error) {
	var metadata NFTMetadata

	tokenURI, err := callContractRaw(ctx, client, addr, "tokenURI(uint256)", tokenID)
	if err != nil || len(tokenURI) == 0 {
		tokenURI, err = callContractRaw(ctx, client, addr, "uri(uint256)", tokenID)
		if err != nil || len(tokenURI) == 0 {
			return metadata, fmt.Errorf("fetch token URI: %w", err)
		}
	}

	uri := string(bytesRemoveNull(tokenURI))
	metadata.TokenURI = uri

	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		httpClient := &http.Client{
			Timeout: 5 * time.Second,
		}
		resp, err := httpClient.Get(uri)
		if err != nil {
			return metadata, fmt.Errorf("fetch metadata from URI %s: %w", uri, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return metadata, fmt.Errorf("fetch metadata from URI %s: status %d", uri, resp.StatusCode)
		}

		var meta map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
			return metadata, fmt.Errorf("decode metadata from URI %s: %w", uri, err)
		}
		metadata.Metadata = meta
	}

	return metadata, nil
}

type NFTMetadata struct {
	TokenURI string                 `json:"token_uri"`
	Metadata map[string]interface{} `json:"metadata"`
}

func bytesRemoveNull(b []byte) []byte {
	return bytes.Map(func(r rune) rune {
		if r == '\x00' {
			return -1
		}
		return r
	}, b)
}
