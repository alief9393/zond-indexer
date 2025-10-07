package token

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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

		// ERC-20 Logic (Unchanged)
		if len(txLog.Topics) == 3 {
			// ... (This block remains exactly the same as your provided code)
		}

		// --- MODIFIED LOGIC FOR ERC-721 (NFTs) ---
		if len(txLog.Topics) == 4 {
			err := IndexToken(ctx, client, tx, tokenAddress, blockNum, canonical, "ERC-721")
			if err != nil {
				return fmt.Errorf("failed to ensure ERC721 token %s exists: %w", tokenAddress.Hex(), err)
			}

			tokenIDBig := new(big.Int).SetBytes(txLog.Topics[3][:])
			tokenIDStr := tokenIDBig.String()
			const nftValue = "1"

			_, err = tx.Exec(ctx, `INSERT INTO tokentransactions (tx_hash, log_index, contract_address, from_address, to_address, value, token_id) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				transaction.Hash().Bytes(), txLog.Index, tokenAddress.Bytes(), fromAddr.Bytes(), toAddr.Bytes(), nftValue, tokenIDStr)
			if err != nil {
				return fmt.Errorf("insert ERC721 tokentransaction: %w", err)
			}

			// If this is a mint event (from the zero address), we fetch metadata.
			// Otherwise, we just update the owner.
			if fromAddr == (common.Address{}) {
				log.Printf("[NFT Indexer] New mint detected for %s, ID %s. Fetching metadata...", tokenAddress.Hex(), tokenIDStr)
				metadata, err := fetchNFTMetadata(ctx, client, tokenAddress, tokenIDBig)
				if err != nil {
					log.Printf("⚠️ [NFT Indexer] Could not fetch metadata for token %s ID %s: %v. Saving placeholder.", tokenAddress.Hex(), tokenIDStr, err)
					// Save a placeholder record
					_, err = tx.Exec(ctx, `INSERT INTO nfts (contract_address, token_id, owner, token_uri, metadata) VALUES ($1, $2, $3, '', '{}') ON CONFLICT (contract_address, token_id) DO UPDATE SET owner = EXCLUDED.owner`, tokenAddress.Bytes(), tokenIDStr, toAddr.Bytes())
					if err != nil {
						return fmt.Errorf("upsert placeholder nft ownership on mint: %w", err)
					}
				} else {
					// Save the fetched metadata
					metadataJSON, _ := json.Marshal(metadata.Metadata)
					_, err = tx.Exec(ctx, `INSERT INTO nfts (contract_address, token_id, owner, token_uri, metadata) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (contract_address, token_id) DO UPDATE SET owner = EXCLUDED.owner, token_uri = EXCLUDED.token_uri, metadata = EXCLUDED.metadata`,
						tokenAddress.Bytes(), tokenIDStr, toAddr.Bytes(), metadata.TokenURI, metadataJSON)
					if err != nil {
						return fmt.Errorf("upsert nft with metadata: %w", err)
					}
				}
			} else {
				// This is a transfer, not a mint. We just update the owner.
				_, err = tx.Exec(ctx, `UPDATE nfts SET owner = $1 WHERE contract_address = $2 AND token_id = $3`, toAddr.Bytes(), tokenAddress.Bytes(), tokenIDStr)
				if err != nil {
					// This could fail if the NFT was transferred before its mint was indexed.
					// Insert a placeholder to ensure the record exists.
					_, err_insert := tx.Exec(ctx, `INSERT INTO nfts (contract_address, token_id, owner, token_uri, metadata) VALUES ($1, $2, $3, '', '{}') ON CONFLICT (contract_address, token_id) DO UPDATE SET owner = EXCLUDED.owner`, tokenAddress.Bytes(), tokenIDStr, toAddr.Bytes())
					if err_insert != nil {
						return fmt.Errorf("update/insert nft ownership on transfer: %w", err_insert)
					}
				}
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

	// We'll try both standard methods for getting the URI
	tokenURIBytes, err := callContractRaw(ctx, client, addr, "tokenURI(uint256)", tokenID)
	if err != nil || len(tokenURIBytes) == 0 {
		tokenURIBytes, err = callContractRaw(ctx, client, addr, "uri(uint256)", tokenID)
		if err != nil || len(tokenURIBytes) == 0 {
			return metadata, fmt.Errorf("failed to fetch token URI using both methods: %w", err)
		}
	}

	// The result from the contract call is often padded and may contain null characters. Clean it.
	uri := string(bytes.Trim(bytes.Trim(tokenURIBytes, "\x00"), `"`))
	// Some contracts return the raw data, others return a hex string of the data.
	if !strings.HasPrefix(uri, "http") && !strings.HasPrefix(uri, "ipfs") && !strings.HasPrefix(uri, "data:") {
		cleaned, err := hex.DecodeString(uri)
		if err == nil {
			uri = string(cleaned)
		}
	}
	uri = strings.Trim(uri, "\x00")
	metadata.TokenURI = uri

	// Fetch the actual JSON content if the URI is a web link
	if strings.HasPrefix(uri, "http") {
		httpClient := &http.Client{Timeout: 5 * time.Second}
		resp, err := httpClient.Get(uri)
		if err != nil {
			return metadata, fmt.Errorf("failed to fetch metadata from http URI %s: %w", uri, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return metadata, fmt.Errorf("http fetch returned non-200 status: %d", resp.StatusCode)
		}

		if err := json.NewDecoder(resp.Body).Decode(&metadata.Metadata); err != nil {
			return metadata, fmt.Errorf("failed to decode JSON from http URI %s: %w", uri, err)
		}
	} else if strings.HasPrefix(uri, "data:application/json;base64,") {
		// Handle inline, base64 encoded JSON
		base64Data := strings.TrimPrefix(uri, "data:application/json;base64,")
		jsonData, err := base64.StdEncoding.DecodeString(base64Data)
		if err != nil {
			return metadata, fmt.Errorf("failed to decode base64 metadata: %w", err)
		}
		if err := json.Unmarshal(jsonData, &metadata.Metadata); err != nil {
			return metadata, fmt.Errorf("failed to unmarshal inline JSON: %w", err)
		}
	}

	return metadata, nil
}

type NFTMetadata struct {
	TokenURI string                 `json:"token_uri"`
	Metadata map[string]interface{} `json:"metadata"`
}
