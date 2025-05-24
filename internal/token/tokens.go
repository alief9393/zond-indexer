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

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
)

// IndexToken indexes a token contract based on its type
func IndexToken(ctx context.Context, client *rpc.Client, tx pgx.Tx, addr common.Address, blockNum uint64, canonical bool, tokenType string) error {
	name, err := callContractRaw(ctx, client, addr, "name()")
	if err != nil {
		return fmt.Errorf("fetch name: %w", err)
	}
	nameStr := decodeStringABI(name)

	symbol, err := callContractRaw(ctx, client, addr, "symbol()")
	if err != nil {
		return fmt.Errorf("fetch symbol: %w", err)
	}
	symbolStr := decodeStringABI(symbol)

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

	totalSupplyStr := "0"
	if tokenType == "ERC20" {
		supplyBytes, err := callContractRaw(ctx, client, addr, "totalSupply()")
		if err == nil && len(supplyBytes) > 0 {
			supplyBig := new(big.Int).SetBytes(supplyBytes)
			totalSupplyStr = supplyBig.String()
		}
	}

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
		addr.Bytes(),
		nameStr,
		symbolStr,
		totalSupplyStr,
		decimals,
		tokenType,
		"",
		"",
		canonical,
		time.Now(),
		"zond_node",
		nil,
	)
	if err != nil {
		return fmt.Errorf("insert token %s: %w", addr.Hex(), err)
	}

	return nil
}

// IndexTokenTransactionsAndNFTs indexes token transactions and NFTs for a transaction
func IndexTokenTransactionsAndNFTs(ctx context.Context, client *rpc.Client, tx pgx.Tx, transaction *types.Transaction, receipt *types.Receipt, blockNum uint64, canonical bool) error {
	for _, txLog := range receipt.Logs {
		if len(txLog.Topics) == 0 {
			continue
		}

		// Check for ERC20 Transfer event
		transferEventSig := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
		if txLog.Topics[0] == transferEventSig && len(txLog.Topics) == 4 {
			if len(txLog.Topics[1]) != 32 || len(txLog.Topics[2]) != 32 || len(txLog.Topics[3]) != 32 {
				continue
			}

			fromAddr := common.BytesToAddress(txLog.Topics[1][12:])
			toAddr := common.BytesToAddress(txLog.Topics[2][12:])
			value := new(big.Int).SetBytes(txLog.Topics[3][:])

			_, err := tx.Exec(ctx,
				`INSERT INTO TokenTransactions (
					tx_hash, contract_address, from_address, to_address, token_id, value,
					is_canonical, retrieved_at, retrieved_from, reverted_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (tx_hash, contract_address, from_address, to_address, token_id) DO UPDATE
				SET value = EXCLUDED.value, is_canonical = EXCLUDED.is_canonical,
					retrieved_at = EXCLUDED.retrieved_at, retrieved_from = EXCLUDED.retrieved_from,
					reverted_at = EXCLUDED.reverted_at`,
				transaction.Hash().Bytes(),
				txLog.Address.Bytes(),
				fromAddr.Bytes(),
				toAddr.Bytes(),
				"", // token_id (empty for ERC20)
				value.Int64(),
				canonical,
				time.Now(),
				"zond_node",
				nil, // reverted_at
			)
			if err != nil {
				return fmt.Errorf("insert token transaction: %w", err)
			}
		}

		// Check for ERC721 or ERC1155 Transfer events
		transferSingleSig := common.HexToHash("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f")
		if (txLog.Topics[0] == transferEventSig && len(txLog.Topics) == 4) || (txLog.Topics[0] == transferSingleSig && len(txLog.Topics) == 4) {
			isERC721 := txLog.Topics[0] == transferEventSig
			isERC1155 := txLog.Topics[0] == transferSingleSig

			var tokenID *big.Int
			var toAddr common.Address

			if isERC721 {
				tokenID = new(big.Int).SetBytes(txLog.Topics[3][:])
				toAddr = common.BytesToAddress(txLog.Topics[2][12:])
			} else if isERC1155 {
				if len(txLog.Data) < 64 {
					continue
				}
				tokenID = new(big.Int).SetBytes(txLog.Data[:32])
				toAddr = common.BytesToAddress(txLog.Topics[3][12:])
			} else {
				continue
			}

			if toAddr != (common.Address{}) {
				metadata, err := fetchNFTMetadata(ctx, client, txLog.Address, tokenID)
				if err != nil {
					log.Printf("Block %d: Failed to fetch NFT metadata for token %s ID %s: %v", blockNum, txLog.Address.Hex(), tokenID.String(), err)
				}

				metadataJSON, err := json.Marshal(metadata)
				if err != nil {
					log.Printf("Block %d: Failed to marshal NFT metadata for token %s ID %s: %v", blockNum, txLog.Address.Hex(), tokenID.String(), err)
					metadataJSON = []byte("{}")
				}

				_, err = tx.Exec(ctx,
					`INSERT INTO NFTs (
						contract_address, token_id, token_uri, owner, metadata,
						is_canonical, retrieved_at, retrieved_from, reverted_at
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
					ON CONFLICT (contract_address, token_id) DO UPDATE
					SET owner = EXCLUDED.owner, token_uri = EXCLUDED.token_uri,
						metadata = EXCLUDED.metadata, is_canonical = EXCLUDED.is_canonical,
						retrieved_at = EXCLUDED.retrieved_at, retrieved_from = EXCLUDED.retrieved_from,
						reverted_at = EXCLUDED.reverted_at`,
					txLog.Address.Bytes(),
					tokenID.String(),
					metadata.TokenURI,
					toAddr.Bytes(),
					metadataJSON,
					canonical,
					time.Now(),
					"zond_node",
					nil,
				)
				if err != nil {
					return fmt.Errorf("insert NFT: %w", err)
				}
			}
		}
	}

	return nil
}

// IndexNFTs indexes NFTs for an ERC721 or ERC1155 contract
func IndexNFTs(ctx context.Context, client *rpc.Client, tx pgx.Tx, addr common.Address, blockNum uint64, canonical bool) error {
	// For simplicity, NFTs are indexed via Transfer events in IndexTokenTransactionsAndNFTs
	return nil
}

// fetchNFTMetadata fetches the token URI and metadata for an NFT
func fetchNFTMetadata(ctx context.Context, client *rpc.Client, addr common.Address, tokenID *big.Int) (NFTMetadata, error) {
	var metadata NFTMetadata

	// Try to fetch tokenURI (ERC721) or uri (ERC1155)
	tokenURI, err := callContractRaw(ctx, client, addr, "tokenURI(uint256)", tokenID)
	if err != nil || len(tokenURI) == 0 {
		// Try ERC1155 uri
		tokenURI, err = callContractRaw(ctx, client, addr, "uri(uint256)", tokenID)
		if err != nil || len(tokenURI) == 0 {
			return metadata, fmt.Errorf("fetch token URI: %w", err)
		}
	}

	uri := string(bytesRemoveNull(tokenURI))
	metadata.TokenURI = uri

	// Fetch metadata from the token URI (simplified, assumes URI points to JSON metadata)
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

// NFTMetadata holds metadata for an NFT
type NFTMetadata struct {
	TokenURI string                 `json:"token_uri"`
	Metadata map[string]interface{} `json:"metadata"`
}

// bytesTrimNull trims null bytes from the end of a byte slice
func bytesTrimNull(b []byte) []byte {
	return bytes.TrimRight(b, "\x00")
}

func bytesRemoveNull(b []byte) []byte {
	return bytes.Map(func(r rune) rune {
		if r == '\x00' {
			return -1
		}
		return r
	}, b)
}

func decodeStringABI(data []byte) string {
	if len(data) < 96 {
		return string(bytesTrimNull(data))
	}
	strLen := new(big.Int).SetBytes(data[32:64]).Int64()
	strBytes := data[64 : 64+strLen]
	return string(strBytes)
}
