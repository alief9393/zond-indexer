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

// IndexToken indexes an ERC20 token contract
func IndexToken(ctx context.Context, client *rpc.Client, tx pgx.Tx, addr common.Address, blockNum uint64, canonical bool) error {
	name, err := callContractRaw(ctx, client, addr, "name()")
	if err != nil {
		return fmt.Errorf("fetch name: %w", err)
	}
	nameStr := string(bytesTrimNull(name))

	symbol, err := callContractRaw(ctx, client, addr, "symbol()")
	if err != nil {
		return fmt.Errorf("fetch symbol: %w", err)
	}
	symbolStr := string(bytesTrimNull(symbol))

	decimals, err := callContractRaw(ctx, client, addr, "decimals()")
	if err != nil {
		return fmt.Errorf("fetch decimals: %w", err)
	}
	var decimalsInt int
	if len(decimals) > 0 {
		decimalsBig := new(big.Int).SetBytes(decimals)
		if !decimalsBig.IsInt64() {
			return fmt.Errorf("decimals value too large: %s", decimalsBig.String())
		}
		decimalsInt = int(decimalsBig.Int64())
	} else {
		decimalsInt = 0
	}

	totalSupply, err := callContractRaw(ctx, client, addr, "totalSupply()")
	if err != nil {
		return fmt.Errorf("fetch totalSupply: %w", err)
	}
	totalSupplyStr := "0"
	if len(totalSupply) > 0 {
		totalSupplyBig := new(big.Int).SetBytes(totalSupply)
		totalSupplyStr = totalSupplyBig.String()
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO Tokens (
            contract_address, token_name, symbol, decimals, total_supply, block_number,
            retrieved_at, retrieved_from, is_canonical
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (contract_address) DO UPDATE
        SET token_name = EXCLUDED.token_name, symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals,
            total_supply = EXCLUDED.total_supply, block_number = EXCLUDED.block_number,
            retrieved_at = EXCLUDED.retrieved_at, retrieved_from = EXCLUDED.retrieved_from,
            is_canonical = EXCLUDED.is_canonical`,
		addr.Bytes(),
		nameStr,
		symbolStr,
		decimalsInt,
		totalSupplyStr,
		blockNum,
		time.Now(),
		"zond_node",
		canonical)
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
                    tx_hash, token_address, from_address, to_address, value,
                    block_number, retrieved_at, retrieved_from, is_canonical
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (tx_hash) DO UPDATE
                SET token_address = EXCLUDED.token_address, from_address = EXCLUDED.from_address,
                    to_address = EXCLUDED.to_address, value = EXCLUDED.value,
                    block_number = EXCLUDED.block_number, retrieved_at = EXCLUDED.retrieved_at,
                    retrieved_from = EXCLUDED.retrieved_from, is_canonical = EXCLUDED.is_canonical`,
				transaction.Hash().Bytes(),
				txLog.Address.Bytes(),
				fromAddr.Bytes(),
				toAddr.Bytes(),
				value.String(),
				blockNum,
				time.Now(),
				"zond_node",
				canonical)
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
                        token_address, token_id, owner_address, token_uri, metadata,
                        block_number, retrieved_at, retrieved_from, is_canonical
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (token_address, token_id) DO UPDATE
                    SET owner_address = $3, block_number = $6, is_canonical = $9`,
					txLog.Address.Bytes(),
					tokenID.String(),
					toAddr.Bytes(),
					metadata.TokenURI,
					metadataJSON,
					blockNum,
					time.Now(),
					"zond_node",
					canonical)
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

	uri := string(bytesTrimNull(tokenURI))
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
