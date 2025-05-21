package models

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"zond-indexer/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/theQRL/go-zond/zondclient"
)

// ValidatorData represents the data for a validator
type ValidatorData struct {
	PublicKey             []byte
	WithdrawalCredentials []byte
	EffectiveBalance      int64
	Status                string
	DepositAmount         int64
}

// IndexValidators fetches validator data from the qrysm beacon chain
func IndexValidators(ctx context.Context, client *zondclient.Client, tx pgx.Tx, cfg config.Config) error {
	beaconNodeURL := cfg.BeaconURL() + "/zond/v1/beacon/states/head/validators"
	const depositAmount = 32_000_000_000

	// Fetch existing validator data from the database for comparison
	existingValidators := make(map[int64]ValidatorData)
	rows, err := tx.Query(ctx, `
        SELECT validator_index, public_key, withdrawal_credentials, effective_balance, status, deposit_amount
        FROM Validators
    `)
	if err != nil {
		return fmt.Errorf("fetch existing validators: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var index int64
		var data ValidatorData
		if err := rows.Scan(&index, &data.PublicKey, &data.WithdrawalCredentials, &data.EffectiveBalance, &data.Status, &data.DepositAmount); err != nil {
			return fmt.Errorf("scan existing validator: %w", err)
		}
		existingValidators[index] = data
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate existing validators: %w", err)
	}

	// Fetch validator data from the beacon node
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", beaconNodeURL, nil)
	if err != nil {
		return fmt.Errorf("create HTTP request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch validators from beacon node: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("beacon node returned status %d", resp.StatusCode)
	}

	var validatorResponse struct {
		Data []struct {
			Index     string `json:"index"`
			Validator struct {
				PublicKey             string `json:"pubkey"`
				WithdrawalCredentials string `json:"withdrawal_credentials"`
				EffectiveBalance      string `json:"effective_balance"`
			} `json:"validator"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&validatorResponse); err != nil {
		return fmt.Errorf("decode validator response: %w", err)
	}

	// Track changes and summarize statuses
	statusCount := make(map[string]int)
	updatedValidators := 0
	newValidators := 0

	for _, v := range validatorResponse.Data {
		validatorIndex, err := parseInt(v.Index)
		if err != nil {
			log.Printf("Skipping validator with invalid index %s: %v", v.Index, err)
			continue
		}

		publicKeyBytes, err := hexToBytes(v.Validator.PublicKey)
		if err != nil {
			log.Printf("Skipping validator %s with invalid public key %s: %v", v.Index, v.Validator.PublicKey, err)
			continue
		}

		withdrawalCredentialsBytes, err := hexToBytes(v.Validator.WithdrawalCredentials)
		if err != nil {
			log.Printf("Skipping validator %s with invalid withdrawal credentials %s: %v", v.Index, v.Validator.WithdrawalCredentials, err)
			continue
		}

		effectiveBalance, err := parseInt(v.Validator.EffectiveBalance)
		if err != nil {
			log.Printf("Skipping validator %s with invalid effective balance %s: %v", v.Index, v.Validator.EffectiveBalance, err)
			continue
		}

		// Build the new validator data
		newData := ValidatorData{
			PublicKey:             publicKeyBytes,
			WithdrawalCredentials: withdrawalCredentialsBytes,
			EffectiveBalance:      effectiveBalance,
			Status:                v.Status,
			DepositAmount:         depositAmount,
		}

		statusCount[v.Status]++

		// Compare with existing data
		existingData, exists := existingValidators[validatorIndex]
		if !exists {
			// New validator
			_, err := tx.Exec(ctx,
				`INSERT INTO Validators (
					validator_index, public_key, deposit_amount, withdrawal_credentials,
					effective_balance, status, retrieved_at, retrieved_from, reverted_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (validator_index) DO UPDATE
				SET public_key = EXCLUDED.public_key,
					deposit_amount = EXCLUDED.deposit_amount,
					withdrawal_credentials = EXCLUDED.withdrawal_credentials,
					effective_balance = EXCLUDED.effective_balance,
					status = EXCLUDED.status,
					retrieved_at = EXCLUDED.retrieved_at,
					retrieved_from = EXCLUDED.retrieved_from,
					reverted_at = EXCLUDED.reverted_at`,
				validatorIndex,
				publicKeyBytes,
				depositAmount,
				withdrawalCredentialsBytes,
				effectiveBalance,
				v.Status,
				time.Now(),
				"zond_node",
				nil, // reverted_at
			)
			if err != nil {
				log.Printf("Failed to insert validator %s: %v", v.Index, err)
				continue
			}
			newValidators++
			log.Printf("Added new validator %s with status %s", v.Index, v.Status)
		} else {
			// Check if any field has changed
			if !bytesEqual(existingData.PublicKey, newData.PublicKey) ||
				!bytesEqual(existingData.WithdrawalCredentials, newData.WithdrawalCredentials) ||
				existingData.EffectiveBalance != newData.EffectiveBalance ||
				existingData.Status != newData.Status ||
				existingData.DepositAmount != newData.DepositAmount {
				_, err = tx.Exec(ctx,
					`UPDATE Validators
                     SET public_key = $1, deposit_amount = $2, withdrawal_credentials = $3,
                         effective_balance = $4, status = $5, retrieved_at = $6
                     WHERE validator_index = $7`,
					newData.PublicKey,
					newData.DepositAmount,
					newData.WithdrawalCredentials,
					newData.EffectiveBalance,
					newData.Status,
					time.Now(),
					validatorIndex)
				if err != nil {
					log.Printf("Failed to update validator %s: %v", v.Index, err)
					continue
				}
				updatedValidators++
				log.Printf("Updated validator %s: status=%s, effective_balance=%d", v.Index, newData.Status, newData.EffectiveBalance)
			}
		}
	}

	// Log the summary
	for status, count := range statusCount {
		log.Printf("Validators: %d %s", count, status)
	}
	log.Printf("Processed %d validators: %d new, %d updated", len(validatorResponse.Data), newValidators, updatedValidators)

	return nil
}

// parseInt converts a string to an int64
func parseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// hexToBytes converts a hex string (with or without 0x prefix) to a byte slice
func hexToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

// bytesEqual compares two byte slices for equality
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
