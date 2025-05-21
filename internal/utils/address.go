package utils

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/theQRL/go-zond/common"
)

// HexToAddress converts a hex string to a common.Address
func HexToAddress(s string) (common.Address, error) {
	// Handle Z prefix for Zond addresses
	if strings.HasPrefix(s, "Z") {
		s = strings.TrimPrefix(s, "Z")
	} else {
		// Also handle standard 0x prefix for compatibility
		s = strings.TrimPrefix(s, "0x")
	}

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

// IsValidHexAddress checks if an address is a valid 40-character hex string (with Z or 0x prefix)
func IsValidHexAddress(addr string) bool {
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

func HexToAddressBytes(s string) ([]byte, error) {
	addr, err := HexToAddress(s)
	if err != nil {
		return nil, err
	}
	return addr.Bytes(), nil
}

// MustHexToAddressBytes is a strict version that never fails (optional fallback)
func MustHexToAddressBytes(s string) []byte {
	bz, err := HexToAddressBytes(s)
	if err != nil || len(bz) != 20 {
		return make([]byte, 20) // insert placeholder to avoid panic
	}
	return bz
}
