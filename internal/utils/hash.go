package utils

import (
	"golang.org/x/crypto/sha3"
)

// Keccak256 returns the Keccak256 hash of the input data.
func Keccak256(data []byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	hash.Write(data)
	return hash.Sum(nil)
}
