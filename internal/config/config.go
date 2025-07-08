package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	RPCEndpoint  string
	PostgresConn string
	RateLimit    time.Duration
	BeaconHost   string
	BeaconPort   int
}

// LoadConfig loads the configuration from environment variables
func LoadConfig() (Config, error) {
	cfg := Config{
		RPCEndpoint:  getEnv("RPC_ENDPOINT", ""),
		PostgresConn: getEnv("POSTGRES_CONN", ""),
		BeaconHost:   getEnv("BEACON_HOST", "localhost"),
	}

	// Parse the beacon port
	beaconPortStr := getEnv("BEACON_PORT", "3500")
	beaconPort, err := strconv.Atoi(beaconPortStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid BEACON_PORT: %w", err)
	}
	cfg.BeaconPort = beaconPort

	// Parse the rate limit (in ms)
	rateLimitStr := getEnv("RATE_LIMIT_MS", "0")
	rateLimitMs, err := strconv.Atoi(rateLimitStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid RATE_LIMIT_MS: %w", err)
	}
	cfg.RateLimit = time.Duration(rateLimitMs) * time.Millisecond

	// Valida6te the configuration
	if cfg.RPCEndpoint == "" {
		return Config{}, fmt.Errorf("RPC_ENDPOINT is required")
	}
	if cfg.PostgresConn == "" {
		return Config{}, fmt.Errorf("POSTGRES_CONN is required")
	}
	if cfg.BeaconHost == "" {
		return Config{}, fmt.Errorf("BEACON_HOST is required")
	}
	if cfg.BeaconPort <= 0 || cfg.BeaconPort > 65535 {
		return Config{}, fmt.Errorf("BEACON_PORT must be between 1 and 65535, got %d", cfg.BeaconPort)
	}

	return cfg, nil
}

// BeaconURL constructs the full beacon node URL
func (c Config) BeaconURL() string {
	return fmt.Sprintf("http://%s:%d", c.BeaconHost, c.BeaconPort)
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
