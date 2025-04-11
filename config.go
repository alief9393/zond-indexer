package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	PostgresConn     string
	PostgresUser     string
	PostgresPassword string
	PostgresHost     string
	PostgresPort     string
	RPCEndpoint      string
	RateLimit        time.Duration
}

func loadConfig() Config {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found, falling back to environment variables: %v", err)
	}

	// Helper function to get environment variable or panic if not set
	getEnv := func(key string) string {
		value, exists := os.LookupEnv(key)
		if !exists {
			log.Fatalf("Environment variable %s is required but not set", key)
		}
		return value
	}

	// Load configuration values
	postgresConn := getEnv("POSTGRES_CONN")
	rpcEndpoint := getEnv("RPC_ENDPOINT")

	// Parse POSTGRES_CONN to extract user, password, host, and port
	var postgresUser, postgresPassword, postgresHost, postgresPort string
	parts := strings.Split(postgresConn, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "user=") {
			postgresUser = strings.TrimPrefix(part, "user=")
		} else if strings.HasPrefix(part, "password=") {
			postgresPassword = strings.TrimPrefix(part, "password=")
		} else if strings.HasPrefix(part, "host=") {
			postgresHost = strings.TrimPrefix(part, "host=")
		} else if strings.HasPrefix(part, "port=") {
			postgresPort = strings.TrimPrefix(part, "port=")
		}
	}
	if postgresUser == "" || postgresPassword == "" || postgresHost == "" || postgresPort == "" {
		log.Fatalf("POSTGRES_CONN must include user, password, host, and port")
	}

	// Parse rate limit (in milliseconds)
	rateLimitStr := getEnv("RATE_LIMIT_MS")
	rateLimitMs, err := strconv.Atoi(rateLimitStr)
	if err != nil {
		log.Fatalf("Invalid RATE_LIMIT_MS value: %v", err)
	}
	rateLimit := time.Duration(rateLimitMs) * time.Millisecond

	return Config{
		PostgresConn:     postgresConn,
		PostgresUser:     postgresUser,
		PostgresPassword: postgresPassword,
		PostgresHost:     postgresHost,
		PostgresPort:     postgresPort,
		RPCEndpoint:      rpcEndpoint,
		RateLimit:        rateLimit,
	}
}

var defaultConfig = loadConfig()
