package config

// Config holds runtime configuration for the duckflight server.
type Config struct {
	// DuckDB resource limits
	MemoryLimit  string
	MaxThreads   int
	QueryTimeout string

	// Arrow connection pool
	PoolSize int

	// Iceberg catalog (M12)
	IcebergClientID     string
	IcebergClientSecret string
	IcebergOAuth2URI    string
	IcebergWarehouse    string
	IcebergEndpoint     string

	// Server
	ListenAddr string

	// Metrics
	MetricAddr string

	// Auth
	AuthTokens []string

	// Metering
	MaxResultBytes int64
}

// DefaultConfig returns a Config with sensible defaults for development.
func DefaultConfig() *Config {
	return &Config{
		MemoryLimit:  "512MB",
		MaxThreads:   4,
		QueryTimeout: "30s",
		PoolSize:     4,
		ListenAddr:   "localhost:0",
	}
}
