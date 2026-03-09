package config

// Config holds runtime configuration for the duckflight server.
type Config struct {
	// DuckDB resource limits
	MemoryLimit  string
	MaxThreads   int
	QueryTimeout string

	// Arrow connection pool
	PoolSize int

	// Iceberg catalog
	IcebergEndpoint  string // REST catalog URL (e.g. http://host:8181/catalog)
	IcebergWarehouse string // Warehouse name to ATTACH

	// Iceberg catalog auth (OAuth2) — optional, skip CREATE SECRET if empty
	IcebergClientID     string
	IcebergClientSecret string
	IcebergOAuth2URI    string

	// Storage credentials — optional, for when catalog doesn't vend credentials
	// S3-compatible storage
	S3Endpoint  string // e.g. http://minio:9000
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3URLStyle  string // "path" for MinIO, "vhost" for AWS (default)

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
