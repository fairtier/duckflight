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

	// Extensions
	ExtensionDir string // custom extension_directory for pre-installed extensions

	// RejectClientExtensions makes the server refuse client-issued
	// INSTALL/LOAD statements. Extensions are then exclusively managed by the
	// operator (baked into ExtensionDir, loaded via boot or reconcile SQL).
	RejectClientExtensions bool

	// ReconcileSQLPath points at a SQL file (typically a mounted Kubernetes
	// Secret) that is executed instance-wide at startup and re-executed
	// whenever its content changes. Empty disables the watcher.
	ReconcileSQLPath string

	// TempDirectory enables DuckDB spilling to disk. An in-memory database
	// has no temp_directory by default, so large operations error instead of
	// degrading to disk.
	TempDirectory string

	// Metering
	MaxResultBytes int64

	// ReadOnly enables read-only sessions, served by a second, hardened
	// DuckDB instance. nil (the default) disables them: no second instance
	// is created and a read-only request is refused.
	ReadOnly *ReadOnlyConfig
}

// ReadOnlyConfig sizes and confines the DuckDB instance behind read-only
// sessions. It shares the Iceberg/S3 settings of the main Config.
type ReadOnlyConfig struct {
	// PoolSize is the connection pool of the read-only instance. 0 means
	// the main PoolSize.
	PoolSize int
	// MemoryLimit is the read-only instance's own buffer pool. "" means the
	// main MemoryLimit. The two instances do not share memory, so the
	// process can use up to the sum of both.
	MemoryLimit string
	// AllowedDirectories are the only paths/URL prefixes the read-only
	// instance may open once external access is disabled (DuckDB's
	// allowed_directories). Iceberg metadata and data files are read through
	// it, so it must cover the warehouse location. nil means ["s3://"] when
	// an Iceberg catalog is configured and nothing otherwise; narrow it to
	// the warehouse bucket (e.g. "s3://warehouse/") where possible.
	AllowedDirectories []string
}

// DefaultConfig returns a Config with sensible defaults for development.
//
// Listen addresses, auth and rate limiting are deliberately absent: they are
// read from the environment in cmd/server and never travel through Config.
// Fields that look like they configure something but are read nowhere are a
// trap — setting AuthTokens here once looked like it would gate access.
func DefaultConfig() *Config {
	return &Config{
		MemoryLimit:  "512MB",
		MaxThreads:   4,
		QueryTimeout: "30s",
		PoolSize:     4,
	}
}
