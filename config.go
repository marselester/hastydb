package hasty

const (
	// DefaultMaxMemtableSize is a maximum memtable size in bytes when it is written on disk.
	// Default value is 4 megabytes.
	DefaultMaxMemtableSize = 4 * 1024 * 1024
)

// Config contains database settings which are updated with ConfigOption functions.
type Config struct {
	maxMemtableSize int
}

// ConfigOption helps to change default database settings.
type ConfigOption func(*Config)

// WithMaxMemtableSize sets maximum memtable size in bytes when it should be written on disk.
func WithMaxMemtableSize(threshold int) ConfigOption {
	return func(c *Config) {
		c.maxMemtableSize = threshold
	}
}
