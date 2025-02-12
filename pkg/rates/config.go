package rates

import "time"

// Config holds configuration for rates service
type Config struct {
	// BaseURLs contains the base URLs for different rate services
	BaseURLs struct {
		TonAPI     string `env:"TONAPI_URL" envDefault:"https://bridge-ton.hii.network"`
		BridgeTon  string `env:"BRIDGE_TON_URL" envDefault:"https://bridge-ton.hii.network"`
		// Add other service URLs as needed
	}
	// Timeout for API requests
	RequestTimeout time.Duration `env:"RATES_REQUEST_TIMEOUT" envDefault:"10s"`
}

// NewDefaultConfig returns a new Config with default values
func NewDefaultConfig() *Config {
	cfg := &Config{}
	cfg.BaseURLs.TonAPI = "https://bridge-ton.hii.network"
	cfg.BaseURLs.BridgeTon = "https://bridge-ton.hii.network"
	cfg.RequestTimeout = 10 * time.Second
	return cfg
} 