package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BlockCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tonapi_block_cache_hits_total",
		Help: "The total number of block cache hits",
	})

	BlockCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tonapi_block_cache_misses_total",
		Help: "The total number of block cache misses",
	})

	BlockStorageErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tonapi_block_storage_errors_total",
		Help: "The total number of block storage errors",
	})
)
