package litestorage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)


func initMetrics() *StorageMetrics {
	return &StorageMetrics{
		dbLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "litestorage_db_operation_latency",
				Help:    "Database operation latency in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
			},
			[]string{"operation"},
		),
		dbOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "litestorage_db_operations_total",
				Help: "Total number of database operations",
			},
			[]string{"operation", "status"},
		),
		dbErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "litestorage_db_errors_total",
				Help: "Total number of database errors",
			},
			[]string{"operation", "error_type"},
		),
		cacheHits: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "litestorage_cache_hits_total",
				Help: "Total number of cache hits",
			},
			[]string{"cache"},
		),
		cacheMisses: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "litestorage_cache_misses_total",
				Help: "Total number of cache misses",
			},
			[]string{"cache"},
		),
	}
} 