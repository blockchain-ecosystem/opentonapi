package litestorage

import (
	"time"

	"github.com/tonkeeper/opentonapi/pkg/cache"
)

type CacheManager struct {
	caches    map[string]cache.Cache[string, interface{}]
	metrics   *StorageMetrics
	evictTime time.Duration
}

func (cm *CacheManager) Set(cacheName string, key string, value interface{}) {
	if c, exists := cm.caches[cacheName]; exists {
		c.Set(key, value, cache.WithExpiration(cm.evictTime))
		cm.metrics.dbOperations.WithLabelValues("cache", cacheName).Inc()
	}
}

func (cm *CacheManager) Evict(cacheName string) {
	if c, exists := cm.caches[cacheName]; exists {
		for _, key := range c.Keys() {
			c.Delete(key)
		}
		cm.metrics.dbOperations.WithLabelValues("cache", cacheName+"_clear").Inc()
	}
} 