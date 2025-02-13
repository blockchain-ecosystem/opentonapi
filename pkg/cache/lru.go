package cache

import (
	"container/list"
	"sync"
)

type LRUCache[K comparable, V any] struct {
	capacity int
	items    *list.List
	cache    map[K]*list.Element
	mu       sync.RWMutex
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

func NewGenericLRUCache[K comparable, V any](capacity int, name string) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity: capacity,
		items:    list.New(),
		cache:    make(map[K]*list.Element),
	}
}

func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if element, exists := c.cache[key]; exists {
		c.items.MoveToFront(element)
		return element.Value.(entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (c *LRUCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, exists := c.cache[key]; exists {
		c.items.MoveToFront(element)
		element.Value = entry[K, V]{key, value}
		return
	}
	element := c.items.PushFront(entry[K, V]{key, value})
	c.cache[key] = element
	if c.items.Len() > c.capacity {
		c.removeOldest()
	}
}

func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = list.New()
	c.cache = make(map[K]*list.Element)
}

func (c *LRUCache[K, V]) removeOldest() {
	element := c.items.Back()
	if element != nil {
		c.items.Remove(element)
		delete(c.cache, element.Value.(entry[K, V]).key)
	}
} 