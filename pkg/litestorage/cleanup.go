package litestorage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type cleanupMetrics struct {
	processedCount int
	errorCount     int
	startTime      time.Time
}

func (s *LiteStorage) startBlockCleanup(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.cleanupWithMetrics(); err != nil {
				s.logger.Error("cleanup failed", zap.Error(err))
			}
		}
	}
}

func (s *LiteStorage) cleanupWithMetrics() error {
	start := time.Now()

	s.cleanupMutex.Lock()
	defer s.cleanupMutex.Unlock()

	metrics := &cleanupMetrics{
		processedCount: 0,
		errorCount:     0,
		startTime:      start,
	}
	batchSize := 500
	for {
		processed, err := s.cleanupBatch(batchSize, metrics)
		if err != nil {
			s.logger.Error("cleanup batch failed", zap.Error(err))
			return err
		}
		if processed == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.recordCleanupMetrics(metrics)
	return nil
}

func (s *LiteStorage) writeBatch(txn *badger.Txn, batch map[string][]byte) error {
	for key, value := range batch {
		if err := txn.Set([]byte(key), value); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}
		// Delete old transaction
		oldKey := strings.Replace(key, ltxKeyPrefix, txKeyPrefix, 1)
		if err := txn.Delete([]byte(oldKey)); err != nil {
			s.logger.Error("failed to delete old transaction", zap.Error(err))
		}
	}
	return nil
}

func (s *LiteStorage) cleanupBatch(batchSize int, metrics *cleanupMetrics) (int, error) {
	const maxBatchSize = 1000
	if batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}

	var processed int
	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(txKeyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		batch := make(map[string][]byte, batchSize)
		for it.Rewind(); it.Valid() && len(batch) < batchSize; it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				batch[string(item.Key())] = val
				return nil
			})
			if err != nil {
				metrics.errorCount++
				continue
			}
			processed++
			metrics.processedCount++
		}
		return s.writeBatch(txn, batch)
	})
	return processed, err
}

func (s *LiteStorage) recordCleanupMetrics(metrics *cleanupMetrics) {
	duration := time.Since(metrics.startTime)
	s.metrics.cleanupDuration.Observe(duration.Seconds())
	s.logger.Info("cleanup completed",
		zap.Duration("duration", duration),
		zap.Int("processed", metrics.processedCount),
		zap.Int("errors", metrics.errorCount))
}
