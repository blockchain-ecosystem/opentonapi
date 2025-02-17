package litestorage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tonkeeper/opentonapi/pkg/core"
	"github.com/tonkeeper/tongo"
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

	batchSize := 100
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

func (s *LiteStorage) cleanOldBlocks() error {
	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(txKeyPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		// Create a map to track transactions by block
		txsByBlock := make(map[tongo.BlockID][]*core.Transaction)

		// First pass: group transactions by block
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var tx core.Transaction
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &tx)
			})
			if err != nil {
				continue
			}
			txsByBlock[tx.BlockID] = append(txsByBlock[tx.BlockID], &tx)
		}

		// Process blocks and their transactions
		for blockID, txs := range txsByBlock {
			// Convert transactions to lite format
			for _, tx := range txs {
				liteTx := core.ConvertToLiteTransaction(tx)
				liteKey := append([]byte(ltxKeyPrefix), liteTx.Hash[:]...)
				liteData, err := json.Marshal(liteTx)
				if err != nil {
					continue
				}

				if err := txn.Set(liteKey, liteData); err != nil {
					continue
				}
				if err := txn.Delete(append([]byte(txKeyPrefix), tx.Hash[:]...)); err != nil {
					s.logger.Error("failed to delete old transaction", zap.Error(err))
				}
			}

			// Delete the block
			key := append([]byte(blockKeyPrefix), []byte(blockID.String())...)
			if err := txn.Delete(key); err != nil {
				s.logger.Error("failed to delete old block", zap.Error(err))
			}
		}

		return nil
	})
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
