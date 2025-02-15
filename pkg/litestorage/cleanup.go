package litestorage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tonkeeper/opentonapi/pkg/core"
	"github.com/tonkeeper/tongo"
	"go.uber.org/zap"
)

func (s *LiteStorage) startBlockCleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.cleanOldBlocks(); err != nil {
				s.logger.Error("failed to clean old blocks", zap.Error(err))
			}
		}
	}
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
