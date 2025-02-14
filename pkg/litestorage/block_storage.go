package litestorage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/tlb"
	"go.uber.org/zap"
)

func (s *LiteStorage) storeBlock(blockID tongo.BlockIDExt, block *tlb.Block) error {
	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("blk:"), []byte(blockID.String())...)
		return txn.Set(key, data)
	})
}

func (s *LiteStorage) getBlock(blockID tongo.BlockIDExt) (*tlb.Block, error) {
	// Check cache first
	if block, ok := s.blockCache.Load(blockID); ok {
		return block, nil
	}

	s.blockMutex.RLock()
	defer s.blockMutex.RUnlock()

	var block tlb.Block
	err := s.db.View(func(txn *badger.Txn) error {
		key := append([]byte("blk:"), []byte(blockID.String())...)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &block)
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// Cache for future use
	s.blockCache.Store(blockID, &block)
	return &block, nil
}

func (s *LiteStorage) fetchBlockFromChain(ctx context.Context, blockID tongo.BlockIDExt) (*tlb.Block, error) {
	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block from chain: %w", err)
	}

	if err := s.storeBlock(blockID, &block); err != nil {
		s.logger.Error("failed to store fetched block",
			zap.String("block_id", blockID.String()),
			zap.Error(err))
	}

	return &block, nil
}
