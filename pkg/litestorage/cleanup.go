package litestorage

import (
	"context"
	"time"

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
	// Keep only last 1000 blocks
	// Implementation depends on your requirements
	return nil
}
