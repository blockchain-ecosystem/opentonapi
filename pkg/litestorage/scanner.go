package litestorage

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/tonkeeper/tongo"
	"go.uber.org/zap"
)

type AccountScanner struct {
	storage *LiteStorage
	logger  *zap.Logger
	workers chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewAccountScanner(storage *LiteStorage, logger *zap.Logger, workerPoolSize int) *AccountScanner {
	return &AccountScanner{
		storage: storage,
		logger:  logger,
		workers: make(chan struct{}, workerPoolSize),
		stopCh:  make(chan struct{}),
	}
}

func (s *AccountScanner) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				if err := s.scanAccounts(ctx); err != nil {
					s.logger.Error("scan accounts failed", zap.Error(err))
				}
			}
		}
	}()
}

func (s *AccountScanner) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *AccountScanner) scanAccounts(ctx context.Context) error {
	return s.storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("account:")
		it := txn.NewIterator(opts)
		defer it.Close()

		var wg sync.WaitGroup
		errCh := make(chan error, 1)
		
		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stopCh:
				return nil
			case err := <-errCh:
				return err
			default:
				key := append([]byte{}, it.Item().Key()...)
				wg.Add(1)
				s.workers <- struct{}{} // Acquire worker
				
				go func() {
					defer func() {
						<-s.workers // Release worker
						wg.Done()
					}()
					
					if err := s.processAccountSafe(ctx, key); err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}()
			}
		}

		go func() {
			wg.Wait()
			close(errCh)
		}()

		if err := <-errCh; err != nil {
			return fmt.Errorf("scan accounts: %w", err)
		}
		return nil
	})
}

func (s *AccountScanner) processAccountSafe(ctx context.Context, key []byte) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in process account",
				zap.Any("recover", r),
				zap.String("stack", string(debug.Stack())))
		}
	}()

	account := parseAccountFromKey(key)
	if account == (tongo.AccountID{}) {
		return nil
	}

	if err := s.processAccount(ctx, account); err != nil {
		s.logger.Error("failed to process account",
			zap.Error(err),
			zap.String("account", account.String()))
		s.storage.metrics.dbOperations.WithLabelValues("process_account", "error").Inc()
		return err
	}
	
	s.storage.metrics.dbOperations.WithLabelValues("process_account", "success").Inc()
	return nil
}

func parseAccountFromKey(key []byte) tongo.AccountID {
	// Assuming key format is "account:{workchain}:{address}"
	parts := bytes.Split(key, []byte(":"))
	if len(parts) != 3 || string(parts[0]) != "account" {
		return tongo.AccountID{}
	}
	
	workchain, _ := strconv.Atoi(string(parts[1]))
	var address [32]byte
	copy(address[:], parts[2])
	return tongo.AccountID{
		Workchain: int32(workchain),
		Address:   address,
	}
}

func (s *AccountScanner) processAccount(ctx context.Context, account tongo.AccountID) error {
	// Process account transactions and update storage
	if err := s.storage.ReindexAccount(ctx, account); err != nil {
		return fmt.Errorf("failed to reindex account: %w", err)
	}
	
	s.storage.metrics.accountsProcessed.Inc()
	return nil
} 