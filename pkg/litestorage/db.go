package litestorage

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

// dbOperation represents a database transaction operation
type dbOperation func(txn *badger.Txn) error

// dbConfig holds database configuration
type dbConfig struct {
    maxRetries      int
    baseDelay       time.Duration
    maxDelay        time.Duration
    valueLogGCRatio float64
}

// defaultDBConfig returns default configuration for database operations
func defaultDBConfig() dbConfig {
    return dbConfig{
        maxRetries:      3,
        baseDelay:       10 * time.Millisecond,
        maxDelay:        1 * time.Second,
        valueLogGCRatio: 0.5,
    }
}

// withRetry executes database operations with exponential backoff retry
func (s *LiteStorage) withRetry(operation dbOperation) error {
    config := defaultDBConfig()
    var lastErr error
    
    for i := 0; i < config.maxRetries; i++ {
        // Track operation metrics
        start := time.Now()
        err := s.db.Update(func(txn *badger.Txn) error {
            return operation(txn)
        })
        s.metrics.dbLatency.WithLabelValues("write").Observe(time.Since(start).Seconds())
        
        if err == nil {
            s.metrics.dbOperations.WithLabelValues("write", "success").Inc()
            return nil
        }
        
        s.metrics.dbOperations.WithLabelValues("write", "error").Inc()
        
        if err == badger.ErrConflict {
            delay := config.baseDelay * time.Duration(1<<uint(i))
            if delay > config.maxDelay {
                delay = config.maxDelay
            }
            time.Sleep(delay)
            lastErr = err
            continue
        }
        
        return fmt.Errorf("database operation error: %w", err)
    }
    
    return fmt.Errorf("operation failed after %d retries: %w", config.maxRetries, lastErr)
}

// withReadTxn executes read-only operations
func (s *LiteStorage) withReadTxn(operation dbOperation) error {
    start := time.Now()
    err := s.db.View(func(txn *badger.Txn) error {
        return operation(txn)
    })
    s.metrics.dbLatency.WithLabelValues("read").Observe(time.Since(start).Seconds())
    
    if err != nil {
        s.metrics.dbOperations.WithLabelValues("read", "error").Inc()
        return fmt.Errorf("read transaction error: %w", err)
    }
    
    s.metrics.dbOperations.WithLabelValues("read", "success").Inc()
    return nil
}

// runGC runs garbage collection for BadgerDB
func (s *LiteStorage) runGC(ctx context.Context) {
    config := defaultDBConfig()
    ticker := time.NewTicker(10 * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            err := s.db.RunValueLogGC(config.valueLogGCRatio)
            if err != nil && err != badger.ErrNoRewrite {
                s.logger.Error("failed to run value log GC", zap.Error(err))
            }
        }
    }
}

// Close gracefully shuts down the storage
func (s *LiteStorage) Close() error {
    // Signal shutdown
    close(s.stopCh)
    
    // Wait for background operations with timeout
    done := make(chan struct{})
    go func() {
        s.wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
    case <-time.After(30 * time.Second):
        s.logger.Warn("timeout waiting for background operations")
    }
    
    // Clear caches
    s.clearCaches()
    
    // Close DB
    if err := s.db.Close(); err != nil {
        s.logger.Error("failed to close database", zap.Error(err))
        return fmt.Errorf("close database: %w", err)
    }
    
    return nil
}

// Example usage methods:

// Get retrieves a value from the database
func (s *LiteStorage) Get(key []byte) ([]byte, error) {
    var value []byte
    err := s.withReadTxn(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err == badger.ErrKeyNotFound {
            return nil
        }
        if err != nil {
            return err
        }
        value, err = item.ValueCopy(nil)
        return err
    })
    return value, err
}

// Set stores a value in the database
func (s *LiteStorage) Set(key, value []byte) error {
    return s.withRetry(func(txn *badger.Txn) error {
        return txn.Set(key, value)
    })
}

// Delete removes a key from the database
func (s *LiteStorage) Delete(key []byte) error {
    return s.withRetry(func(txn *badger.Txn) error {
        return txn.Delete(key)
    })
}

type BatchOperation struct {
    Key    []byte
    Value  []byte
    Delete bool
}

func (s *LiteStorage) WriteBatch(ops []BatchOperation) error {
    const batchSize = 100
    
    for i := 0; i < len(ops); i += batchSize {
        end := i + batchSize
        if end > len(ops) {
            end = len(ops)
        }
        
        err := s.db.Update(func(txn *badger.Txn) error {
            for _, op := range ops[i:end] {
                var err error
                if op.Delete {
                    err = txn.Delete(op.Key)
                } else {
                    err = txn.Set(op.Key, op.Value)
                }
                if err != nil {
                    return fmt.Errorf("batch operation failed: %w", err)
                }
            }
            return nil
        })
        if err != nil {
            return err
        }
    }
    return nil
}

func (s *LiteStorage) BatchWrite(operations []BatchOperation) error {
    start := time.Now()
    wb := s.db.NewWriteBatch()
    defer wb.Cancel()
    
    for _, op := range operations {
        var err error
        if op.Delete {
            err = wb.Delete(op.Key)
        } else {
            err = wb.Set(op.Key, op.Value)
        }
        if err != nil {
            s.metrics.dbOperations.WithLabelValues("batch_write", "error").Inc()
            return fmt.Errorf("batch operation failed: %w", err)
        }
    }
    
    if err := wb.Flush(); err != nil {
        s.metrics.dbOperations.WithLabelValues("batch_write", "error").Inc()
        return fmt.Errorf("flush batch: %w", err)
    }
    
    s.metrics.dbLatency.WithLabelValues("batch_write").Observe(time.Since(start).Seconds())
    s.metrics.dbOperations.WithLabelValues("batch_write", "success").Inc()
    return nil
}