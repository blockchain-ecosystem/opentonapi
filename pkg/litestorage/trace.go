package litestorage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/avast/retry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/abi"
	"github.com/tonkeeper/tongo/boc"
	"github.com/tonkeeper/tongo/tlb"
	"go.uber.org/zap"

	"github.com/dgraph-io/badger/v4"
	"github.com/tonkeeper/opentonapi/pkg/core"
)

const (
	maxDepthLimit = 1024
)

var (
	emulatedAccountCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tonapi_emulated_account_code_litestorage_counter",
	}, []string{"code_hash"})
)

func (s *LiteStorage) GetTrace(ctx context.Context, hash tongo.Bits256) (*core.Trace, error) {
	s.logger.Info("getting trace",
		zap.String("hash", hash.Hex()))

	if s == nil {
		return nil, fmt.Errorf("storage is nil")
	}
	if s.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}
	if s.client == nil {
		return nil, fmt.Errorf("lite client not initialized")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_trace").Observe(v)
	}))
	defer timer.ObserveDuration()
	tx, err := s.GetTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	root, err := s.findRoot(ctx, tx, 0)
	if err != nil {
		return nil, err
	}
	trace, err := s.recursiveGetChildren(ctx, *root, 0)
	return &trace, err
}

func (s *LiteStorage) SearchTraces(ctx context.Context, a tongo.AccountID, limit int, beforeLT, startTime, endTime *int64, initiator bool) ([]core.TraceID, error) {
	return nil, nil
}

func (s *LiteStorage) recursiveGetChildren(ctx context.Context, tx core.Transaction, depth int) (core.Trace, error) {
	if depth > maxDepthLimit {
		return core.Trace{}, fmt.Errorf("max depth limit reached")
	}

	if ctx.Err() != nil {
		return core.Trace{}, ctx.Err()
	}

	trace := core.Trace{Transaction: tx}
	externalMessages := make([]core.Message, 0, len(tx.OutMsgs))

	for _, m := range tx.OutMsgs {
		if m.Destination == nil {
			externalMessages = append(externalMessages, m)
			continue
		}

		childTx, err := s.searchTransactionNearBlock(ctx, *m.Destination, m.CreatedLt, tx.BlockID, false, depth+1)
		if err != nil {
			return core.Trace{}, fmt.Errorf("failed to find child tx: %w", err)
		}

		child, err := s.recursiveGetChildren(ctx, *childTx, depth+1)
		if err != nil {
			return core.Trace{}, err
		}
		trace.Children = append(trace.Children, &child)
	}

	interfaces, err := s.getAccountInterfaces(ctx, tx.Account)
	if err != nil {
		return core.Trace{}, fmt.Errorf("failed to get interfaces: %w", err)
	}

	trace.AccountInterfaces = interfaces
	trace.OutMsgs = externalMessages
	return trace, nil
}

func (s *LiteStorage) findRoot(ctx context.Context, tx *core.Transaction, depth int) (*core.Transaction, error) {
	if depth >= maxDepthLimit {
		return nil, fmt.Errorf("max recursion depth reached (%d)", maxDepthLimit)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if tx == nil {
		return nil, fmt.Errorf("can't find root of nil transaction")
	}

	if tx.InMsg == nil || tx.InMsg.IsExternal() || tx.InMsg.IsEmission() {
		return tx, nil
	}

	parentTx, err := s.searchTransactionNearBlock(ctx, *tx.InMsg.Source, tx.InMsg.CreatedLt, tx.BlockID, true, depth)
	if err != nil {
		return nil, fmt.Errorf("failed to find parent transaction: %w", err)
	}

	if parentTx == nil {
		return tx, nil
	}

	return s.findRoot(ctx, parentTx, depth+1)
}

func (s *LiteStorage) searchTransactionNearBlock(ctx context.Context, a tongo.AccountID, lt uint64, blockID tongo.BlockID, back bool, depth int) (*core.Transaction, error) {
	if depth > maxDepthLimit {
		return nil, fmt.Errorf("can't find tx because of depth limit")
	}
	tx := s.searchTxInCache(a, lt)
	if tx != nil {
		return tx, nil
	}
	tx, err := s.searchTransactionInBlock(ctx, a, lt, blockID, back)
	if err != nil {
		if back {
			blockID.Seqno--
		} else {
			blockID.Seqno++
		}
		tx, err = s.searchTransactionInBlock(ctx, a, lt, blockID, back)
		if err != nil {
			return nil, err
		}

	}
	return tx, nil
}

func (s *LiteStorage) searchTransactionInBlock(ctx context.Context, a tongo.AccountID, lt uint64, blockID tongo.BlockID, back bool) (*core.Transaction, error) {
	s.txMutex.RLock()
	defer s.txMutex.RUnlock()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	var blockIDExt tongo.BlockIDExt
	var block *tlb.Block

	err := retry.Do(func() error {
		var err error
		blockIDExt, _, err = s.client.LookupBlock(ctx, blockID, 1, nil, nil)
		if err != nil {
			return err
		}

		if b, prs := s.blockCache.Load(blockIDExt); prs {
			block = b
			return nil
		}

		b, err := s.client.GetBlock(ctx, blockIDExt)
		if err != nil {
			return err
		}
		block = &b
		s.blockCache.Store(blockIDExt, block)
		return nil
	},
		retry.Attempts(3),
		retry.Delay(time.Second),
		retry.DelayType(retry.BackOffDelay))

	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	for _, tx := range block.AllTransactions() {
		if tx.AccountAddr != a.Address {
			continue
		}

		s.txMutex.Lock()
		transaction, err := safeConvertTransaction(a.Workchain, tongo.Transaction{
			BlockID:     blockIDExt,
			Transaction: *tx,
		}, nil)
		s.txMutex.Unlock()

		if err != nil {
			s.logger.Error("failed to process transaction", zap.Error(err))
			continue
		}

		if matchTransaction(transaction, lt, back) {
			return transaction, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (s *LiteStorage) getAccountInterfaces(ctx context.Context, id tongo.AccountID) ([]abi.ContractInterface, error) {
	interfaces, ok := s.accountInterfacesCache.Load(id)
	if ok {
		return interfaces, nil
	}
	account, err := s.GetRawAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(account.Code) > 0 {
		cells, err := boc.DeserializeBoc(account.Code)
		if err != nil {
			return nil, err
		}
		if len(cells) == 0 {
			return nil, fmt.Errorf("failed to find a root cell")
		}
		h, err := cells[0].HashString()
		if err != nil {
			return nil, err
		}
		emulatedAccountCode.WithLabelValues(h).Inc()
	}
	inspector := abi.NewContractInspector(abi.InspectWithLibraryResolver(s))
	cd, err := inspector.InspectContract(ctx, account.Code, s.executor, id)
	if err != nil {
		return nil, err
	}
	interfaces = cd.ContractInterfaces
	s.accountInterfacesCache.Store(id, interfaces)
	return interfaces, nil
}

func matchTransaction(tx *core.Transaction, lt uint64, back bool) bool {
	if tx == nil {
		return false
	}
	if !back && tx.InMsg != nil && tx.InMsg.CreatedLt == lt {
		return true
	}
	if back {
		for _, m := range tx.OutMsgs {
			if m.CreatedLt == lt {
				return true
			}
		}
	}
	return false
}

func (s *LiteStorage) getTransactionWithRetry(ctx context.Context, hash tongo.Bits256) (*core.Transaction, error) {
	s.logger.Debug("attempting to get transaction from DB",
		zap.String("hash", hash.Hex()))

	s.txMutex.RLock()
	defer s.txMutex.RUnlock()

	var tx *core.Transaction
	err := s.db.View(func(txn *badger.Txn) error {
		key := append([]byte("tx:"), hash[:]...)
		item, err := txn.Get(key)
		if err != nil {
			s.logger.Debug("transaction not found in DB",
				zap.String("hash", hash.Hex()),
				zap.Error(err))
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &tx)
		})
	})

	if err != nil {
		s.logger.Info("fetching transaction from chain",
			zap.String("hash", hash.Hex()))
		return s.fetchTransactionFromChain(ctx, hash)
	}

	s.logger.Debug("transaction found in DB",
		zap.String("hash", hash.Hex()))
	return tx, nil
}

func (s *LiteStorage) fetchTransactionFromChain(ctx context.Context, hash tongo.Bits256) (*core.Transaction, error) {
	s.logger.Info("searching transaction in blockchain",
		zap.String("hash", hash.Hex()))

	client, release := s.getClient()
	if client == nil {
		return nil, fmt.Errorf("failed to get lite client")
	}
	defer release()

	// Get latest masterchain info
	info, err := client.GetMasterchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get masterchain info: %w", err)
	}

	// Start from latest block and search backwards
	blockID := info.Last.ToBlockIdExt()

	// Search through recent blocks
	for i := 0; i < 1000; i++ {
		s.logger.Debug("searching block",
			zap.String("tx_hash", hash.Hex()),
			zap.String("block_id", blockID.String()),
			zap.Int("attempt", i+1))

		block, err := client.GetBlock(ctx, blockID)
		if err != nil {
			return nil, fmt.Errorf("failed to get block: %w", err)
		}

		// Search transactions in block
		for _, tx := range block.AllTransactions() {
			txHash := tongo.Bits256(tx.Hash())
			if txHash == hash {
				s.logger.Info("found transaction in block",
					zap.String("hash", hash.Hex()),
					zap.String("block_id", blockID.String()))

				transaction, err := safeConvertTransaction(blockID.Workchain, tongo.Transaction{
					BlockID:     blockID,
					Transaction: *tx,
				}, nil)
				if err != nil {
					return nil, err
				}

				if err := s.storeTransaction(hash, transaction); err != nil {
					return nil, err
				}

				return transaction, nil
			}
		}

		blockID.Seqno--
	}

	s.logger.Error("transaction not found in recent blocks",
		zap.String("hash", hash.Hex()))
	return nil, fmt.Errorf("transaction not found")
}
