package litestorage

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/abi"
	"github.com/tonkeeper/tongo/boc"
	"go.uber.org/zap"

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
	if s == nil {
		return nil, fmt.Errorf("storage is nil")
	}

	// s.logger.Info("starting GetTrace",
	// 	zap.String("hash", hash.Hex()))

	if s.db == nil {
		s.logger.Error("database is not initialized")
		return nil, fmt.Errorf("database is not initialized")
	}
	if s.client == nil {
		s.logger.Error("lite client not initialized")
		return nil, fmt.Errorf("lite client not initialized")
	}

	ctx, cancel := s.withTimeout(ctx, s.timeout)
	defer cancel()

	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_trace").Observe(v)
	}))
	defer timer.ObserveDuration()

	// s.logger.Info("getting transaction",
	// 	zap.String("hash", hash.Hex()))
	tx, err := s.GetTransaction(ctx, hash)
	if err != nil {
		s.logger.Error("failed to get transaction",
			zap.String("hash", hash.Hex()),
			zap.Error(err))
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// s.logger.Info("finding root transaction")
	root, err := s.findRoot(ctx, tx, 0)
	if err != nil {
		s.logger.Error("failed to find root transaction",
			zap.Error(err))
		return nil, fmt.Errorf("failed to find root transaction: %w", err)
	}

	// s.logger.Info("getting children recursively")
	trace, err := s.recursiveGetChildren(ctx, *root, 0)
	if err != nil {
		s.logger.Error("failed to get children recursively",
			zap.Error(err))
		return nil, fmt.Errorf("failed to get children recursively: %w", err)
	}

	// s.logger.Info("GetTrace completed successfully")
	return &trace, nil
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

	s.logger.Info("starting transaction search",
		zap.String("account", a.String()),
		zap.Uint64("lt", lt),
		zap.String("block", blockID.String()),
		zap.Bool("back", back),
		zap.Int("depth", depth))

	// Try cache first
	tx := s.searchTxInCache(a, lt)
	if tx != nil {
		s.logger.Info("found transaction in cache",
			zap.String("account", a.String()),
			zap.Uint64("lt", lt))
		return tx, nil
	}

	// Get current masterchain info
	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get masterchain info: %w", err)
	}

	// Convert shard block seqno to masterchain seqno
	masterSeqno := blockID.Seqno
	if blockID.Workchain != -1 || blockID.Shard != 0x8000000000000000 {
		s.logger.Info("converting shard block to masterchain seqno",
			zap.String("block", blockID.String()))

		header, err := s.GetBlockHeader(ctx, blockID)
		if err != nil {
			return nil, fmt.Errorf("failed to get block header: %w", err)
		}
		masterSeqno = header.MasterRef.Seqno
		s.logger.Info("converted to masterchain seqno",
			zap.Uint32("master_seqno", masterSeqno))
	}

	// Ensure we don't exceed bounds
	if masterSeqno > info.Last.Seqno {
		s.logger.Info("adjusting seqno to last known masterchain block",
			zap.Uint32("from", masterSeqno),
			zap.Uint32("to", info.Last.Seqno))
		masterSeqno = info.Last.Seqno
	}

	// Search in masterchain blocks
	const searchRange = 10
	for i := 0; i < searchRange; i++ {
		seqno := masterSeqno
		if back {
			seqno -= uint32(i)
		} else {
			seqno += uint32(i)
		}

		s.logger.Info("searching in masterchain block",
			zap.Uint32("seqno", seqno))

		transactions, err := s.GetMasterchainTransactions(ctx, int32(seqno))
		if err != nil {
			s.logger.Error("failed to get masterchain transactions",
				zap.Uint32("seqno", seqno),
				zap.Error(err))
			continue
		}

		s.logger.Info("checking transactions in block",
			zap.Uint32("seqno", seqno),
			zap.Int("tx_count", len(transactions)))

		for _, tx := range transactions {
			if tx.Account == a && matchTransaction(&tx, lt, back) {
				s.logger.Info("found matching transaction",
					zap.String("account", a.String()),
					zap.Uint64("lt", lt),
					zap.String("hash", tx.Hash.Hex()))
				return &tx, nil
			}
		}
	}

	s.logger.Info("transaction not found",
		zap.String("account", a.String()),
		zap.Uint64("lt", lt))
	return nil, fmt.Errorf("not found")
}

func (s *LiteStorage) searchTransactionInBlock(ctx context.Context, a tongo.AccountID, lt uint64, blockID tongo.BlockID, back bool) (*core.Transaction, error) {
	// Get block first without holding txMutex
	blockIDExt, block, err := s.getBlockWithRetry(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	s.txMutex.RLock()
	defer s.txMutex.RUnlock()

	for _, tx := range block.AllTransactions() {
		if tx.AccountAddr != a.Address {
			continue
		}

		transaction, err := safeConvertTransaction(a.Workchain, tongo.Transaction{
			BlockID:     blockIDExt,
			Transaction: *tx,
		}, nil)
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
