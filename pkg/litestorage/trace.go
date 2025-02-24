package litestorage

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// Add this type to define trace options

func (s *LiteStorage) GetTrace(ctx context.Context, hash tongo.Bits256) (*core.Trace, error) {
	traceID := hash.Hex()

	// Try cache first
	if cached, ok := s.traceCache.Load(traceID); ok {
		return cached.(*core.Trace), nil
	}

	start := time.Now()
	// s.logger.Info("trace request started",
	// 	zap.String("trace_id", traceID),
	// 	zap.String("operation", "GetTrace"))

	if s == nil {
		return nil, fmt.Errorf("storage is nil")
	}

	if s.db == nil {
		s.logger.Error("database is not initialized")
		return nil, fmt.Errorf("database is not initialized")
	}
	if s.client == nil {
		s.logger.Error("lite client not initialized")
		return nil, fmt.Errorf("lite client not initialized")
	}

	// Get initial transaction
	tx, err := s.GetTransaction(ctx, hash)
	if err != nil {
		if errors.Is(err, core.ErrEntityNotFound) {
			s.logger.Debug("transaction not found",
				zap.String("trace_id", traceID))
			return nil, core.ErrEntityNotFound // This will be converted to 404
		}
		s.logger.Error("transaction fetch failed",
			zap.String("trace_id", traceID),
			zap.Error(err))
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	s.logger.Debug("transaction found", // Debug level for successful operations
		zap.String("trace_id", traceID),
		zap.String("account", tx.Account.String()),
		zap.Uint64("lt", tx.Lt))

	s.logger.Info("trace processing completed",
		zap.String("trace_id", traceID),
		zap.Duration("duration", time.Since(start)))

	// If no options provided or just want transaction, return early
	// if opts == nil || (!opts.FindRoot && !opts.FindChildren) {
	// 	return &core.Trace{
	// 		Transaction: *tx,
	// 		Children:    []*core.Trace{},
	// 	}, nil
	// }

	// Use shorter timeout for subsequent operations
	shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Find root with logging
	root, err := s.findRoot(shortCtx, tx, 0)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Debug("timeout finding root, using current transaction",
				zap.String("trace_id", traceID))
			root = tx
		} else {
			s.logger.Warn("using current transaction as root due to error",
				zap.String("trace_id", traceID),
				zap.Error(err))
			root = tx
		}
	}

	// Get children with shorter timeout
	trace, err := s.recursiveGetChildren(shortCtx, *root, 0)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Debug("timeout getting children, returning root only",
				zap.String("trace_id", traceID))
		} else {
			s.logger.Warn("failed to get children, returning root only",
				zap.String("trace_id", traceID),
				zap.Error(err))
		}

		// Return root transaction with empty children
		return &core.Trace{
			Transaction: *root,
			Children:    []*core.Trace{},
		}, nil
	}

	result := &trace
	s.traceCache.Store(traceID, result)
	return result, nil
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

		childTx, err := s.searchTransactionNearBlockV2(ctx, *m.Destination, m.CreatedLt, tx.BlockID, false, depth+1)
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

	s.logger.Info("finding root transaction",
		zap.String("tx_hash", tx.Hash.Hex()),
		zap.String("account", tx.Account.String()),
		zap.Uint64("lt", tx.Lt),
		zap.Int("depth", depth))

	if tx.InMsg == nil || tx.InMsg.IsExternal() || tx.InMsg.IsEmission() {
		s.logger.Info("found root transaction - external or emission",
			zap.String("tx_hash", tx.Hash.Hex()))
		return tx, nil
	}

	s.logger.Info("searching parent transaction",
		zap.String("source_account", tx.InMsg.Source.String()),
		zap.Uint64("created_lt", tx.InMsg.CreatedLt))

	parentTx, err := s.searchTransactionNearBlockV2(ctx, *tx.InMsg.Source, tx.InMsg.CreatedLt, tx.BlockID, true, depth)
	if err != nil {
		return nil, fmt.Errorf("failed to find parent transaction: %w", err)
	}

	if parentTx == nil {
		s.logger.Info("no parent found, using current as root",
			zap.String("tx_hash", tx.Hash.Hex()))
		return tx, nil
	}

	s.logger.Info("found parent transaction",
		zap.String("parent_hash", parentTx.Hash.Hex()),
		zap.String("parent_account", parentTx.Account.String()),
		zap.Uint64("parent_lt", parentTx.Lt))

	return s.findRoot(ctx, parentTx, depth+1)
}

func (s *LiteStorage) searchTransactionNearBlock(ctx context.Context, a tongo.AccountID, lt uint64, blockID tongo.BlockID, back bool, depth int) (*core.Transaction, error) {
	if depth > maxDepthLimit {
		return nil, fmt.Errorf("can't find tx because of depth limit")
	}

	// Log search attempt with more details
	s.logger.Info("searching transaction near block",
		zap.String("account", a.String()),
		zap.Uint64("lt", lt),
		zap.String("block", blockID.String()),
		zap.Bool("back", back),
		zap.Int("depth", depth),
		zap.String("search_id", fmt.Sprintf("%s_%d", a.String(), lt))) // Add unique search identifier

	// Try cache first
	tx := s.searchTxInStorage(a, lt)
	if tx != nil {
		s.logger.Info("transaction found in cache", // Reduced to debug level
			zap.String("account", a.String()),
			zap.Uint64("lt", lt))
		return tx, nil
	}

	// Try current block
	tx, err := s.searchTransactionInBlock(ctx, a, lt, blockID, back)

	// Try next/previous block
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

func (s *LiteStorage) searchTransactionNearBlockV2(ctx context.Context, a tongo.AccountID, lt uint64, blockID tongo.BlockID, back bool, depth int) (*core.Transaction, error) {
	if depth > maxDepthLimit {
		return nil, fmt.Errorf("can't find tx because of depth limit")
	}

	// Log search attempt with more details
	s.logger.Info("searching transaction near block",
		zap.String("account", a.String()),
		zap.Uint64("lt", lt),
		zap.String("block", blockID.String()),
		zap.Bool("back", back),
		zap.Int("depth", depth),
		zap.String("search_id", fmt.Sprintf("%s_%d", a.String(), lt))) // Add unique search identifier

	// Try cache first
	tx := s.searchTxInStorage(a, lt)
	if tx != nil {
		s.logger.Info("transaction found in cache", // Reduced to debug level
			zap.String("account", a.String()),
			zap.Uint64("lt", lt))
		return tx, nil
	}

	// Get current masterchain info
	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		s.logger.Error("masterchain info fetch failed",
			zap.Error(err),
			zap.String("search_id", fmt.Sprintf("%s_%d", a.String(), lt)))
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

	// Log only significant state changes
	if blockID.Workchain != -1 || blockID.Shard != 0x8000000000000000 {
		s.logger.Info("converting shard block",
			zap.String("block", blockID.String()),
			zap.String("search_id", fmt.Sprintf("%s_%d", a.String(), lt)))
	}

	// Rest of the function remains the same, but we'll add trace points
	s.logger.Info("search parameters", // Debug level for detailed info
		zap.Uint32("master_seqno", masterSeqno),
		zap.Uint32("last_seqno", info.Last.Seqno),
		zap.String("search_id", fmt.Sprintf("%s_%d", a.String(), lt)))

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
	blockIDExt, _, err := s.client.LookupBlock(ctx, blockID, 1, nil, nil)
	if err != nil {
		return nil, err
	}

	s.logger.Info("searching transaction in block",
		zap.String("block_id", blockIDExt.String()),
		zap.String("account", a.String()),
		zap.Uint64("lt", lt),
		zap.Bool("back", back))

	block, prs := s.blockCache.Load(blockIDExt)
	if !prs {
		b, err := s.client.GetBlock(ctx, blockIDExt)
		if err != nil {
			return nil, err
		}
		s.blockCache.Store(blockIDExt, &b)
		block = &b
	}

	txs := block.AllTransactions()
	s.logger.Info("found transactions in block",
		zap.String("block_id", blockIDExt.String()),
		zap.Int("transaction_count", len(txs)))

	for _, tx := range txs {
		if tx.AccountAddr != a.Address {
			continue
		}
		inMsg := tx.Msgs.InMsg
		if !back && inMsg.Exists && inMsg.Value.Value.Info.IntMsgInfo != nil && inMsg.Value.Value.Info.IntMsgInfo.CreatedLt == lt {
			s.logger.Debug("found matching transaction by incoming message",
				zap.String("block_id", blockIDExt.String()),
				zap.String("account", a.String()),
				zap.Uint64("lt", lt))
			return core.ConvertTransaction(a.Workchain, tongo.Transaction{BlockID: blockIDExt, Transaction: *tx}, nil)
		}
		if back {
			for _, m := range tx.Msgs.OutMsgs.Values() {
				if m.Value.Info.IntMsgInfo != nil && m.Value.Info.IntMsgInfo.CreatedLt == lt {
					s.logger.Debug("found matching transaction by outgoing message",
						zap.String("block_id", blockIDExt.String()),
						zap.String("account", a.String()),
						zap.Uint64("lt", lt))
					return core.ConvertTransaction(a.Workchain, tongo.Transaction{BlockID: blockIDExt, Transaction: *tx}, nil)
				}
			}
		}
	}

	s.logger.Debug("no matching transaction found in block",
		zap.String("block_id", blockIDExt.String()),
		zap.String("account", a.String()),
		zap.Uint64("lt", lt))
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
