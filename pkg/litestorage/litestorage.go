package litestorage

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/labstack/gommon/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/puzpuzpuz/xsync/v2"
	"github.com/sourcegraph/conc/iter"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/abi"
	"github.com/tonkeeper/tongo/boc"
	"github.com/tonkeeper/tongo/liteapi"
	"github.com/tonkeeper/tongo/tep64"
	"github.com/tonkeeper/tongo/tlb"
	"github.com/tonkeeper/tongo/ton"
	"go.uber.org/zap"

	"encoding/hex"

	"hash/maphash"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/tonkeeper/opentonapi/pkg/blockchain/indexer"
	"github.com/tonkeeper/opentonapi/pkg/cache"
	"github.com/tonkeeper/opentonapi/pkg/core"
)

var storageTimeHistogramVec = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "litestorage_functions_time",
		Help:    "LiteStorage functions execution duration distribution in seconds",
		Buckets: []float64{0.001, 0.01, 0.05, 0.1, 1, 5, 10},
	},
	[]string{"method"},
)

// inMsgCreatedLT is used as a key to look up a transaction's hash based on in msg's account and created lt.
type inMsgCreatedLT struct {
	account tongo.AccountID
	lt      uint64
}

func extractInMsgCreatedLT(accountID tongo.AccountID, tx *tlb.Transaction) (inMsgCreatedLT, bool) {
	if tx.Msgs.InMsg.Exists && tx.Msgs.InMsg.Value.Value.Info.IntMsgInfo != nil {
		return inMsgCreatedLT{account: accountID, lt: tx.Msgs.InMsg.Value.Value.Info.IntMsgInfo.CreatedLt}, true
	}
	return inMsgCreatedLT{}, false
}

type CacheOptions struct {
	TTL     time.Duration
	MaxSize int
}

type storageMetrics struct {
	blockProcessingTime       prometheus.Histogram
	transactionProcessingTime prometheus.Histogram
	cleanupDuration           prometheus.Histogram
}

type LiteStorage struct {
	logger          *zap.Logger
	client          *liteapi.Client
	executor        abi.Executor
	jettonMetaCache *xsync.MapOf[string, tep64.Metadata]
	// transactionsIndexByHash *xsync.MapOf[tongo.Bits256, *core.Transaction]
	// transactionsByInMsgLT   *xsync.MapOf[inMsgCreatedLT, tongo.Bits256]
	blockCache             *xsync.MapOf[tongo.BlockIDExt, *tlb.Block]
	accountInterfacesCache *xsync.MapOf[tongo.AccountID, []abi.ContractInterface]
	// tvmLibraryCache contains public tvm libraries.
	// As a library is immutable, it's ok to cache it.
	tvmLibraryCache cache.Cache[string, boc.Cell]
	knownAccounts   map[string][]tongo.AccountID
	// maxGoroutines specifies a number of goroutines used to perform some time-consuming operations.
	maxGoroutines int
	// // trackingAccounts is a list of accounts we track. Defined with ACCOUNTS env variable.
	// trackingAccounts  map[tongo.AccountID]struct{}
	pubKeyByAccountID *xsync.MapOf[tongo.AccountID, ed25519.PublicKey]
	configCache       cache.Cache[int, ton.BlockchainConfig]

	stopCh chan struct{}
	// mu protects trimmedConfigBase64.
	mu sync.RWMutex
	// trimmedConfigBase64 is a blockchain config but with a limited set of keys.
	// it's performance optimization.
	// tmv and txEmulator work much faster with a smaller config.
	trimmedConfigBase64 string
	db                  *badger.DB
	connPool            sync.Pool
	maxConns            int
	timeout             time.Duration
	txMutex             sync.RWMutex
	blockMutex          sync.RWMutex
	blockRetryCount     int
	blockRetryDelay     time.Duration
	lastProcessedSeqno  uint32
	seqnoMutex          sync.RWMutex
	blockQueue          *BlockQueue
	cleanupInterval     time.Duration
	maxBatchSize        int
	connPoolMetrics     struct {
		active    prometheus.Gauge
		available prometheus.Gauge
	}
	metrics      *storageMetrics
	cleanupMutex sync.Mutex
}

type BlockQueue struct {
	blocks    []indexer.IDandBlock
	processed *xsync.MapOf[tongo.BlockIDExt, bool]
	mu        sync.RWMutex
}

func NewBlockQueue() *BlockQueue {
	return &BlockQueue{
		blocks: make([]indexer.IDandBlock, 0, 1000),
		processed: xsync.NewTypedMapOf[tongo.BlockIDExt, bool](func(_ maphash.Seed, v tongo.BlockIDExt) uint64 {
			return uint64(v.Seqno)
		}),
	}
}

func (q *BlockQueue) Add(block indexer.IDandBlock) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.IsProcessed(block.ID) {
		return
	}

	// Find insertion point to maintain order
	idx := sort.Search(len(q.blocks), func(i int) bool {
		return q.blocks[i].ID.Seqno >= block.ID.Seqno
	})

	// Insert block at correct position
	if idx == len(q.blocks) {
		q.blocks = append(q.blocks, block)
	} else if q.blocks[idx].ID.Seqno != block.ID.Seqno {
		q.blocks = append(q.blocks[:idx+1], q.blocks[idx:]...)
		q.blocks[idx] = block
	}
}

func (q *BlockQueue) IsProcessed(id tongo.BlockIDExt) bool {
	val, exists := q.processed.Load(id)
	return exists && val
}

func (q *BlockQueue) MarkProcessed(id tongo.BlockIDExt) {
	q.processed.Store(id, true)
	go q.cleanup()
}

func (q *BlockQueue) cleanup() {
	q.mu.Lock()
	defer q.mu.Unlock()

	newBlocks := make([]indexer.IDandBlock, 0, len(q.blocks))
	for _, block := range q.blocks {
		if !q.IsProcessed(block.ID) {
			newBlocks = append(newBlocks, block)
		}
	}
	q.blocks = newBlocks
}

func (q *BlockQueue) GetBlocks() []indexer.IDandBlock {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]indexer.IDandBlock, len(q.blocks))
	copy(result, q.blocks)
	return result
}

func (q *BlockQueue) GetOrderedBlocks() []indexer.IDandBlock {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Return a copy of the blocks slice to prevent race conditions
	orderedBlocks := make([]indexer.IDandBlock, len(q.blocks))
	copy(orderedBlocks, q.blocks)
	return orderedBlocks
}

type Options struct {
	preloadAccounts []tongo.AccountID
	preloadBlocks   []tongo.BlockID
	tfPools         []tongo.AccountID
	jettons         []tongo.AccountID
	executor        abi.Executor
	// blockCh is used to receive new blocks in the blockchain, if set.
	blockCh <-chan indexer.IDandBlock
}

func WithPreloadAccounts(a []tongo.AccountID) Option {
	return func(o *Options) {
		o.preloadAccounts = a
	}
}

func WithPreloadBlocks(ids []tongo.BlockID) Option {
	return func(o *Options) {
		o.preloadBlocks = ids
	}
}

func WithKnownJettons(a []tongo.AccountID) Option {
	return func(o *Options) {
		o.jettons = a
	}
}

func WithTFPools(pools []tongo.AccountID) Option {
	return func(o *Options) {
		o.tfPools = pools
	}
}

// WithBlockChannel configures a channel to receive notifications about new blocks in the blockchain.
func WithBlockChannel(ch <-chan indexer.IDandBlock) Option {
	return func(o *Options) {
		o.blockCh = ch
	}
}

type Option func(o *Options)

func NewLiteStorage(logger *zap.Logger, cli *liteapi.Client, opts ...Option) (*LiteStorage, error) {
	if cli == nil {
		return nil, fmt.Errorf("lite client cannot be nil")
	}

	// Test connection before proceeding
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := cli.GetMasterchainInfo(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to lite server: %w", err)
	}

	o := &Options{}
	for i := range opts {
		opts[i](o)
	}
	if o.executor == nil {
		o.executor = cli
	}

	badgerOpts := badger.DefaultOptions("./badger").
		WithValueLogFileSize(1 << 30).   // 1GB value logs
		WithNumVersionsToKeep(1).        // Single version
		WithCompression(options.Snappy). // Enable compression
		WithNumGoroutines(32).           // More concurrent processing
		WithValueThreshold(32).          // Optimize for larger values
		WithBlockCacheSize(32 << 30).    // 32GB block cache
		WithIndexCacheSize(64 << 30).    // 64GB index cache
		WithMemTableSize(1 << 30)        // 1GB memtable

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	s := &LiteStorage{
		logger:          logger,
		client:          cli,
		executor:        o.executor,
		stopCh:          make(chan struct{}),
		knownAccounts:   make(map[string][]tongo.AccountID),
		jettonMetaCache: xsync.NewTypedMapOf[string, tep64.Metadata](hashString),
		blockCache: xsync.NewTypedMapOf[tongo.BlockIDExt, *tlb.Block](func(_ maphash.Seed, v tongo.BlockIDExt) uint64 {
			return uint64(v.Seqno)
		}),
		accountInterfacesCache: xsync.NewTypedMapOf[tongo.AccountID, []abi.ContractInterface](hashAccountID),
		pubKeyByAccountID:      xsync.NewTypedMapOf[tongo.AccountID, ed25519.PublicKey](hashAccountID),
		tvmLibraryCache:        cache.NewLRUCache[string, boc.Cell](10000, "tvm_libraries"),
		configCache:            cache.NewLRUCache[int, ton.BlockchainConfig](4, "config"),
		db:                     db,
		maxConns:               10,
		timeout:                30 * time.Second,
		connPool: sync.Pool{
			New: func() interface{} {
				return cli // Just return the original client
			},
		},
		blockRetryCount: 3,
		blockRetryDelay: 100 * time.Millisecond,
		blockQueue:      NewBlockQueue(),
		cleanupInterval: time.Hour, // default 1 hour interval
		maxBatchSize:    1000,
	}
	s.knownAccounts["tf_pools"] = o.tfPools
	s.knownAccounts["jettons"] = o.jettons

	blockIterator := iter.Iterator[tongo.BlockID]{MaxGoroutines: s.maxGoroutines}
	blockIterator.ForEach(o.preloadBlocks, func(id *tongo.BlockID) {
		if err := s.preloadBlock(*id); err != nil {
			log.Error("failed to preload block",
				zap.String("blockID", id.String()),
				zap.Error(err))
		}
	})
	iterator := iter.Iterator[tongo.AccountID]{MaxGoroutines: s.maxGoroutines}
	iterator.ForEach(o.preloadAccounts, func(accountID *tongo.AccountID) {
		if err := s.preloadAccount(*accountID); err != nil {
			log.Error("failed to preload account",
				zap.String("accountID", accountID.String()),
				zap.Error(err))
		}
	})
	go s.run(context.Background(), o.blockCh)
	go s.runBlockchainConfigUpdate(5 * time.Second)

	// Initialize connection pool
	for i := 0; i < s.maxConns; i++ {
		s.connPool.Put(cli)
	}

	// Start background tasks
	go s.startBlockCleanup(ctx)
	// go s.startTransactionCleanup(ctx)
	// go s.startTransactionConversion(ctx)

	return s, nil
}

func (s *LiteStorage) SetExecutor(e abi.Executor) {
	s.executor = e
}

// Shutdown stops all background goroutines.
func (s *LiteStorage) Shutdown() {
	s.stopCh <- struct{}{}
}

const (
	txKeyPrefix    = "tx:"
	blockKeyPrefix = "blk:"
)

func (s *LiteStorage) storeTransaction(hash tongo.Bits256, tx *core.Transaction) error {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()

	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	key := append([]byte(txKeyPrefix), hash[:]...)
	s.logger.Info("storing transaction with key",
		zap.String("key", hex.EncodeToString(key)),
		zap.String("hash", hash.Hex()))

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (s *LiteStorage) getTransaction(hash tongo.Bits256) (*core.Transaction, error) {
	s.txMutex.RLock()
	defer s.txMutex.RUnlock()

	var tx core.Transaction
	key := append([]byte(txKeyPrefix), hash[:]...)

	// s.logger.Info("getting transaction with key",
	// 	zap.String("key", hex.EncodeToString(key)),
	// 	zap.String("hash", hash.Hex()))

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &tx)
		})
	})

	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (s *LiteStorage) StoreTransactionByInMsgLT(accountID string, createLT uint64, hash tongo.Bits256) error {
	key := []byte("lt_" + accountID + "_" + fmt.Sprint(createLT))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(key, hash[:]).WithMeta(0x01))
	})
}

func (s *LiteStorage) GetTransactionByInMsgLT(accountID string, createLT uint64) (tongo.Bits256, error) {
	var hash tongo.Bits256
	err := s.db.View(func(txn *badger.Txn) error {
		key := []byte("lt_" + accountID + "_" + fmt.Sprint(createLT))
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			copy(hash[:], val)
			return nil
		})
	})
	return hash, err
}

func (s *LiteStorage) run(ctx context.Context, ch <-chan indexer.IDandBlock) {
	for {
		select {
		case <-ctx.Done():
			return
		case block := <-ch:
			if block.Block == nil {
				continue
			}

			s.blockQueue.Add(block)
			s.processQueuedBlocks(ctx)
		}
	}
}

func (s *LiteStorage) processQueuedBlocks(ctx context.Context) {
	s.seqnoMutex.Lock()
	defer s.seqnoMutex.Unlock()

	lastSeqno, err := s.getLastProcessedSeqno()
	if err != nil {
		s.logger.Error("failed to get last processed seqno", zap.Error(err))
		return
	}
	s.lastProcessedSeqno = lastSeqno

	// Process blocks in batches
	batchSize := 100
	blocks := s.blockQueue.GetOrderedBlocks()

	for i := 0; i < len(blocks); i += batchSize {
		end := i + batchSize
		if end > len(blocks) {
			end = len(blocks)
		}

		if err := s.processBatchAtomically(ctx, blocks[i:end]); err != nil {
			s.logger.Error("failed to process batch",
				zap.Error(err),
				zap.Int("batch_start", i),
				zap.Int("batch_end", end))
			return
		}
	}
}

func (s *LiteStorage) processBatchAtomically(ctx context.Context, blocks []indexer.IDandBlock) error {
	for _, block := range blocks {
		if err := s.processBlockAtomically(block); err != nil {
			s.logger.Error("failed to process block", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *LiteStorage) processBlockAtomically(block indexer.IDandBlock) error {
	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	// Create error channel for concurrent processing
	errChan := make(chan error, 2)

	// Process master block in goroutine
	go func() {
		err := s.retryOperation(context.Background(), func(txn *badger.Txn) error {
			if err := s.storeBlockTx(txn, block.ID, block.Block); err != nil {
				return err
			}
			if err := s.processBlockTransactionsSafely(block.ID, block.Block); err != nil {
				return err
			}
			return s.updateLastProcessedSeqnoTx(txn, block.ID.Seqno)
		})
		if err != nil {
			s.logger.Error("failed to process master block",
				zap.String("block_id", block.ID.String()),
				zap.Error(err))
		}
		errChan <- err
	}()

	// Process shard blocks in goroutine
	go func() {
		err := s.processShardBlocks(context.Background(), block.ID)
		if err != nil {
			s.logger.Error("failed to process shard blocks",
				zap.String("master_block", block.ID.String()),
				zap.Error(err))
		}
		errChan <- err
	}()

	// Wait for both operations to complete
	masterErr := <-errChan
	shardErr := <-errChan

	if masterErr != nil {
		return fmt.Errorf("master block processing failed: %w", masterErr)
	}
	if shardErr != nil {
		return fmt.Errorf("shard blocks processing failed: %w", shardErr)
	}

	// Verify sequence after all processing is done
	if err := s.verifyBlockSequence(context.Background(), block.ID); err != nil {
		return fmt.Errorf("block sequence verification failed: %w", err)
	}

	return nil
}

func (s *LiteStorage) processBlockTransactionsSafely(blockID tongo.BlockIDExt, block *tlb.Block) error {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()

	// Convert transactions to batch
	batch := make([]*core.Transaction, 0, len(block.AllTransactions()))
	for _, tx := range block.AllTransactions() {
		hash := tongo.Bits256(tx.Hash())
		transaction, err := safeConvertTransaction(blockID.Workchain, tongo.Transaction{
			BlockID:     blockID,
			Transaction: *tx,
		}, nil)
		if err != nil {
			s.logger.Error("failed to convert transaction",
				zap.String("hash", hash.Hex()),
				zap.Error(err))
			continue
		}
		batch = append(batch, transaction)
	}

	// Process batch using existing method
	if err := s.storeTransactionBatch(batch); err != nil {
		return fmt.Errorf("failed to store transaction batch: %w", err)
	}

	// Store LT indices for each transaction
	return s.db.Update(func(txn *badger.Txn) error {
		for _, tx := range batch {
			if err := s.StoreTransactionByInMsgLT(tx.Account.String(), tx.Lt, tx.Hash); err != nil {
				return fmt.Errorf("failed to store LT index: %w", err)
			}
		}
		return nil
	})
}

func (s *LiteStorage) GetContract(ctx context.Context, id tongo.AccountID) (*core.Contract, error) {
	account, err := s.GetRawAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	return &core.Contract{
		Balance:           account.TonBalance,
		Status:            account.Status,
		Code:              account.Code,
		Data:              account.Data,
		Libraries:         account.Libraries,
		LastTransactionLt: account.LastTransactionLt,
	}, nil
}

// GetRawAccount returns low-level information about an account taken directly from the blockchain.
func (s *LiteStorage) GetRawAccount(ctx context.Context, address tongo.AccountID) (*core.Account, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_raw_account").Observe(v)
	}))
	defer timer.ObserveDuration()
	var account tlb.ShardAccount
	err := retry.Do(func() error {
		state, err := s.client.GetAccountState(ctx, address)
		if err != nil {
			return err
		}
		account = state
		return nil
	}, retry.Attempts(10), retry.Delay(10*time.Millisecond))

	if err != nil {
		return nil, err
	}
	return core.ConvertToAccount(address, account)
}

// GetRawAccounts returns low-level information about several accounts taken directly from the blockchain.
func (s *LiteStorage) GetRawAccounts(ctx context.Context, ids []tongo.AccountID) ([]*core.Account, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_raw_accounts").Observe(v)
	}))
	defer timer.ObserveDuration()
	var accounts []*core.Account
	for _, address := range ids {
		var account tlb.ShardAccount
		err := retry.Do(func() error {
			state, err := s.client.GetAccountState(ctx, address)
			if err != nil {
				return err
			}
			account = state
			return nil
		}, retry.Attempts(10), retry.Delay(10*time.Millisecond))
		if err != nil {
			return nil, err
		}
		acc, err := core.ConvertToAccount(address, account)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}
	return accounts, nil
}

func (s *LiteStorage) preloadAccount(a tongo.AccountID) error {
	ctx := context.Background()
	accountTxs, err := s.client.GetLastTransactions(ctx, a, 2000)
	if err != nil {
		return err
	}
	for _, tx := range accountTxs {
		inspector := abi.NewContractInspector(abi.InspectWithLibraryResolver(s))
		account, err := s.GetRawAccount(ctx, a)
		if err != nil {
			return err
		}
		cd, err := inspector.InspectContract(ctx, account.Code, s.executor, a)
		t, err := core.ConvertTransaction(a.Workchain, tx, cd)
		if err != nil {
			return err
		}
		hash := tongo.Bits256(tx.Hash())
		// s.transactionsIndexByHash.Store(hash, t)
		s.storeTransactionWithRetry(hash, t)
		createLT, ok := extractInMsgCreatedLT(a, &tx.Transaction)
		if ok {
			// s.transactionsByInMsgLT.Store(createLT, hash)
			s.StoreTransactionByInMsgLT(a.String(), createLT.lt, hash)
		}
	}
	return nil
}

func (s *LiteStorage) preloadBlock(id tongo.BlockID) error {
	ctx := context.Background()
	extID, _, err := s.client.LookupBlock(ctx, id, 1, nil, nil)
	if err != nil {
		return err
	}
	block, err := s.client.GetBlock(ctx, extID)
	if err != nil {
		return err
	}
	s.blockCache.Store(extID, &block)
	for _, tx := range block.AllTransactions() {
		accountID := tongo.AccountID{
			Workchain: extID.Workchain,
			Address:   tx.AccountAddr,
		}
		inspector := abi.NewContractInspector(abi.InspectWithLibraryResolver(s))
		account, err := s.GetRawAccount(ctx, accountID)
		if err != nil {
			return err
		}
		cd, err := inspector.InspectContract(ctx, account.Code, s.executor, accountID)
		t, err := core.ConvertTransaction(extID.Workchain, tongo.Transaction{Transaction: *tx, BlockID: extID}, cd)
		if err != nil {
			return err
		}
		hash := tongo.Bits256(tx.Hash())
		// s.transactionsIndexByHash.Store(hash, t)
		s.storeTransactionWithRetry(hash, t)
		createLT, ok := extractInMsgCreatedLT(accountID, tx)
		if ok {
			// s.transactionsByInMsgLT.Store(createLT, hash)
			s.StoreTransactionByInMsgLT(accountID.String(), createLT.lt, hash)
		}
	}
	return nil
}

func (s *LiteStorage) GetBlockHeader(ctx context.Context, id tongo.BlockID) (*core.BlockHeader, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_block_header").Observe(v)
	}))
	defer timer.ObserveDuration()
	blockID, _, err := s.client.LookupBlock(ctx, id, 1, nil, nil)
	if err != nil {
		return nil, err
	}
	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		return nil, err
	}

	s.blockCache.Store(blockID, &block)
	header, err := core.ConvertToBlockHeader(blockID, &block)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (s *LiteStorage) GetBlockShards(ctx context.Context, id tongo.BlockID) ([]ton.BlockID, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_block_shards").Observe(v)
	}))
	defer timer.ObserveDuration()
	blockID, _, err := s.client.LookupBlock(ctx, id, 1, nil, nil)
	if err != nil {
		return nil, err
	}
	shards, err := s.client.GetAllShardsInfo(ctx, blockID)
	if err != nil {
		return nil, err
	}
	res := make([]ton.BlockID, len(shards))
	for i, s := range shards {
		res[i] = s.BlockID
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Shard < shards[j].Shard
	})
	return res, nil
}

func (s *LiteStorage) LastMasterchainBlockHeader(ctx context.Context) (*core.BlockHeader, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_masterchain").Observe(v)
	}))
	defer timer.ObserveDuration()

	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		s.logger.Error("failed to get masterchain info",
			zap.Error(err))
		return nil, err
	}

	blockID := info.Last.ToBlockIdExt().BlockID
	header, err := s.GetBlockHeader(ctx, blockID)
	if err != nil {
		s.logger.Error("failed to get block header",
			zap.String("block_id", blockID.String()),
			zap.Error(err))
		return nil, err
	}

	return header, nil
}

func (s *LiteStorage) GetTransaction(ctx context.Context, hash tongo.Bits256) (*core.Transaction, error) {
	// s.logger.Info("starting transaction lookup",
	// 	zap.String("hash", hash.Hex()))

	// Try getting from DB first
	tx, err := s.getTransactionWithRetry(ctx, hash)
	if err == nil {
		// s.logger.Info("found transaction in DB")
		return tx, nil
	}

	// Try both workchains
	// workchains := []int32{0, -1} // Try workchain 0 first
	// for _, wc := range workchains {
	// s.logger.Info("searching in workchain",
	// 	zap.String("hash", hash.Hex()))

	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		// s.logger.Error("failed to get masterchain info", zap.Error(err))
		return nil, err
	}

	blockID := info.Last.ToBlockIdExt()
	s.logger.Info("searching in",
		zap.String("block_id", blockID.String()))
	// blockID.Workchain = wc

	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		// s.logger.Error("failed to get block",
		// 	zap.String("block_id", blockID.String()),
		// 	zap.Error(err))
		return nil, err
	}

	// Search in current block
	for _, tx := range block.AllTransactions() {
		if tongo.Bits256(tx.Hash()) == hash {
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
	// }

	// If not found in recent blocks, try chain search
	// s.logger.Info("transaction not found in recent blocks, trying chain search",
	// 	zap.String("hash", hash.Hex()))
	return s.fetchTransactionFromChain(ctx, hash)
}

func (s *LiteStorage) SearchTransactionByMessageHash(ctx context.Context, hash tongo.Bits256) (*tongo.Bits256, error) {
	//var txHash tongo.Bits256
	//found := false
	//s.transactionsIndexByHash.Range(func(key tongo.Bits256, value *core.Transaction) bool {
	//	if value.InMsg != nil && value.InMsg.Hash {
	//		txHash = key
	//		found =true
	//		return false
	//	}
	//	return true
	//})
	//if found {
	//	return &txHash, nil
	//}
	return nil, core.ErrEntityNotFound
}

func (s *LiteStorage) GetBlockTransactions(ctx context.Context, id tongo.BlockID) ([]*core.Transaction, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_block_transactions").Observe(v)
	}))
	defer timer.ObserveDuration()
	blockID, _, err := s.client.LookupBlock(ctx, id, 1, nil, nil)
	if err != nil {
		return nil, err
	}

	s.logger.Info("getting block transactions",
		zap.String("block_id", blockID.String()))
	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		return nil, err
	}
	return core.ExtractTransactions(s.logger, blockID, &block)
}

func (s *LiteStorage) searchTxInCache(a tongo.AccountID, lt uint64) *core.Transaction {
	hash, err := s.GetTransactionByInMsgLT(a.String(), lt)
	if err != nil {
		return nil
	}
	tx, err := s.getTransaction(hash)
	if err != nil {
		return nil
	}
	return tx
}

func (s *LiteStorage) GetStorageProviders(ctx context.Context) ([]core.StorageProvider, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_storage_providers").Observe(v)
	}))
	defer timer.ObserveDuration()

	return nil, errors.New("not implemented")
}

func (s *LiteStorage) RunSmcMethod(ctx context.Context, id tongo.AccountID, method string, stack tlb.VmStack) (uint32, tlb.VmStack, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("run_smc_method").Observe(v)
	}))
	defer timer.ObserveDuration()
	return s.client.RunSmcMethod(ctx, id, method, stack)
}

func (s *LiteStorage) RunSmcMethodByID(ctx context.Context, id tongo.AccountID, method int, stack tlb.VmStack) (uint32, tlb.VmStack, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("run_smc_method_by_id").Observe(v)
	}))
	defer timer.ObserveDuration()
	return s.client.RunSmcMethodByID(ctx, id, method, stack)
}

func (s *LiteStorage) GetAccountTransactions(ctx context.Context, id tongo.AccountID, limit int, beforeLt, afterLt uint64, descendingOrder bool) ([]*core.Transaction, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_account_transactions").Observe(v)
	}))
	defer timer.ObserveDuration()
	txs, err := s.client.GetLastTransactions(ctx, id, limit) //todo: custom with beforeLt, afterLt and descendingOrder
	if err != nil {
		return nil, err
	}
	result := make([]*core.Transaction, len(txs))
	for i := range txs {
		tx, err := core.ConvertTransaction(id.Workchain, txs[i], nil)
		if err != nil {
			return nil, err
		}
		result[i] = tx
	}
	return result, nil
}

func (s *LiteStorage) FindAllDomainsResolvedToAddress(ctx context.Context, a tongo.AccountID, collections map[tongo.AccountID]string) ([]string, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("find_all_domains_resolved_to_address").Observe(v)
	}))
	defer timer.ObserveDuration()
	return nil, nil
}

func (s *LiteStorage) GetWalletPubKey(ctx context.Context, address tongo.AccountID) (ed25519.PublicKey, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_wallet_by_pubkey").Observe(v)
	}))
	defer timer.ObserveDuration()
	_, result, err := abi.GetPublicKey(ctx, s.executor, address)
	if err == nil {
		if r, ok := result.(abi.GetPublicKeyResult); ok {
			i := big.Int(r.PublicKey)
			b := i.Bytes()
			if len(b) < 24 || len(b) > 32 {
				return nil, fmt.Errorf("invalid public key")
			}
			return append(make([]byte, 32-len(b)), b...), nil
		}
	}
	pubKey, ok := s.pubKeyByAccountID.Load(address)
	if ok {
		return pubKey, nil
	}
	return nil, fmt.Errorf("can't get public key")
}

func (s *LiteStorage) ReindexAccount(ctx context.Context, accountID tongo.AccountID) error {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("reindex_account").Observe(v)
	}))
	defer timer.ObserveDuration()
	return nil
}

func (s *LiteStorage) GetDnsExpiring(ctx context.Context, id tongo.AccountID, period *int) ([]core.DnsExpiring, error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		storageTimeHistogramVec.WithLabelValues("get_dns_expiring").Observe(v)
	}))
	defer timer.ObserveDuration()
	return nil, nil
}

func (c *LiteStorage) GetInscriptionBalancesByAccount(ctx context.Context, a ton.AccountID) ([]core.InscriptionBalance, error) {
	return nil, fmt.Errorf("not implemented") //and cannot be without full blockckchain index
}

func (c *LiteStorage) GetInscriptionsHistoryByAccount(ctx context.Context, a ton.AccountID, ticker *string, beforeLt int64, limit int) ([]core.InscriptionMessage, error) {
	return nil, fmt.Errorf("not implemented") //and cannot be without full blockckchain index
}

func (s *LiteStorage) GetReducedBlocks(ctx context.Context, from, to int64) ([]core.ReducedBlock, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *LiteStorage) GetAccountMultisigs(ctx context.Context, accountID ton.AccountID) ([]core.Multisig, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *LiteStorage) GetMultisigByID(ctx context.Context, accountID ton.AccountID) (*core.Multisig, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *LiteStorage) getClient() (*liteapi.Client, func()) {
	client := s.connPool.Get().(*liteapi.Client)
	return client, func() {
		s.connPool.Put(client)
	}
}

// Add context timeout wrapper
func (s *LiteStorage) withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

func safeConvertTransaction(workchain int32, tx tongo.Transaction, cd *abi.ContractDescription) (*core.Transaction, error) {
	var result *core.Transaction
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in transaction conversion: %v", r)
		}
	}()

	result, err = core.ConvertTransaction(workchain, tx, cd)
	return result, err
}

func (s *LiteStorage) storeTransactionWithRetry(hash tongo.Bits256, tx *core.Transaction) error {
	return retry.Do(
		func() error {
			return s.storeTransaction(hash, tx)
		},
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
	)
}

func (s *LiteStorage) fetchTransactionFromChain(ctx context.Context, hash tongo.Bits256) (*core.Transaction, error) {
	s.logger.Info("fetching transaction from chain", zap.String("hash", hash.Hex()))

	for _, wc := range []int32{0, -1} {
		info, err := s.client.GetMasterchainInfo(ctx)
		if err != nil {
			continue
		}

		blockID := info.Last.ToBlockIdExt()
		blockID.Workchain = wc

		for i := 0; i < 1000; i++ {
			// Get master block
			block, err := s.getBlock(blockID)
			if err != nil {
				block, err = s.fetchBlockFromChain(ctx, blockID)
				if err != nil {
					break
				}
			}

			// Check master block transactions
			if tx := s.findTransactionInBlock(block, blockID, hash); tx != nil {
				return tx, nil
			}

			// Check shard blocks
			shards, err := s.client.GetAllShardsInfo(ctx, blockID)
			if err != nil {
				continue
			}

			for _, shard := range shards {
				shardIDExt := tongo.BlockIDExt{
					BlockID: tongo.BlockID{
						Workchain: shard.BlockID.Workchain,
						Shard:     shard.BlockID.Shard,
						Seqno:     shard.BlockID.Seqno,
					},
					RootHash: blockID.RootHash,
					FileHash: blockID.FileHash,
				}
				block, err := s.getBlock(shardIDExt)
				if err != nil {
					block, err = s.fetchBlockFromChain(ctx, shardIDExt)
					if err != nil {
						continue
					}
				}

				if tx := s.findTransactionInBlock(block, blockID, hash); tx != nil {
					return tx, nil
				}
			}

			blockID.Seqno--
		}
	}
	return nil, fmt.Errorf("transaction not found")
}

func (s *LiteStorage) processShardBlocks(ctx context.Context, masterBlock tongo.BlockIDExt) error {
	block, err := s.getBlock(masterBlock)
	if err != nil {
		// If not in DB, fetch from chain
		block, err = s.fetchBlockFromChain(ctx, masterBlock)
		if err != nil {
			return fmt.Errorf("failed to get master block: %w", err)
		}
	}

	shardIDs := tongo.ShardIDs(block)
	s.logger.Info("starting shard blocks processing",
		zap.String("master_block", masterBlock.String()),
		zap.Int("shard_count", len(shardIDs)))

	for _, shardID := range shardIDs {
		shardBlock, err := s.getBlock(shardID)
		if err != nil {
			// If not in DB, fetch from chain
			shardBlock, err = s.fetchBlockFromChain(ctx, shardID)
			if err != nil {
				s.logger.Error("failed to get shard block",
					zap.String("shard_block", shardID.String()),
					zap.Error(err))
				continue
			}
		}

		if err := s.processBlockTransactionsSafely(shardID, shardBlock); err != nil {
			s.logger.Error("failed to process shard block transactions",
				zap.String("shard_block", shardID.String()),
				zap.Error(err))
			continue
		}
	}
	return nil
}

func (s *LiteStorage) findTransactionInBlock(block *tlb.Block, blockID tongo.BlockIDExt, hash tongo.Bits256) *core.Transaction {
	for _, tx := range block.AllTransactions() {
		if tongo.Bits256(tx.Hash()) == hash {
			transaction, err := safeConvertTransaction(blockID.Workchain, tongo.Transaction{
				BlockID:     blockID,
				Transaction: *tx,
			}, nil)
			if err != nil {
				s.logger.Error("failed to convert transaction",
					zap.String("hash", hash.Hex()),
					zap.Error(err))
				continue
			}
			return transaction
		}
	}
	return nil
}

func (s *LiteStorage) validateAndRecoverBlockSequence(ctx context.Context, block indexer.IDandBlock) error {
	if block.ID.Seqno != s.lastProcessedSeqno+1 {
		s.logger.Warn("block sequence gap detected",
			zap.Uint32("expected", s.lastProcessedSeqno+1),
			zap.Uint32("got", block.ID.Seqno))

		// Attempt to recover missing blocks
		for seqno := s.lastProcessedSeqno + 1; seqno < block.ID.Seqno; seqno++ {
			if err := s.recoverMissingBlocks(ctx, seqno, seqno); err != nil {
				return fmt.Errorf("failed to recover block %d: %w", seqno, err)
			}
		}
	}
	return nil
}

func (s *LiteStorage) getLastProcessedSeqno() (uint32, error) {
	var lastSeqno uint32
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("last_processed_seqno"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Return 0 for initial state
			}
			return fmt.Errorf("database error: %w", err)
		}
		return item.Value(func(val []byte) error {
			lastSeqno = binary.LittleEndian.Uint32(val)
			return nil
		})
	})
	return lastSeqno, err
}

// Also add method to update the seqno
func (s *LiteStorage) updateStateAtomically(seqno uint32, block indexer.IDandBlock) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Update last processed seqno
		if err := s.updateLastProcessedSeqnoTx(txn, seqno); err != nil {
			return err
		}

		// Mark block as processed in queue
		s.blockQueue.MarkProcessed(block.ID)

		return nil
	})
}

func (s *LiteStorage) recoverMissingBlocks(ctx context.Context, fromSeqno, toSeqno uint32) error {
	s.logger.Info("recovering missing blocks",
		zap.Uint32("from", fromSeqno),
		zap.Uint32("to", toSeqno))

	// Get current masterchain info
	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get masterchain info: %w", err)
	}

	for seqno := fromSeqno; seqno <= toSeqno; seqno++ {
		blockID := tongo.BlockID{
			Workchain: -1,
			Shard:     info.Last.Shard,
			Seqno:     seqno,
		}

		s.logger.Info("attempting to recover block",
			zap.String("block_id", blockID.String()),
			zap.Uint64("shard", info.Last.Shard),
			zap.Uint32("seqno", seqno))

		blockIDExt, _, err := s.client.LookupBlock(ctx, blockID, 1, nil, nil)
		if err != nil {
			s.logger.Error("failed to lookup block",
				zap.String("block_id", blockID.String()),
				zap.Error(err))
			continue
		}

		block, err := s.client.GetBlock(ctx, blockIDExt)
		if err != nil {
			s.logger.Error("failed to get block",
				zap.String("block_id_ext", blockIDExt.String()),
				zap.Error(err))
			continue
		}

		if err := s.processBlockAtomically(indexer.IDandBlock{
			ID:    blockIDExt,
			Block: &block,
		}); err != nil {
			s.logger.Error("failed to process recovered block",
				zap.String("block_id_ext", blockIDExt.String()),
				zap.Error(err))
			continue
		}

		s.logger.Info("successfully recovered block",
			zap.String("block_id_ext", blockIDExt.String()))
	}

	return nil
}

func (s *LiteStorage) storeBlockWithCache(blockID tongo.BlockIDExt, block *tlb.Block) error {
	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	if err := s.storeBlock(blockID, block); err != nil {
		return err
	}

	s.blockCache.Store(blockID, block)
	return nil
}

func (s *LiteStorage) updateLastProcessedSeqnoTx(txn *badger.Txn, seqno uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, seqno)
	return txn.Set([]byte("last_processed_seqno"), buf)
}

func (s *LiteStorage) storeBlockTx(txn *badger.Txn, blockID tongo.BlockIDExt, block *tlb.Block) error {
	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	key := append([]byte("blk:"), []byte(blockID.String())...)
	return txn.Set(key, data)
}

const maxBatchSize = 1000

func (s *LiteStorage) storeTransactionBatch(batch []*core.Transaction) error {
	return s.retryOperation(context.Background(), func(txn *badger.Txn) error {
		for _, tx := range batch {
			data, err := json.Marshal(tx)
			if err != nil {
				return fmt.Errorf("failed to marshal transaction: %w", err)
			}
			key := append([]byte(txKeyPrefix), tx.Hash[:]...)
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LiteStorage) retryOperation(ctx context.Context, fn func(txn *badger.Txn) error) error {
	return retry.Do(
		func() error { return s.db.Update(fn) },
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.Context(ctx))
}

func (s *LiteStorage) getBlockWithRetry(ctx context.Context, blockID tongo.BlockID) (tongo.BlockIDExt, *tlb.Block, error) {
	var blockIDExt tongo.BlockIDExt
	var block *tlb.Block

	err := retry.Do(func() error {
		var err error
		blockIDExt, _, err = s.client.LookupBlock(ctx, blockID, 1, nil, nil)
		if err != nil {
			return err
		}

		block, err = s.getBlock(blockIDExt)
		if err != nil {
			block, err = s.fetchBlockFromChain(ctx, blockIDExt)
		}
		return err
	},
		retry.Attempts(uint(s.blockRetryCount)),
		retry.Delay(s.blockRetryDelay),
		retry.DelayType(retry.BackOffDelay))

	return blockIDExt, block, err
}

func (s *LiteStorage) verifyBlockSequence(ctx context.Context, currentBlock tongo.BlockIDExt) error {
	lastProcessed, err := s.getLastProcessedSeqno()
	if err != nil {
		if err == badger.ErrKeyNotFound {
			// Get current masterchain info
			info, err := s.client.GetMasterchainInfo(ctx)
			if err != nil {
				return fmt.Errorf("failed to get masterchain info: %w", err)
			}

			// Initialize with current block's seqno - 1 to match indexer
			initialSeqno := currentBlock.Seqno - 1
			s.logger.Info("initializing last processed seqno",
				zap.Uint32("current_block", currentBlock.Seqno),
				zap.Uint32("initial_seqno", initialSeqno),
				zap.Uint32("masterchain_seqno", info.Last.Seqno))

			if err := s.retryOperation(ctx, func(txn *badger.Txn) error {
				return s.updateLastProcessedSeqnoTx(txn, initialSeqno)
			}); err != nil {
				return fmt.Errorf("failed to initialize last processed seqno: %w", err)
			}

			// Update in-memory value
			s.seqnoMutex.Lock()
			s.lastProcessedSeqno = initialSeqno
			s.seqnoMutex.Unlock()

			return nil
		}
		return err
	}

	s.logger.Info("initializing last processed seqno before verification",
		zap.Uint32("lastProcessed", lastProcessed))

	if lastProcessed == 0 {
		lastProcessed = currentBlock.Seqno - 3
		s.lastProcessedSeqno = currentBlock.Seqno - 3
		s.logger.Info("initializing last processed seqno",
			zap.Uint32("current_block", currentBlock.Seqno),
			zap.Uint32("lastProcessed", lastProcessed))
	}

	// Check for gaps
	if currentBlock.Seqno > lastProcessed+1 {
		return s.recoverMissingBlocks(ctx, lastProcessed+1, currentBlock.Seqno-1)
	}
	return nil
}
