package litestorage

import (
	"context"
	"crypto/ed25519"
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
		WithNumGoroutines(16).           // More concurrent processing
		WithValueThreshold(32).          // Optimize for larger values
		WithBlockCacheSize(8 << 30).     // 8GB block cache
		WithIndexCacheSize(16 << 30).    // 16GB index cache
		WithMemTableSize(512 << 20)      // 512MB memtable

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	s := &LiteStorage{
		logger:                 logger,
		client:                 cli,
		executor:               o.executor,
		stopCh:                 make(chan struct{}),
		knownAccounts:          make(map[string][]tongo.AccountID),
		jettonMetaCache:        xsync.NewTypedMapOf[string, tep64.Metadata](hashString),
		blockCache:             xsync.NewTypedMapOf[tongo.BlockIDExt, *tlb.Block](hashBlockIDExt),
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
	s.logger.Debug("storing transaction with key",
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

	s.logger.Debug("getting transaction with key",
		zap.String("key", hex.EncodeToString(key)),
		zap.String("hash", hash.Hex()))

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
	s.logger.Info("starting block processing loop")

	for block := range ch {
		// Validate block
		if block.Block == nil {
			s.logger.Error("received nil block")
			continue
		}

		// Process block atomically
		err := s.processBlockAtomically(block)
		if err != nil {
			s.logger.Error("failed to process block",
				zap.String("block_id", block.ID.String()),
				zap.Error(err))
			continue
		}
	}
}

func (s *LiteStorage) processBlockAtomically(block indexer.IDandBlock) error {
	s.logger.Info("starting block processing",
		zap.String("block_id", block.ID.String()))

	// Store master block
	if err := s.storeBlock(block.ID, block.Block); err != nil {
		return fmt.Errorf("failed to store master block: %w", err)
	}

	// Process master block transactions
	if err := s.processBlockTransactions(block.ID, block.Block); err != nil {
		return fmt.Errorf("failed to process master transactions: %w", err)
	}

	// Process shard blocks
	if err := s.processShardBlocks(context.Background(), block.ID); err != nil {
		s.logger.Error("failed to process shard blocks", zap.Error(err))
		// Don't return error to continue processing
	}

	return nil
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
	s.logger.Info("starting transaction lookup",
		zap.String("hash", hash.Hex()))

	// Try getting from DB first
	tx, err := s.getTransactionWithRetry(ctx, hash)
	if err == nil {
		s.logger.Info("found transaction in DB")
		return tx, nil
	}

	// Try both workchains
	// workchains := []int32{0, -1} // Try workchain 0 first
	// for _, wc := range workchains {
	s.logger.Info("searching in workchain",
		zap.String("hash", hash.Hex()))

	info, err := s.client.GetMasterchainInfo(ctx)
	if err != nil {
		s.logger.Error("failed to get masterchain info", zap.Error(err))
		return nil, err
	}

	blockID := info.Last.ToBlockIdExt()
	s.logger.Info("searching in",
		zap.String("block_id", blockID.String()))
	// blockID.Workchain = wc

	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		s.logger.Error("failed to get block",
			zap.String("block_id", blockID.String()),
			zap.Error(err))
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
	s.logger.Info("transaction not found in recent blocks, trying chain search",
		zap.String("hash", hash.Hex()))
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
	block, err := s.client.GetBlock(ctx, blockID)
	if err != nil {
		return nil, err
	}
	return core.ExtractTransactions(blockID, &block)
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
func (s *LiteStorage) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
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
	shards, err := s.client.GetAllShardsInfo(ctx, masterBlock)
	if err != nil {
		s.logger.Error("failed to get shard blocks",
			zap.String("master_block", masterBlock.String()),
			zap.Error(err))
		return err
	}

	for _, shard := range shards {
		s.logger.Info("processing shard block",
			zap.String("shard_block", shard.BlockID.String()),
			zap.String("master_block", masterBlock.String()))

		blockIDExt := tongo.BlockIDExt{
			BlockID:  shard.BlockID,
			RootHash: masterBlock.RootHash,
			FileHash: masterBlock.FileHash,
		}
		block, err := s.client.GetBlock(ctx, blockIDExt)
		if err != nil {
			s.logger.Error("failed to get shard block", zap.Error(err))
			continue
		}

		if err := s.storeBlock(blockIDExt, &block); err != nil {
			s.logger.Error("failed to store shard block", zap.Error(err))
			continue
		}

		if err := s.processBlockTransactions(blockIDExt, &block); err != nil {
			s.logger.Error("failed to process shard transactions", zap.Error(err))
		}
	}
	return nil
}

func (s *LiteStorage) processBlockTransactions(blockID tongo.BlockIDExt, block *tlb.Block) error {
	for _, tx := range block.AllTransactions() {
		hash := tongo.Bits256(tx.Hash())
		accountID := tongo.AccountID{
			Workchain: blockID.Workchain,
			Address:   tx.AccountAddr,
		}

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

		if err := s.storeTransaction(hash, transaction); err != nil {
			s.logger.Error("failed to store transaction",
				zap.String("hash", hash.Hex()),
				zap.Error(err))
			continue
		}

		s.logger.Info("stored transaction",
			zap.String("hash", hash.Hex()),
			zap.String("block_id", blockID.String()),
			zap.String("account", accountID.String()))
	}
	return nil
}

func (s *LiteStorage) findTransactionInBlock(block *tlb.Block, blockID tongo.BlockIDExt, hash tongo.Bits256) *core.Transaction {
	for _, tx := range block.AllTransactions() {
		if tongo.Bits256(tx.Hash()) == hash {
			transaction, err := safeConvertTransaction(block.Info.Shard.WorkchainID, tongo.Transaction{
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
