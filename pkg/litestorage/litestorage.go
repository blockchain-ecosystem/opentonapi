package litestorage

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/puzpuzpuz/xsync/v2"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/abi"
	"github.com/tonkeeper/tongo/boc"
	"github.com/tonkeeper/tongo/liteapi"
	"github.com/tonkeeper/tongo/tep64"
	"github.com/tonkeeper/tongo/tlb"
	"github.com/tonkeeper/tongo/ton"
	"go.uber.org/zap"

	"github.com/dgraph-io/badger/v3"
	"github.com/tonkeeper/opentonapi/pkg/blockchain/indexer"
	"github.com/tonkeeper/opentonapi/pkg/cache"
	"github.com/tonkeeper/opentonapi/pkg/core"
)

const (
	prefixTx    = "tx:"
	prefixBlock = "block:"
	prefixMeta  = "meta:"
	// Cache TTL constants
	TransactionCacheTTL = time.Minute * 5
	BlockCacheTTL      = time.Minute * 10
	MetadataCacheTTL   = time.Hour    // For less frequently changing data
	ConfigCacheTTL     = time.Second * 2
	// BadgerDB cleanup settings
	BadgerGCInterval = 6 * time.Hour
	BadgerTTL       = 48 * 24 * time.Hour  // 48 days retention
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

type BadgerStorage struct {
	db *badger.DB
	mu sync.RWMutex  // Add mutex for synchronization
	// Hot cache for frequently accessed data
	transactionCache cache.Cache[tongo.Bits256, *core.Transaction]
	blockCache       cache.Cache[tongo.BlockIDExt, *tlb.Block]
	stopGC          chan struct{} // Channel to stop GC goroutine
}

// StartGC starts the garbage collection process
func (b *BadgerStorage) StartGC() {
	go func() {
		ticker := time.NewTicker(BadgerGCInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				// Run value log GC
				err := b.db.RunValueLogGC(0.5) // Run when we can release 50% of space
				if err != nil && err != badger.ErrNoRewrite {
					// Log error but continue
					log.Printf("Badger GC error: %v", err)
				}
			case <-b.stopGC:
				return
			}
		}
	}()
}

// StopGC stops the garbage collection process
func (b *BadgerStorage) StopGC() {
	if b.stopGC != nil {
		close(b.stopGC)
	}
}

// Add new configuration types
type StorageConfig struct {
	MaxCacheSize      int
	CacheTTL         time.Duration
	MaxDBConnections int
	DBPath           string
	EnableFullScan    bool          // Add new field
	WorkerPoolSize    int           // Add new field
	CacheEvictionTime time.Duration // Add new field
}

type StorageMetrics struct {
	dbOperations   *prometheus.CounterVec
	dbLatency      *prometheus.HistogramVec
	cacheHitRate   *prometheus.GaugeVec
	dbSize         prometheus.Gauge
	accountsCount  prometheus.Gauge
	dbErrors      *prometheus.CounterVec
	cacheHits     *prometheus.CounterVec
	cacheMisses   *prometheus.CounterVec
}

type LiteStorage struct {
	logger     *zap.Logger
	client     *liteapi.Client
	executor   abi.Executor
	persistent *BadgerStorage
	
	// Hot caches
	jettonMetaCache         cache.Cache[tongo.AccountID, tep64.Metadata]
	transactionsIndexByHash *xsync.MapOf[tongo.Bits256, *core.Transaction]
	transactionsByInMsgLT   *xsync.MapOf[inMsgCreatedLT, tongo.Bits256]
	blockCache              *xsync.MapOf[tongo.BlockIDExt, *tlb.Block]
	accountInterfacesCache  cache.Cache[tongo.AccountID, []abi.ContractInterface]
	// tvmLibraryCache contains public tvm libraries.
	// As a library is immutable, it's ok to cache it.
	tvmLibraryCache        cache.Cache[string, boc.Cell]
	configCache            cache.Cache[int, ton.BlockchainConfig]
	
	// maxGoroutines specifies a number of goroutines used to perform some time-consuming operations.
	maxGoroutines      int
	pubKeyByAccountID  *xsync.MapOf[tongo.AccountID, ed25519.PublicKey]
	
	stopCh chan struct{}
	// mu protects trimmedConfigBase64.
	mu sync.RWMutex
	// trimmedConfigBase64 is a blockchain config but with a limited set of keys.
	// it's performance optimization.
	// tmv and txEmulator work much faster with a smaller config.
	trimmedConfigBase64 string
	config *StorageConfig  // Change from anonymous struct to StorageConfig pointer
	
	// Add metrics for monitoring
	metrics struct {
		accountsProcessed    prometheus.Counter
		cacheHitRate        prometheus.Gauge
		cacheSize           prometheus.Gauge
		processingLatency   prometheus.Histogram
		dbLatency          *prometheus.HistogramVec
		dbOperations       *prometheus.CounterVec
	}
	db *badger.DB
	wg sync.WaitGroup
}

// Option configures LiteStorage
type Option func(o *Options)

type Options struct {
	preloadAccounts []tongo.AccountID
	preloadBlocks   []tongo.BlockID
	tfPools         []tongo.AccountID
	jettons         []tongo.AccountID
	executor        abi.Executor
	// blockCh is used to receive new blocks in the blockchain, if set.
	blockCh <-chan indexer.IDandBlock
	MaxGoroutines int // number of concurrent goroutines for transaction processing
	config StorageConfig
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

// Add configuration option
func WithStorageConfig(config StorageConfig) Option {
	return func(o *Options) {
		o.config = config
	}
}

// Initialize metrics
func initStorageMetrics() *StorageMetrics {
	return &StorageMetrics{
		dbOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "badger_operations_total",
				Help: "Number of BadgerDB operations",
			},
			[]string{"operation", "status"},
		),
		// ... initialize other metrics
	}
}

func NewLiteStorage(logger *zap.Logger, client *liteapi.Client, opts ...Option) (*LiteStorage, error) {
	config := &StorageConfig{
		DBPath: "/tmp/badger",  // Default path
		MaxCacheSize: 1000000,
		CacheTTL: time.Hour,
	}
	
	// Open BadgerDB
	dbOpts := badger.DefaultOptions(config.DBPath)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	s := &LiteStorage{
		logger: logger,
		client: client,
		config: config,
		metrics: struct {
			accountsProcessed    prometheus.Counter
			cacheHitRate        prometheus.Gauge
			cacheSize           prometheus.Gauge
			processingLatency   prometheus.Histogram
			dbLatency          *prometheus.HistogramVec
			dbOperations       *prometheus.CounterVec
		}{
			dbLatency: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: "litestorage_db_operation_latency",
					Help: "Database operation latency in seconds",
				},
				[]string{"operation"},
			),
			dbOperations: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "litestorage_db_operations_total",
					Help: "Total number of database operations",
				},
				[]string{"operation", "status"},
			),
		},
		db: db,
	}
	
	// Initialize scanner if full scan is enabled
	if s.config.EnableFullScan {
		scanner := &AccountScanner{
			storage: s,
			logger:  logger,
			workers: make(chan struct{}, s.config.WorkerPoolSize),
		}
		scanner.Start(context.Background())
	}
	
	return s, nil
}

func (s *LiteStorage) SetExecutor(e abi.Executor) {
	s.executor = e
}

// Shutdown stops all background goroutines.
func (s *LiteStorage) Shutdown() {
	s.stopCh <- struct{}{}
	if err := s.persistent.db.Close(); err != nil {
		s.logger.Error("failed to close BadgerDB", zap.Error(err))
	}
}

func (s *LiteStorage) run(ch <-chan indexer.IDandBlock) {
	workers := make(chan struct{}, s.maxGoroutines)
	
	for idAndBlock := range ch {
		block := idAndBlock.Block
		blockID := idAndBlock.ID
		
		// Process all accounts from transactions in the block
		accounts := make([]tongo.AccountID, 0)
		for _, tx := range block.AllTransactions() {
			accountID := tongo.AccountID{
				Workchain: blockID.Workchain,
				Address:   tx.AccountAddr,
			}
			accounts = append(accounts, accountID)
		}

		// Process each account concurrently
		for _, accountID := range accounts {
			workers <- struct{}{} // Acquire worker slot
			go func(accID tongo.AccountID) {
				defer func() { <-workers }() // Release worker slot
				
				if err := s.processTransactions(accID, block, tongo.BlockIDExt{BlockID: blockID.BlockID}); err != nil {
					s.logger.Error("failed to process transactions",
						zap.String("account", accID.String()),
						zap.Error(err))
				}
			}(accountID)
		}
	}
}

func (s *LiteStorage) processTransactions(accountID tongo.AccountID, block *tlb.Block, blockID tongo.BlockIDExt) error {
	s.logger.Debug("processing transactions", 
		zap.String("account", accountID.String()),
		zap.Int("tx_count", len(block.AllTransactions())))

	txs := make([]*core.Transaction, 0, len(block.AllTransactions()))
	
	for _, tx := range block.AllTransactions() {
		hash := tongo.Bits256(tx.Hash())
		
		transaction, err := core.ConvertTransaction(accountID.Workchain, tongo.Transaction{
			Transaction: *tx,
			BlockID:    blockID,
		}, nil)
		
		if err != nil {
			s.logger.Error("failed to convert tx",
				zap.String("tx_hash", hash.Hex()),
				zap.Error(err))
			continue
		}

		// Store individual transaction immediately
		err = s.SaveTransaction(transaction)
		if err != nil {
			s.logger.Error("failed to save transaction",
				zap.String("tx_hash", hash.Hex()),
				zap.Error(err))
			continue
		}

		s.logger.Debug("transaction processed and saved",
			zap.String("tx_hash", hash.Hex()))

		txs = append(txs, transaction)

		if createLT, ok := extractInMsgCreatedLT(accountID, tx); ok {
			s.transactionsByInMsgLT.Store(createLT, hash)
		}
	}

	// Batch store with retry
	err := retry.Do(
		func() error {
			return s.persistent.BatchSetTransactions(txs)
		},
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
	)

	if err != nil {
		s.logger.Error("failed to batch store transactions", zap.Error(err))
		return err
	}

	// Update memory cache
	for _, tx := range txs {
		s.transactionsIndexByHash.Store(tx.Hash, tx)
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
		s.transactionsIndexByHash.Store(hash, t)
		if createLT, ok := extractInMsgCreatedLT(a, &tx.Transaction); ok {
			s.transactionsByInMsgLT.Store(createLT, hash)
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
		s.transactionsIndexByHash.Store(hash, t)
		if createLT, ok := extractInMsgCreatedLT(accountID, tx); ok {
			s.transactionsByInMsgLT.Store(createLT, hash)
		}
	}
	return nil
}

func (s *LiteStorage) GetBlockHeader(ctx context.Context, id tongo.BlockID) (*core.BlockHeader, error) {
	var header *core.BlockHeader
	err := s.db.View(func(txn *badger.Txn) error {
		key := append([]byte(prefixBlock), []byte(id.String())...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("block not found: %v", id)
		}
		if err != nil {
			return fmt.Errorf("get block: %w", err)
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &header)
		})
	})
	
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
		return nil, err
	}
	return s.GetBlockHeader(ctx, info.Last.ToBlockIdExt().BlockID)
}

func (s *LiteStorage) GetTransaction(ctx context.Context, hash tongo.Bits256) (*core.Transaction, error) {
	s.logger.Debug("getting transaction from BadgerDB", zap.String("hash", hash.Hex()))

	var tx core.Transaction
	err := s.db.View(func(txn *badger.Txn) error {
		key := append([]byte("tx:"), []byte(hash.Hex())...)
		s.logger.Debug("searching in BadgerDB", zap.String("key", string(key)))
		
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			s.logger.Debug("transaction not found in BadgerDB", zap.String("hash", hash.Hex()))
			return core.ErrEntityNotFound
		}
		if err != nil {
			s.logger.Error("BadgerDB error", zap.Error(err))
			return fmt.Errorf("get transaction: %w", err)
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &tx)
		})
	})

	if err != nil {
		s.logger.Error("failed to get transaction", 
			zap.String("hash", hash.Hex()),
			zap.Error(err))
		return nil, err
	}

	s.logger.Debug("transaction found in BadgerDB", zap.String("hash", hash.Hex()))
	return &tx, nil
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
	hash, ok := s.transactionsByInMsgLT.Load(inMsgCreatedLT{account: a, lt: lt})
	if !ok {
		return nil
	}
	tx, ok := s.transactionsIndexByHash.Load(hash)
	if !ok {
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

func (b *BadgerStorage) SetTransaction(tx *core.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	data, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	
	return b.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(prefixTx + tx.Hash.Hex()), data).
			WithTTL(BadgerTTL)
		return txn.SetEntry(entry)
	})
}

func (b *BadgerStorage) BatchSetTransactions(txs []*core.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	batch := b.db.NewWriteBatch()
	defer batch.Cancel()
	
	for _, tx := range txs {
		data, err := json.Marshal(tx)
		if err != nil {
			return err
		}
		
		err = batch.SetEntry(badger.NewEntry([]byte(prefixTx + tx.Hash.Hex()), data).
			WithTTL(BadgerTTL))
		if err != nil {
			return err
		}
	}
	
	return batch.Flush()
}

// Example usage in other methods
func (s *LiteStorage) SaveTransaction(tx *core.Transaction) error {
	// Store in memory cache
	s.transactionsIndexByHash.Store(tx.Hash, tx)

	// Store in BadgerDB
	err := s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(tx)
		if err != nil {
			return fmt.Errorf("marshal transaction: %w", err)
		}

		key := append([]byte("tx:"), []byte(tx.Hash.Hex())...)
		if err := txn.Set(key, data); err != nil {
			return fmt.Errorf("set transaction: %w", err)
		}
		
		// Log successful storage
		s.logger.Debug("transaction saved", 
			zap.String("hash", tx.Hash.Hex()),
			zap.Int("data_size", len(data)))
		
		return nil
	})

	if err != nil {
		s.logger.Error("failed to save transaction",
			zap.String("hash", tx.Hash.Hex()),
			zap.Error(err))
		return err
	}

	return nil
}

func makeTransactionKey(hash tongo.Bits256) []byte {
	return []byte(prefixTx + hash.Hex())
}

// Process all accounts from BadgerDB
func (s *LiteStorage) ProcessAllAccounts(ctx context.Context, fn func(accountID tongo.AccountID) error) error {
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("account:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				account := parseAccountFromKey(it.Item().Key())
				if account == (tongo.AccountID{}) {
					continue
				}
				if err := fn(account); err != nil {
					return fmt.Errorf("process account %s: %w", account, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("process all accounts: %w", err)
	}
	return nil
}


func (s *LiteStorage) clearCaches() {
	// Clear all keys from caches
	for _, key := range s.jettonMetaCache.Keys() {
		s.jettonMetaCache.Delete(key)
	}
	for _, key := range s.accountInterfacesCache.Keys() {
		s.accountInterfacesCache.Delete(key)
	}
	for _, key := range s.tvmLibraryCache.Keys() {
		s.tvmLibraryCache.Delete(key)
	}
	for _, key := range s.configCache.Keys() {
		s.configCache.Delete(key)
	}
	
	s.metrics.dbOperations.WithLabelValues("cache", "clear").Inc()
}

func (s *LiteStorage) SaveBlock(blockIDExt tongo.BlockIDExt, block *tlb.Block) error {
	// Store in memory cache first
	s.blockCache.Store(blockIDExt, block)

	// Store in BadgerDB with TTL
	err := s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(block)
		if err != nil {
			return fmt.Errorf("marshal block: %w", err)
		}

		key := append([]byte(prefixBlock), []byte(blockIDExt.BlockID.String())...)
		entry := badger.NewEntry(key, data).WithTTL(BlockCacheTTL)
		
		s.logger.Debug("saving block to BadgerDB",
			zap.String("block_id", blockIDExt.BlockID.String()),
			zap.Int("data_size", len(data)))
			
		return txn.SetEntry(entry)
	})

	if err != nil {
		s.logger.Error("failed to save block",
			zap.String("block_id", blockIDExt.BlockID.String()),
			zap.Error(err))
		return err
	}

	return nil
}

func (s *LiteStorage) GetBlock(ctx context.Context, id tongo.BlockID) (*tlb.Block, error) {
	s.logger.Debug("getting block", zap.String("block_id", id.String()))

	// Try memory cache first - convert BlockID to BlockIDExt for cache lookup
	blockIDExt := tongo.BlockIDExt{BlockID: id}
	if block, ok := s.blockCache.Load(blockIDExt); ok {
		s.logger.Debug("block found in cache", zap.String("block_id", id.String()))
		return block, nil
	}

	// Try BadgerDB
	var block tlb.Block
	err := s.db.View(func(txn *badger.Txn) error {
		key := append([]byte(prefixBlock), []byte(id.String())...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			s.logger.Debug("block not found in BadgerDB", zap.String("block_id", id.String()))
			return fmt.Errorf("block not found: %v", id)
		}
		if err != nil {
			return fmt.Errorf("get block: %w", err)
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &block)
		})
	})

	if err != nil {
		s.logger.Error("failed to get block", 
			zap.String("block_id", id.String()),
			zap.Error(err))
		return nil, err
	}

	s.logger.Debug("block found in BadgerDB", zap.String("block_id", id.String()))
	return &block, nil
}
