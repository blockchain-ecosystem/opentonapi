package sources

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tonkeeper/opentonapi/pkg/cache"
	"github.com/tonkeeper/opentonapi/pkg/core"
	"github.com/tonkeeper/tongo"
	"go.uber.org/zap"
)

var (
	traceNumber = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streaming_api_trace_dispatch",
		Help: "Number of traces",
	}, []string{"type"})
)

type SubscribeToTraceOptions struct {
	AllAccounts bool
	Accounts    []tongo.AccountID
}

type TraceSource interface {
	SubscribeToTraces(ctx context.Context, deliveryFn DeliveryFn, opts SubscribeToTraceOptions) CancelFn
}

type storage interface {
	GetTrace(ctx context.Context, hash tongo.Bits256) (*core.Trace, error)
	SearchTransactionByMessageHash(ctx context.Context, hash tongo.Bits256) (*tongo.Bits256, error)
}

type dispatcher interface {
	Dispatch(accountIDs []tongo.AccountID, event []byte)
	RegisterSubscriber(fn DeliveryFn, options SubscribeToTraceOptions) CancelFn
}

type Tracer struct {
	logger     *zap.Logger
	storage    storage
	dispatcher dispatcher
	source     TransactionSource

	mu         sync.Mutex
	traceCache cache.Cache[string, struct{}]
	workerPool chan struct{}
	maxWorkers int
}

func NewTracer(logger *zap.Logger, storage storage, source TransactionSource) *Tracer {
	return &Tracer{
		logger:     logger,
		storage:    storage,
		source:     source,
		dispatcher: NewTraceDispatcher(logger),
		traceCache: cache.NewLRUCache[string, struct{}](10000, "tracer_trace_cache"),
		maxWorkers: 100,
		workerPool: make(chan struct{}, 100),
	}
}

var _ TraceSource = (*Tracer)(nil)

func (t *Tracer) SubscribeToTraces(ctx context.Context, deliveryFn DeliveryFn, opts SubscribeToTraceOptions) CancelFn {
	t.logger.Debug("subscribe to traces",
		zap.Bool("all-accounts", opts.AllAccounts),
		zap.Stringers("accounts", opts.Accounts))

	return t.dispatcher.RegisterSubscriber(deliveryFn, opts)
}

func (t *Tracer) Run(ctx context.Context) error {
	txCh := make(chan TransactionEventData, 1000)
	cancelFn := t.source.SubscribeToTransactions(ctx, func(eventData []byte) {
		var tx TransactionEventData
		if err := json.Unmarshal(eventData, &tx); err != nil {
			t.logger.Error("json.Unmarshal() failed", zap.Error(err))
			return
		}
		select {
		case txCh <- tx:
		case <-ctx.Done():
			return
		}
	}, SubscribeToTransactionsOptions{AllAccounts: true, AllOperations: true})

	defer cancelFn()

	// Initialize worker pool
	for i := 0; i < t.maxWorkers; i++ {
		t.workerPool <- struct{}{}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txEvent := <-txCh:
			select {
			case worker := <-t.workerPool:
				go func(tx TransactionEventData) {
					defer func() { t.workerPool <- worker }()
					
					var hash tongo.Bits256
					if err := hash.FromHex(tx.TxHash); err != nil {
						t.logger.Error("hash.FromHex() failed", zap.Error(err))
						return
					}

					if !t.shouldProcess(hash) {
						return
					}

					trace, err := t.storage.GetTrace(ctx, hash)
					if err != nil {
						if errors.Is(err, core.ErrEntityNotFound) {
							// Try finding by message hash
							txHash, err := t.storage.SearchTransactionByMessageHash(ctx, hash)
							if err != nil {
								// Skip logging for not found cases
								if !errors.Is(err, core.ErrEntityNotFound) && !errors.Is(err, context.Canceled) {
									t.logger.Debug("search transaction failed",
										zap.Error(err),
										zap.String("hash", hash.Hex()))
								}
								return
							}
							
							// Double check txHash is not nil
							if txHash == nil {
								return
							}

							trace, err = t.storage.GetTrace(ctx, *txHash)
							if err != nil {
								// Skip logging for not found cases
								if !errors.Is(err, core.ErrEntityNotFound) && !errors.Is(err, context.Canceled) {
									t.logger.Debug("get trace by tx hash failed",
										zap.Error(err),
										zap.String("hash", txHash.Hex()))
								}
								return
							}
						} else if !errors.Is(err, context.Canceled) {
							// Only log real errors at error level
							t.logger.Error("failed to get trace",
								zap.Error(err),
								zap.String("hash", hash.Hex()))
							return
						} else {
							return
						}
					}

					// Ensure we have a valid trace before dispatching
					if trace != nil {
						t.dispatch(trace)
					}
				}(txEvent)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// putTraceInCache returns true if the trace was not in the cache before.
func (t *Tracer) putTraceInCache(hash string) (success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.traceCache.Get(hash); ok {
		return false
	}
	t.traceCache.Set(hash, struct{}{}, cache.WithExpiration(10*time.Minute))
	return true
}

func (t *Tracer) dispatch(trace *core.Trace) {
	if trace.InProgress() {
		traceNumber.With(map[string]string{"type": "dispatched-in-progress"}).Inc()
		return
	}
	traceNumber.With(map[string]string{"type": "dispatched-completed"}).Inc()

	if success := t.putTraceInCache(trace.Hash.Hex()); !success {
		// ok, this trace is already in cache meaning that we already sent it to subscribers.
		// ignore it.
		return
	}
	traceNumber.With(map[string]string{"type": "converted-to-event"}).Inc()

	accounts := core.DistinctAccounts(trace)
	eventData := &TraceEventData{
		AccountIDs: accounts,
		Hash:       trace.Hash.Hex(),
	}

	eventJSON, err := json.Marshal(eventData)
	if err != nil {
		t.logger.Error("json.Marshal() failed: %v", zap.Error(err))
		return
	}

	t.dispatcher.Dispatch(accounts, eventJSON)
}

// shouldProcess checks if we should process this trace
func (t *Tracer) shouldProcess(hash tongo.Bits256) bool {
	return t.putTraceInCache(hash.Hex())
}
