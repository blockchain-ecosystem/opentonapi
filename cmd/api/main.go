package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tonkeeper/opentonapi/pkg/addressbook"
	"github.com/tonkeeper/opentonapi/pkg/api"
	"github.com/tonkeeper/opentonapi/pkg/app"
	"github.com/tonkeeper/opentonapi/pkg/blockchain"
	"github.com/tonkeeper/opentonapi/pkg/blockchain/indexer"
	"github.com/tonkeeper/opentonapi/pkg/config"
	"github.com/tonkeeper/opentonapi/pkg/litestorage"
	"github.com/tonkeeper/opentonapi/pkg/pusher/sources"
	"github.com/tonkeeper/opentonapi/pkg/spam"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/liteapi"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	cfg := config.Load()
	log := app.Logger(cfg.App.LogLevel)

	storageBlockCh := make(chan indexer.IDandBlock, 50000)
	pusherBlockCh := make(chan indexer.IDandBlock, 50000)

	var err error
	var client *liteapi.Client
	if len(cfg.App.LiteServers) == 0 {
		log.Warn("USING PUBLIC CONFIG for NewLiteStorage! BE CAREFUL!")
		client, err = liteapi.NewClientWithDefaultMainnet()
	} else {
		client, err = liteapi.NewClient(liteapi.WithLiteServers(cfg.App.LiteServers))
	}
	if err != nil {
		log.Fatal("failed to create liteapi client", zap.Error(err))
	}

	storage, err := litestorage.NewLiteStorage(
		log,
		client,
		litestorage.WithPreloadAccounts(cfg.App.Accounts),
		litestorage.WithBlockChannel(storageBlockCh),
	)
	if err != nil {
		log.Fatal("storage init", zap.Error(err))
	}

	// Only create AddressBook after successful storage initialization
	book := addressbook.NewAddressBook(log, config.AddressPath, config.JettonPath, config.CollectionPath, storage)

	// The executor is used to resolve DNS records.
	tongo.SetDefaultExecutor(storage)

	// mempool receives a copy of any payload that goes through our API method /v2/blockchain/message
	mempool := sources.NewMemPool(log)
	mempoolCh := mempool.Run(ctx)

	msgSender, err := blockchain.NewMsgSender(log, cfg.App.LiteServers, map[string]chan<- blockchain.ExtInMsgCopy{
		"mempool": mempoolCh,
	})
	if err != nil {
		log.Fatal("failed to create msg sender", zap.Error(err))
	}
	spamFilter := spam.NewSpamFilter()
	h, err := api.NewHandler(log,
		api.WithStorage(storage),
		api.WithAddressBook(book),
		api.WithExecutor(storage),
		api.WithMessageSender(msgSender),
		api.WithSpamFilter(spamFilter),
		api.WithTonConnectSecret(cfg.TonConnect.Secret),
	)
	if err != nil {
		log.Fatal("failed to create api handler", zap.Error(err))
	}
	source := sources.NewBlockchainSource(log, client)
	source.Run(ctx, pusherBlockCh)

	// Add readiness check here
	ready := make(chan struct{})
	go func() {
		for i := 0; i < 30; i++ { // 30 second timeout
			if _, err := client.GetMasterchainInfo(ctx); err == nil {
				close(ready)
				return
			}
			time.Sleep(time.Second)
			log.Warn("waiting for blockchain connection...", zap.Int("attempt", i+1))
		}
		log.Fatal("failed to initialize connection to blockchain")
	}()

	select {
	case <-ready:
		log.Info("service initialized successfully")
	case <-ctx.Done():
		log.Fatal("initialization cancelled")
	}

	tracer := sources.NewTracer(log, storage, source, sources.WithCircuitBreaker())
	go tracer.Run(ctx)

	idx := indexer.New(log, client)
	go idx.Run(ctx, []chan indexer.IDandBlock{
		pusherBlockCh,
		storageBlockCh,
	})

	server, err := api.NewServer(log, h,
		api.WithTransactionSource(source),
		api.WithBlockHeadersSource(source),
		api.WithTraceSource(tracer),
		api.WithMemPool(mempool))
	if err != nil {
		log.Fatal("failed to create api handler", zap.Error(err))
	}

	metricServer := http.Server{
		Addr:    fmt.Sprintf(":%v", cfg.App.MetricsPort),
		Handler: promhttp.Handler(),
	}
	go func() {
		if err := metricServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("listen and serve", zap.Error(err))
		}
	}()

	log.Warn("start server", zap.Int("port", cfg.API.Port))
	server.Run(fmt.Sprintf(":%d", cfg.API.Port), cfg.API.UnixSockets)

	defer cleanup(storage, idx, storageBlockCh, pusherBlockCh)

	// Replace draining with backpressure
	go monitorChannels(ctx, log, storageBlockCh, pusherBlockCh)
}

func cleanup(storage *litestorage.LiteStorage, idx *indexer.Indexer, storageBlockCh, pusherBlockCh chan indexer.IDandBlock) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Graceful shutdown
	storage.Shutdown()

	// Drain channels before closing
	for {
		select {
		case <-storageBlockCh:
		case <-pusherBlockCh:
		case <-ctx.Done():
			close(storageBlockCh)
			close(pusherBlockCh)
			return
		default:
			close(storageBlockCh)
			close(pusherBlockCh)
			return
		}
	}
}

func monitorChannels(ctx context.Context, log *zap.Logger, channels ...chan indexer.IDandBlock) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ch := range channels {
				usage := float64(len(ch)) / float64(cap(ch))
				if usage > 0.8 {
					log.Warn("high channel usage detected",
						zap.Float64("usage", usage))
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}
