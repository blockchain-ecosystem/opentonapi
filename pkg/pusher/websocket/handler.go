package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tonkeeper/opentonapi/pkg/pusher/sources"
)

var (
	upgrader websocket.Upgrader // use default options
)

const (
	writeTimeout = 10 * time.Second
	readTimeout  = 60 * time.Second
	pingPeriod   = (readTimeout * 9) / 10
	maxMessageSize = 1024 * 1024 // 1MB
)

type WSMetrics struct {
	activeConnections prometheus.Gauge
	messagesSent     prometheus.Counter
	messagesReceived prometheus.Counter
	errors           prometheus.CounterVec
}

type JsonRPCRequest struct {
	ID      uint64   `json:"id,omitempty"`
	JSONRPC string   `json:"jsonrpc,omitempty"`
	Method  string   `json:"method,omitempty"`
	Params  []string `json:"params,omitempty"`
}

type JsonRPCResponse struct {
	ID      uint64          `json:"id,omitempty"`
	JSONRPC string          `json:"jsonrpc,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func initWSMetrics() *WSMetrics {
	return &WSMetrics{}
}

func Handler(logger *zap.Logger, txSource sources.TransactionSource, traceSource sources.TraceSource, mempool sources.MemPoolSource, blockSource sources.BlockHeadersSource) func(http.ResponseWriter, *http.Request, int, bool) error {
	metrics := initWSMetrics()

	return func(w http.ResponseWriter, r *http.Request, connectionType int, allowTokenInQuery bool) error {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			metrics.errors.WithLabelValues("upgrade").Inc()
			return err
		}

		// Set connection constraints
		conn.SetReadLimit(maxMessageSize)
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			return nil
		})

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Start ping/pong handler
		go func() {
			ticker := time.NewTicker(pingPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeTimeout)); err != nil {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		metrics.activeConnections.Inc()
		defer metrics.activeConnections.Dec()

		metrics.messagesSent.Inc()

		metrics.messagesReceived.Inc()

		session := newSession(logger, txSource, traceSource, mempool, blockSource, conn)
		requestCh := session.Run(ctx)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					return nil
				}
				metrics.errors.WithLabelValues("read").Inc()
				return err
			}
			var request JsonRPCRequest
			if err = json.Unmarshal(msg, &request); err != nil {
				logger.Error("request unmarshalling error", zap.Error(err))
				metrics.errors.WithLabelValues("unmarshal").Inc()
				return err
			}
			requestCh <- request
		}
	}
}
