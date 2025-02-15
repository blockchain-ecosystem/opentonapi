package litestorage

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/tonkeeper/opentonapi/pkg/core"
)

const ltxKeyPrefix = "ltx:"

func (s *LiteStorage) StoreLiteTransaction(tx *core.LiteTransaction) error {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()

	key := append([]byte(ltxKeyPrefix), tx.Hash[:]...)
	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal lite transaction: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}
