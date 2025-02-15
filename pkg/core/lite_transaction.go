package core

import (
	"github.com/tonkeeper/tongo"
)

type LiteTransaction struct {
	Hash      tongo.Bits256
	Lt        uint64
	AccountID tongo.AccountID
	BlockID   tongo.BlockID
}

func ConvertToLiteTransaction(tx *Transaction) *LiteTransaction {
	return &LiteTransaction{
		Hash:      tx.Hash,
		Lt:        tx.Lt,
		AccountID: tx.Account,
		BlockID:   tx.BlockID,
	}
}
