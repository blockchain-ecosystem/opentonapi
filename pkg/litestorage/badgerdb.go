package litestorage

import (
	"fmt"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type BadgerDBStorage struct {
	db *badger.DB
}

func NewBadgerDBStorage(path string) (*BadgerDBStorage, error) {
	opts := badger.DefaultOptions(path)

	opts.Logger = nil
	badgerInstance, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening kv: %w", err)
	}

	return &BadgerDBStorage{db: badgerInstance}, nil
}


func (k *BadgerDBStorage) Close() error {
	return k.db.Close()
}

// nolint:wrapcheck
func (k *BadgerDBStorage) Exists(key string) (bool, error) {
	var exists bool
	err := k.db.View(
		func(tx *badger.Txn) error {
			if val, err := tx.Get([]byte(key)); err != nil {
				return err
			} else if val != nil {
				exists = true
			}
			return nil
		})
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return exists, err
}

func (k *BadgerDBStorage) Get(key string) (string, error) {
	var value string
	return value, k.db.View(
		func(tx *badger.Txn) error {
			item, err := tx.Get([]byte(key))
			if err != nil {
				return fmt.Errorf("getting value: %w", err)
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("copying value: %w", err)
			}
			value = string(valCopy)
			return nil
		})
}

func (k *BadgerDBStorage) Set(key, value string) error {
	return k.db.Update(
		func(txn *badger.Txn) error {
			return txn.Set([]byte(key), []byte(value))
		})
}

func (k *BadgerDBStorage) Delete(key string) error {
	return k.db.Update(
		func(txn *badger.Txn) error {
			return txn.Delete([]byte(key))
		})
}

