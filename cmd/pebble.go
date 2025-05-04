package cmd

import (
	pebble "github.com/cockroachdb/pebble"
)

func NewPebbleDBAdapter(db *pebble.DB) DBAdapter {
	return &pebbleDBAdapter{db: db}
}

type pebbleDBAdapter struct {
	db *pebble.DB
}

func (a *pebbleDBAdapter) Get(key []byte) ([]byte, error) {
	r, _, e := a.db.Get(key)
	return r, e
}

func (a *pebbleDBAdapter) Set(key, value []byte) error {
	return a.db.Set(key, value, nil)
}

func (a *pebbleDBAdapter) Delete(key []byte) error {
	return a.db.Delete(key, nil)
}

func (a *pebbleDBAdapter) NewBatch() BatchAdapter {
	return &pebbleBatchAdapter{batch: a.db.NewBatch()}
}

func (a *pebbleDBAdapter) Iterator(start, end []byte) (IteratorAdapter, error) {
	var opt *pebble.IterOptions
	if start == nil && end == nil {
		opt = nil
	} else {
		opt = &pebble.IterOptions{LowerBound: start, UpperBound: end}
	}

	iter, err := a.db.NewIter(opt)
	iter.First()
	if err != nil {
		return nil, err
	}
	return &pebbletIteratorAdapter{iter: *iter}, nil
}

func (a *pebbleDBAdapter) Close() error {
	return a.db.Close()
}

func (a *pebbleDBAdapter) Stats() map[string]string {
	// TODO?
	return make(map[string]string)
}

// Comet Batch implementation
type pebbleBatchAdapter struct {
	batch *pebble.Batch
}

func (b *pebbleBatchAdapter) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *pebbleBatchAdapter) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *pebbleBatchAdapter) Write() error {
	return b.batch.Commit(nil)
}

func (b *pebbleBatchAdapter) Close() error {
	return b.batch.Close()
}

// Comet Iterator implementation
type pebbletIteratorAdapter struct {
	iter pebble.Iterator
}

func (i *pebbletIteratorAdapter) Valid() bool {
	valid := i.iter.Valid()
	return valid
}

func (i *pebbletIteratorAdapter) Next() {
	i.iter.Next()
}

func (i *pebbletIteratorAdapter) Key() []byte {
	return i.iter.Key()
}

func (i *pebbletIteratorAdapter) Value() []byte {
	return i.iter.Value()
}

func (i *pebbletIteratorAdapter) Error() error {
	return i.iter.Error()
}

func (i *pebbletIteratorAdapter) Close() error {
	return i.iter.Close()
}
