package cmd

import (
	cometdb "github.com/cometbft/cometbft-db"
	cosmosdb "github.com/cosmos/cosmos-db"
)

// DBAdapter defines a generic interface that can work with both
// cosmos/cosmos-db and cometbft/cometbft-db implementations
type DBAdapter interface {
	// Core database operations
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error

	// Batch operations
	NewBatch() BatchAdapter

	// Iterator operations
	Iterator(start, end []byte) (IteratorAdapter, error)

	// Management operations
	Close() error
	Stats() map[string]string
}

// BatchAdapter defines the generic batch operations
type BatchAdapter interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
}

// IteratorAdapter defines the generic iterator operations
type IteratorAdapter interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

// NewCosmosDBAdapter wraps a cosmos-db instance to implement the DBAdapter interface
func NewCosmosDBAdapter(db cosmosdb.DB) DBAdapter {
	return &cosmosDBAdapter{db: db}
}

// NewCometDBAdapter wraps a cometbft-db instance to implement the DBAdapter interface
func NewCometDBAdapter(db cometdb.DB) DBAdapter {
	return &cometDBAdapter{db: db}
}

// Cosmos DB implementation
type cosmosDBAdapter struct {
	db cosmosdb.DB
}

func (a *cosmosDBAdapter) Get(key []byte) ([]byte, error) {
	return a.db.Get(key)
}

func (a *cosmosDBAdapter) Set(key, value []byte) error {
	return a.db.Set(key, value)
}

func (a *cosmosDBAdapter) Delete(key []byte) error {
	return a.db.Delete(key)
}

func (a *cosmosDBAdapter) NewBatch() BatchAdapter {
	return &cosmosBatchAdapter{batch: a.db.NewBatch()}
}

func (a *cosmosDBAdapter) Iterator(start, end []byte) (IteratorAdapter, error) {
	iter, err := a.db.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cosmosIteratorAdapter{iter: iter}, nil
}

func (a *cosmosDBAdapter) Close() error {
	return a.db.Close()
}

func (a *cosmosDBAdapter) Stats() map[string]string {
	return a.db.Stats()
}

// Cosmos Batch implementation
type cosmosBatchAdapter struct {
	batch cosmosdb.Batch
}

func (b *cosmosBatchAdapter) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *cosmosBatchAdapter) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *cosmosBatchAdapter) Write() error {
	return b.batch.Write()
}

// Cosmos Iterator implementation
type cosmosIteratorAdapter struct {
	iter cosmosdb.Iterator
}

func (i *cosmosIteratorAdapter) Valid() bool {
	return i.iter.Valid()
}

func (i *cosmosIteratorAdapter) Next() {
	i.iter.Next()
}

func (i *cosmosIteratorAdapter) Key() []byte {
	return i.iter.Key()
}

func (i *cosmosIteratorAdapter) Value() []byte {
	return i.iter.Value()
}

func (i *cosmosIteratorAdapter) Error() error {
	return i.iter.Error()
}

func (i *cosmosIteratorAdapter) Close() error {
	return i.iter.Close()
}

// Comet DB implementation
type cometDBAdapter struct {
	db cometdb.DB
}

func (a *cometDBAdapter) Get(key []byte) ([]byte, error) {
	return a.db.Get(key)
}

func (a *cometDBAdapter) Set(key, value []byte) error {
	return a.db.Set(key, value)
}

func (a *cometDBAdapter) Delete(key []byte) error {
	return a.db.Delete(key)
}

func (a *cometDBAdapter) NewBatch() BatchAdapter {
	return &cometBatchAdapter{batch: a.db.NewBatch()}
}

func (a *cometDBAdapter) Iterator(start, end []byte) (IteratorAdapter, error) {
	iter, err := a.db.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cometIteratorAdapter{iter: iter}, nil
}

func (a *cometDBAdapter) Close() error {
	return a.db.Close()
}

func (a *cometDBAdapter) Stats() map[string]string {
	return a.db.Stats()
}

// Comet Batch implementation
type cometBatchAdapter struct {
	batch cometdb.Batch
}

func (b *cometBatchAdapter) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *cometBatchAdapter) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *cometBatchAdapter) Write() error {
	return b.batch.Write()
}

// Comet Iterator implementation
type cometIteratorAdapter struct {
	iter cometdb.Iterator
}

func (i *cometIteratorAdapter) Valid() bool {
	return i.iter.Valid()
}

func (i *cometIteratorAdapter) Next() {
	i.iter.Next()
}

func (i *cometIteratorAdapter) Key() []byte {
	return i.iter.Key()
}

func (i *cometIteratorAdapter) Value() []byte {
	return i.iter.Value()
}

func (i *cometIteratorAdapter) Error() error {
	return i.iter.Error()
}

func (i *cometIteratorAdapter) Close() error {
	return i.iter.Close()
}
