package cmd

import (
	"fmt"
	"time"

	"github.com/cometbft/cometbft/proto/tendermint/state"
	db "github.com/cosmos/cosmos-db"
	"github.com/gogo/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb/opt"
	sei_state "github.com/tendermint/tendermint/proto/tendermint/state"
)

type LatestState struct {
	ChainID         string    `json:"chain_id"`
	InitialHeight   int64     `json:"initial_height"`
	LastBlockHeight int64     `json:"last_block_height"`
	AppHash         string    `json:"app_hash"`
	LastBlockTime   time.Time `json:"last_block_time"`
}

type State interface {
	GetChainID() string
	GetInitialHeight() int64
	GetLastBlockHeight() int64
	GetLastBlockTime() time.Time
	GetAppHash() []byte
}

func newLatestStateFromStateData(stateData State) *LatestState {
	return &LatestState{
		ChainID:         stateData.GetChainID(),
		InitialHeight:   stateData.GetInitialHeight(),
		LastBlockHeight: stateData.GetLastBlockHeight(),
		AppHash:         fmt.Sprintf("%X", stateData.GetAppHash()),
		LastBlockTime:   stateData.GetLastBlockTime(),
	}
}

func unmarshalState(stateBytes []byte) (State, error) {
	var stateData state.State
	err := proto.Unmarshal(stateBytes, &stateData)
	return &stateData, err
}
func unmarshalSeiState(stateBytes []byte) (State, error) {
	var stateData sei_state.State
	err := proto.Unmarshal(stateBytes, &stateData)
	return &stateData, err
}
func ShowDbState(dataDir string) (*LatestState, error) {
	levelOptions := opt.Options{
		ReadOnly: true,
	}
	db, err := db.NewGoLevelDBWithOpts("state", dataDir, &levelOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating database: %w\n", err)
	}
	defer func() {
		db.Close()
	}()

	readOptions := opt.ReadOptions{
		DontFillCache: true,
		Strict:        opt.DefaultStrict,
	}

	stateBytes, err := db.DB().Get([]byte("stateKey"), &readOptions)
	if err == nil {
		state, err := unmarshalState(stateBytes)
		if err != nil {
			return nil, err
		}
		return newLatestStateFromStateData(state), nil
	}

	// sei uses different set of keys:
	// `prefixState = int64(8)`
	// but the key will be passed through `orderedcode`, and become 0x88
	seiStateBytes, err := db.DB().Get([]byte{0x88}, &readOptions)
	if err == nil {
		state, err := unmarshalSeiState(seiStateBytes)
		if err != nil {
			return nil, err
		}
		return newLatestStateFromStateData(state), nil
	}
	return nil, fmt.Errorf("error getting state object: %w\n", err)
}
