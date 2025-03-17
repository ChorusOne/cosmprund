package cmd

import (
	"fmt"
	"time"

	"github.com/cometbft/cometbft/proto/tendermint/state"
	db "github.com/cosmos/cosmos-db"
	"github.com/gogo/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LatestState struct {
	ChainID         string    `json:"chain_id"`
	InitialHeight   int64     `json:"initial_height"`
	LastBlockHeight int64     `json:"last_block_height"`
	AppHash         string    `json:"app_hash"`
	LastBlockTime   time.Time `json:"last_block_time"`
}

func newLatestStateFromStateData(stateData *state.State) *LatestState {
	return &LatestState{
		ChainID:         stateData.GetChainID(),
		InitialHeight:   stateData.GetInitialHeight(),
		LastBlockHeight: stateData.GetLastBlockHeight(),
		AppHash:         fmt.Sprintf("%X", stateData.GetAppHash()),
		LastBlockTime:   stateData.GetLastBlockTime(),
	}
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
	if err != nil {
		return nil, fmt.Errorf("error getting state object: %w\n", err)
	}

	var stateData state.State
	err = proto.Unmarshal(stateBytes, &stateData)
	if err != nil {
		return nil, fmt.Errorf("error parsing state object: %w\n", err)
	}

	latestState := newLatestStateFromStateData(&stateData)
	return latestState, nil
}
