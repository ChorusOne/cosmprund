package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/cometbft/cometbft/proto/tendermint/state"
	db "github.com/cosmos/cosmos-db"
	"github.com/gogo/protobuf/proto"
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

func DbState(dataDir string) (*LatestState, error) {
	dbfmt, err := GetFormat(filepath.Join(dataDir, "state.db"))
	if err != nil {
		return nil, err
	}
	db, err := db.NewDB("state", dbfmt, dataDir)
	if err != nil {
		return nil, err
	}

	stateBytes, err := db.Get([]byte("stateKey"))
	if err == nil && len(stateBytes) > 0 {
		db.Close()
		state, err := unmarshalState(stateBytes)
		if err != nil {
			return nil, err
		}
		return newLatestStateFromStateData(state), nil
	}

	// sei uses different set of keys:
	// `prefixState = int64(8)`
	// but the key will be passed through `orderedcode`, and become 0x88
	seiStateBytes, err := db.Get([]byte{0x88})
	if err == nil && len(seiStateBytes) > 0 {
		db.Close()
		state, err := unmarshalSeiState(seiStateBytes)
		if err != nil {
			return nil, err
		}
		return newLatestStateFromStateData(state), nil
	}
	return nil, fmt.Errorf("error getting state object: %w\n", err)
}
