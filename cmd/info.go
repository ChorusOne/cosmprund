package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	cmtstorev1 "github.com/cometbft/cometbft/api/cometbft/store/v1"
	cmttypesv1 "github.com/cometbft/cometbft/api/cometbft/types/v1"
	"github.com/cometbft/cometbft/proto/tendermint/state"
	db "github.com/cosmos/cosmos-db"
	gogoproto "github.com/cosmos/gogoproto/proto"
	"github.com/gogo/protobuf/proto"
	sei_state "github.com/tendermint/tendermint/proto/tendermint/state"
)

type LatestState struct {
	ChainID         string    `json:"chain_id"`
	InitialHeight   int64     `json:"initial_height"`
	LastBlockHeight int64     `json:"last_block_height"`
	AppHash         string    `json:"app_hash"`
	LastBlockTime   time.Time `json:"last_block_time"`
	// earliest/base block, read from the blockstore (mirrors CometBFT /status).
	// Empty/zero when the blockstore base is unset or its meta was pruned.
	EarliestBlockHeight int64     `json:"earliest_block_height"`
	EarliestBlockHash   string    `json:"earliest_block_hash"`
	EarliestAppHash     string    `json:"earliest_app_hash"`
	EarliestBlockTime   time.Time `json:"earliest_block_time"`
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
	stateDB, err := db.NewDB("state", dbfmt, dataDir)
	if err != nil {
		return nil, err
	}
	defer stateDB.Close()

	var ls *LatestState
	if stateBytes, err := stateDB.Get([]byte("stateKey")); err == nil && len(stateBytes) > 0 {
		stateData, err := unmarshalState(stateBytes)
		if err != nil {
			return nil, err
		}
		ls = newLatestStateFromStateData(stateData)
	} else if seiStateBytes, err := stateDB.Get([]byte{0x88}); err == nil && len(seiStateBytes) > 0 {
		// sei uses a different set of keys: `prefixState = int64(8)`, but the key is
		// passed through `orderedcode` and becomes 0x88.
		stateData, err := unmarshalSeiState(seiStateBytes)
		if err != nil {
			return nil, err
		}
		ls = newLatestStateFromStateData(stateData)
	} else {
		return nil, fmt.Errorf("error getting state object: %w", err)
	}

	// best-effort earliest/base block info from the blockstore (mirrors /status).
	// non-fatal: latest state is still useful even if the base meta is gone.
	if e, eerr := dbEarliest(dataDir); eerr != nil {
		logger.Warn("could not read earliest block info from blockstore", "err", eerr)
	} else if e != nil {
		ls.EarliestBlockHeight = e.Header.Height
		ls.EarliestBlockHash = fmt.Sprintf("%X", e.BlockID.Hash)
		ls.EarliestAppHash = fmt.Sprintf("%X", e.Header.AppHash)
		ls.EarliestBlockTime = e.Header.Time
	}
	return ls, nil
}

// dbEarliest opens the blockstore, reads its base height, and loads the block meta
// ("H:<base>") for that height. Returns nil (no error) when the base is unset or its
// meta was pruned — same condition under which /status reports empty earliest_* values.
func dbEarliest(dataDir string) (*cmttypesv1.BlockMeta, error) {
	dbfmt, err := GetFormat(filepath.Join(dataDir, "blockstore.db"))
	if err != nil {
		return nil, err
	}
	blockDB, err := db.NewDB("blockstore", dbfmt, dataDir)
	if err != nil {
		return nil, err
	}
	defer blockDB.Close()

	raw, err := blockDB.Get([]byte("blockStore"))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var bss cmtstorev1.BlockStoreState
	if err := gogoproto.Unmarshal(raw, &bss); err != nil {
		return nil, fmt.Errorf("unmarshal BlockStoreState: %w", err)
	}
	if bss.Base <= 0 {
		return nil, nil
	}

	metaRaw, err := blockDB.Get(fmt.Appendf(nil, "H:%d", bss.Base))
	if err != nil {
		return nil, err
	}
	if len(metaRaw) == 0 {
		return nil, nil // base points at a pruned meta
	}
	var meta cmttypesv1.BlockMeta
	if err := gogoproto.Unmarshal(metaRaw, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal BlockMeta H:%d: %w", bss.Base, err)
	}
	return &meta, nil
}
