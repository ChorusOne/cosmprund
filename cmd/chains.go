package cmd

import (
	db "github.com/cosmos/cosmos-db"
)

type BlockStatePruner func(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error

// custom application.db pruner
type AppPruner func(appStore db.DB, snapshotDB db.DB, dataDir string, dbfmt db.BackendType, pruneHeight uint64) error

// ChainPruner holds the specific pruning functions for a chain.
type ChainPruner struct {
	PruneBlockState BlockStatePruner
	PruneApp        AppPruner
}

// some chains, e.g Babylon, see very little benefit from using the snapshot restore method.
// TODO: evaluate whether or not adding custom thresholds here makes sense. Currently it's 10 GiB.
var chainConfigs = map[string]ChainPruner{
	"pacific-1":     {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: PruneAppState},
	"atlantic-2":    {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: PruneAppState},
	"bbn-test-5":    {PruneBlockState: pruneBlockAndStateStore, PruneApp: PruneAppState},
	"bbn-1":         {PruneBlockState: pruneBlockAndStateStore, PruneApp: PruneAppState},
	"injective-1":   {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp},
	"injective-888": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp},
	"cosmoshub-4":   {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp},
}

func GetPruner(chainID string) ChainPruner {
	if config, ok := chainConfigs[chainID]; ok {
		logger.Info("Using custom pruning configuration", "chain-id", chainID)
		return config
	}
	logger.Info("Using default pruning configuration")
	return ChainPruner{
		PruneBlockState: pruneBlockAndStateStore,
		PruneApp:        SnapshotAndRestoreApp,
	}
}
