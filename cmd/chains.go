package cmd

import (
	db "github.com/cosmos/cosmos-db"
)

type BlockStatePruner func(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error

// custom application.db pruner
type AppPruner func(appStore db.DB, snapshotDB db.DB, dataDir string,
	dbfmt db.BackendType, pruneHeight uint64, snapshotRestoreThreshold float64) (snapshotted bool, err error)

// ChainPruner holds the specific pruning functions for a chain.
type ChainPruner struct {
	PruneBlockState          BlockStatePruner
	PruneApp                 AppPruner
	SnapshotRestoreThreshold float64
}

// some chains, e.g Babylon, see very little benefit from using the snapshot restore method.
// TODO: see if custom implementation for Babylon makes sense.
var chainConfigs = map[string]ChainPruner{
	"pacific-1":      {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: PruneAppState},
	"atlantic-2":     {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: PruneAppState},
	"injective-1":    {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"injective-888":  {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"stride-1":       {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"cosmoshub-4":    {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 20 * GiB},
	"osmosis-1":      {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 30 * GiB},
	"dydx-testnet-4": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 1 * GiB},
	"dydx-mainnet-1": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 1 * GiB},
	"axelar-dojo-1":  {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 25 * GiB},
	"tacchain_239-1": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 5 * GiB},
	"neutron-1":      {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 30 * GiB},
	"noble-1":        {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"laozi-mainnet":  {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"celestia":       {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
}

func GetPruner(chainID string) ChainPruner {
	if config, ok := chainConfigs[chainID]; ok {
		logger.Info("Using custom pruning configuration", "chain-id", chainID)
		return config
	}
	logger.Info("Using default pruning configuration")
	return ChainPruner{
		PruneBlockState: pruneBlockAndStateStore,
		PruneApp:        PruneAppState,
	}
}
