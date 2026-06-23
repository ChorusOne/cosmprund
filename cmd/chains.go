package cmd

import (
	"fmt"

	db "github.com/cosmos/cosmos-db"
)

type BlockStatePruner func(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error

// custom application.db pruner
type AppPruner func(args *ApplicationPrunerParams) (snapshotted bool, err error)

type ApplicationPrunerParams struct {
	appDB                    db.DB
	snapshotDB               db.DB
	dataDir                  string
	dbfmt                    db.BackendType
	pruneHeight              uint64
	snapshotRestoreThreshold float64
	iavlDisableFastNode      bool
}

func (a ApplicationPrunerParams) String() string {
	return fmt.Sprintf("app pruner params [data dir: %s, db format: %s, prune height: %d, snapshot restore threshold: %f, iavl disable fast node: %t]",
		a.dataDir, a.dbfmt, a.pruneHeight, a.snapshotRestoreThreshold, a.iavlDisableFastNode)
}

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
	"cosmoshub-4":    {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 30 * GiB},
	"osmosis-1":      {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 40 * GiB},
	"dydx-testnet-4": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 1 * GiB},
	"dydx-mainnet-1": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 1 * GiB},
	// "axelar-dojo-1":  {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 25 * GiB},
	"tacchain_239-1": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 5 * GiB},
	"neutron-1":      {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 30 * GiB},
	"noble-1":        {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 10 * GiB},
	"laozi-mainnet":  {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 25 * GiB},
	// celestia/mocha use a forked iavl (v1.2.8) + sdk/store stack. SnapshotAndRestoreApp
	// rebuilds the IAVL tree via export/import using this tool's upstream iavl (v1.2.0),
	// which rehashes inner nodes differently and breaks the app hash (consensus rejects the
	// node, unrecoverable). PruneAppState deletes old versions in place and never rehashes
	// the kept tree, so the on-disk commit info / app hash stays valid. See
	// docs/celestia-pruner-recovery-plan.md.
	"celestia":       {PruneBlockState: pruneBlockAndStateStore, PruneApp: PruneAppState},
	"mocha-4":        {PruneBlockState: pruneBlockAndStateStore, PruneApp: PruneAppState},
	"heimdallv2-137": {PruneBlockState: pruneBlockAndStateStore, PruneApp: SnapshotAndRestoreApp, SnapshotRestoreThreshold: 5 * GiB},
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
