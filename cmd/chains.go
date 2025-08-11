package cmd

import (
	"encoding/binary"
	"fmt"

	db "github.com/cosmos/cosmos-db"
)

type BlockStatePruner func(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error

// custom application.db pruner
type AppPruner func(appStore db.DB, pruneHeight, keepVersions uint64) error

// ChainPruner holds the specific pruning functions for a chain.
type ChainPruner struct {
	PruneBlockState BlockStatePruner
	PruneApp        AppPruner
}

func pruneInjectiveAppStore(appStore db.DB, pruneHeight, keepVersions uint64) error {
	logger.Info("Running Injective-specific application pruner")

	prefixesToPrune := map[string]int{
		"s/k:oracle/s": 12,
		"s/k:wasm/s":   10,
		"s/k:ibc/s":    9,
		"s/k:bank/s":   10,
		"s/k:peggy/s":  11,
		"s/k:acc/s":    9,
	}

	for p, prefixLen := range prefixesToPrune {
		prefix := []byte(p)
		currentPrefixLen := prefixLen

		count, err := deleteAllByPrefix(appStore, prefix, func(key, value []byte) (bool, error) {
			if len(key) < currentPrefixLen+8 {
				return false, nil
			}
			heightBytes := key[currentPrefixLen : currentPrefixLen+8]
			height := binary.BigEndian.Uint64(heightBytes)
			return height < pruneHeight, nil
		})

		if err != nil {
			return fmt.Errorf("prune injective app store prefix %q: %w", string(prefix), err)
		}
		logger.Info("Pruned", "store", "application", "key", fmt.Sprintf("%q", prefix), "count", count)
	}

	return PruneAppState(appStore)
}

func pruneBabylonAppStore(appStore db.DB, pruneHeight, keepVersions uint64) error {
	logger.Info("Running Babylon-specifi application pruner")

	prefixesToPrune := map[string]int{
		"s/k:finality/f\x01": 15,
		"s/k:finality/f\x02": 15,
		"s/k:finality/f\x03": 15,
		"s/k:finality/s\x00": 15,
		"s/k:btcstaking/s":   16,
	}

	for p, prefixLen := range prefixesToPrune {
		prefix := []byte(p)
		currentPrefixLen := prefixLen

		count, err := deleteAllByPrefix(appStore, prefix, func(key, value []byte) (bool, error) {
			if len(key) < currentPrefixLen+8 {
				return false, nil
			}
			heightBytes := key[currentPrefixLen : currentPrefixLen+8]
			height := binary.BigEndian.Uint64(heightBytes)
			return height < pruneHeight, nil
		})

		if err != nil {
			return fmt.Errorf("prune babylon app store prefix %q: %w", string(prefix), err)
		}
		logger.Info("Pruned", "store", "application", "key", fmt.Sprintf("%q", prefix), "count", count)
	}

	return PruneAppState(appStore)
}

var chainConfigs = map[string]ChainPruner{
	"pacific-1":     {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: pruneAppStore},
	"atlantic-2":    {PruneBlockState: pruneSeiBlockAndStateStore, PruneApp: pruneAppStore},
	"bbn-test-5":    {PruneBlockState: pruneBlockAndStateStore, PruneApp: pruneBabylonAppStore},
	"bbn-1":         {PruneBlockState: pruneBlockAndStateStore, PruneApp: pruneBabylonAppStore},
	"injective-1":   {PruneBlockState: pruneBlockAndStateStore, PruneApp: pruneInjectiveAppStore},
	"injective-888": {PruneBlockState: pruneBlockAndStateStore, PruneApp: pruneInjectiveAppStore},
}

func GetPruner(chainID string) ChainPruner {
	if config, ok := chainConfigs[chainID]; ok {
		logger.Info("Using custom pruning configuration", "chain-id", chainID)
		return config
	}
	logger.Info("Using default pruning configuration")
	return ChainPruner{
		PruneBlockState: pruneBlockAndStateStore,
		PruneApp:        pruneAppStore,
	}
}
