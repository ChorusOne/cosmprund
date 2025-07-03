package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"syscall"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/types"
	"golang.org/x/sync/errgroup"

	db "github.com/cosmos/cosmos-db"
	"github.com/rs/zerolog"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

const GiB uint64 = 1073741824 // 2**30
const THRESHOLD_APP_SIZE uint64 = 10 * GiB

var logger log.Logger

func setConfig(cfg *log.Config) {
	cfg.Level = zerolog.InfoLevel
}
func PruneAppState(dataDir string) error {
	backend, err := GetFormat(filepath.Join(dataDir, "state.db"))
	if err != nil {
		return err
	}

	appDB, err := db.NewDB("application", backend, dataDir)
	if err != nil {
		return err
	}
	defer func() {
		err := appDB.Close()
		if err != nil {
			logger.Error("error (in defer) closing app db: %s", err)
		}
	}()

	logger.Info("pruning application state")

	appStore := rootmulti.NewStore(appDB, logger, metrics.NewNoOpMetrics())
	appStore.SetIAVLDisableFastNode(true)
	ver := rootmulti.GetLatestVersion(appDB)

	storeNames := []string{}
	if ver != 0 {
		cInfo, err := appStore.GetCommitInfo(ver)
		if err != nil {
			return err
		}

		for _, storeInfo := range cInfo.StoreInfos {
			// we only want to prune the stores with actual data.
			// sometimes in-memory stores get leaked to disk without data.
			// if that happens, the store's computed hash is empty as well.
			if len(storeInfo.CommitId.Hash) > 0 {
				storeNames = append(storeNames, storeInfo.Name)
			} else {
				logger.Info("skipping due to empty hash", "store", storeInfo.Name)
			}
		}
	}

	keys := types.NewKVStoreKeys(storeNames...)
	for _, value := range keys {
		appStore.MountStoreWithDB(value, types.StoreTypeIAVL, nil)
	}

	err = appStore.LoadLatestVersion()
	if err != nil {
		return err
	}

	versions := appStore.GetAllVersions()
	if len(versions) > 0 {
		v64 := make([]int64, len(versions))
		for i := range versions {
			v64[i] = int64(versions[i])
		}

		// -1 in case we have exactly 1 block in the DB
		idx := int64(len(v64)) - int64(keepVersions)
		idx = max(idx, int64(len(v64))-1)
		logger.Info("Preparing to prune", "v64", len(v64), "keepVersions", keepVersions, "idx", idx)
		targetHeight := v64[idx] - 1
		logger.Info("Pruning up to", "targetHeight", targetHeight)

		if err := appStore.PruneStores(targetHeight); err != nil {
			logger.Error("error pruning app state: %s", err)
			return err
		}
	}

	return nil
}

// Implement a "GC" pass by copying only live data to a new DB
// This function will CLOSE dbToGC.
func gcDB(dataDir string, dbName string, dbToGC db.DB, dbfmt db.BackendType) error {
	logger.Info("starting garbage collection pass", "db", dbName)
	var newDB db.DB
	var err error

	if dbfmt == db.GoLevelDBBackend {
		opts := opt.Options{WriteBuffer: 1_000_000} // Database will only flush the WAL to a SST file after WriteBuffer is full
		newDB, err = db.NewGoLevelDBWithOpts(fmt.Sprintf("%s_gc", dbName), dataDir, &opts)
	} else {
		newDB, err = db.NewDB(fmt.Sprintf("%s_gc", dbName), dbfmt, dataDir)
	}

	if err != nil {
		logger.Error("Failed to open gc db", "err", err)
		return err
	}

	// Copy only live data
	iter, err := dbToGC.Iterator(nil, nil)
	if err != nil {
		logger.Error("Failed to get original db iterator", "err", err)
		return err
	}
	batchSize := 1_000
	batch := newDB.NewBatch()
	count := 0

	for ; iter.Valid(); iter.Next() {
		_ = batch.Set(iter.Key(), iter.Value())
		count++

		if count >= batchSize {
			if err := batch.Write(); err != nil {
				logger.Error("error writing batch: %s, continuing", err)
			}

			if err := batch.Close(); err != nil {
				logger.Error("error closing batch: %s, continuing", err)
			}
			batch = newDB.NewBatch()
			count = 0
		}
	}
	logger.Info("Finished GC, closing", "db", dbName)

	if count > 0 {
		if err := batch.Write(); err != nil {
			logger.Error("error writing batch: %s, continuing", err)
		}
	}

	_ = iter.Close()

	if err := batch.Close(); err != nil {
		logger.Error("error closing batch: %s, continuing", err)
	}

	if err := dbToGC.Close(); err != nil {
		logger.Error("error closing gc db: %s, continuing", err)
	}

	if err := newDB.Close(); err != nil {
		logger.Error("error closing newdb: %s, continuing", err)
	}

	newPath := filepath.Join(dataDir, fmt.Sprintf("%s_gc.db", dbName))
	if count == 0 {
		logger.Info("gc complete, but empty")
		if err := os.RemoveAll(newPath); err != nil {
			logger.Error("error removing files from %s :%s", newPath, err)
		}
		return nil
	}

	oldPath := filepath.Join(dataDir, fmt.Sprintf("%s.db", dbName))

	if err := os.RemoveAll(oldPath); err != nil {
		logger.Error("error removing files from %s :%s", oldPath, err)
	}
	if err := os.Rename(newPath, oldPath); err != nil {
		logger.Error("Failed to swap GC DB", "err", err)
		return err
	}

	return nil
}

func ChownR(path string, uid, gid int) error {
	logger.Info("Running chown", "path", path, "uid", uid, "gid", gid)

	var errs []error

	err := filepath.Walk(path, func(name string, info os.FileInfo, err error) error {
		// here, walk errored. return it immediately
		if err != nil {
			return err
		}

		// chown error: gather them and return in bulk
		if chownErr := os.Chown(name, uid, gid); chownErr != nil {
			errs = append(errs, chownErr)
		}
		return nil
	})

	if err != nil {
		return err
	}

	return errors.Join(errs...)
}

// PruneCmtData prunes the cometbft blocks and state based on the amount of blocks to keep
func PruneCmtData(dataDir string) error {

	logger.Info("Pruning CMT data")
	curState, err := DbState(dataDir)
	if err != nil {
		return err
	}

	dbfmt, err := GetFormat(filepath.Join(dataDir, "state.db"))
	if err != nil {
		return err
	}
	stateStoreDB, err := db.NewDB("state", dbfmt, dataDir)
	if err != nil {
		return err
	}
	blockStoreDB, err := db.NewDB("blockstore", dbfmt, dataDir)
	if err != nil {
		return err
	}
	appStoreDB, err := db.NewDB("application", dbfmt, dataDir)
	if err != nil {
		return err
	}

	logger.Info("Initial state", "ChainId", curState.ChainID, "LastBlockHeight", curState.LastBlockHeight)
	pruneHeight := uint64(curState.LastBlockHeight) - keepBlocks
	logger.Info("Pruning up to", "targetHeight", pruneHeight)
	isSei := slices.Contains([]string{"pacific-1", "atlantic-2"}, curState.ChainID)

	if !isSei {
		err = pruneBlockAndStateStore(blockStoreDB, stateStoreDB, appStoreDB, pruneHeight)
	} else {
		err = pruneSeiBlockAndStateStore(blockStoreDB, stateStoreDB, appStoreDB, pruneHeight)
	}
	if err != nil {
		logger.Error("Failed to prune", "err", err)
		// gcDB closes the databases, and we can't close pebbledb instances twice
		_ = blockStoreDB.Close()
		_ = stateStoreDB.Close()
		_ = appStoreDB.Close()
		return err
	}

	if runGC {
		g, _ := errgroup.WithContext(context.Background())

		g.Go(func() error {
			if err := gcDB(dataDir, "blockstore", blockStoreDB, dbfmt); err != nil {
				logger.Error("Failed to run gcDB", "err", err, "application", appStoreDB, "dbfmt", dbfmt)
				return err
			}
			return nil
		})

		g.Go(func() error {
			if err := gcDB(dataDir, "state", stateStoreDB, dbfmt); err != nil {
				logger.Error("Failed to run gcDB", "err", err, "application", appStoreDB, "dbfmt", dbfmt)
				return err
			}
			return nil
		})

		g.Go(func() error {
			appPath := path.Join(dataDir, "application.db")
			size, err := dirSize(appPath)
			if err != nil {
				logger.Error("Failed to get dir size for app.db, skipping GC", "err", err)
				return err
			}
			if size < THRESHOLD_APP_SIZE || forceCompressApp {
				logger.Info("Starting application DB GC/compact", "sizeGB", size/GiB, "thresholdGB", THRESHOLD_APP_SIZE/GiB, "forced", forceCompressApp)
				if err := gcDB(dataDir, "application", appStoreDB, dbfmt); err != nil {
					logger.Error("Failed to run gcDB", "err", err, "application", appStoreDB, "dbfmt", dbfmt)
					return err
				}
			} else {
				logger.Info("Skipping application DB GC/compact", "sizeGB", size/GiB, "thresholdGB", THRESHOLD_APP_SIZE/GiB, "forced", forceCompressApp)
				if err := appStoreDB.Close(); err != nil {
					logger.Error("error closing app db: %s", err)
				}
			}
			return nil
		})

		return g.Wait()
	} else {
		logger.Info("NOT running GC on state/block stores")
		_ = blockStoreDB.Close()
		_ = stateStoreDB.Close()
		_ = appStoreDB.Close()
	}

	return nil
}

func dirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Warn("cannot access file", "file", filePath, "err", err)
			return nil
		}

		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})

	return size, err
}

func Stat(path string) (int, int, error) {
	stat, err := os.Stat(path)
	if err != nil {
		logger.Error("Failed stat db", "err", err, "path", path)
		return 0, 0, err
	}

	if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid), int(stat.Gid), nil
	}

	return 0, 0, fmt.Errorf("result of stat was not a Stat_t")
}
