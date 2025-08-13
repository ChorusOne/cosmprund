package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"cosmossdk.io/log"
	"cosmossdk.io/store"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/snapshots"
	"cosmossdk.io/store/types"
	storetypes "cosmossdk.io/store/types"
	"golang.org/x/sync/errgroup"

	snapshottypes "cosmossdk.io/store/snapshots/types"

	db "github.com/cosmos/cosmos-db"
	"github.com/rs/zerolog"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

const GiB uint64 = 1073741824 // 2**30
const appSizeThreshold uint64 = 10 * GiB

var logger log.Logger

func setConfig(cfg *log.Config) {
	cfg.Level = zerolog.InfoLevel
}

func PruneAppState(appDB db.DB, _ db.DB, _ string, _ db.BackendType, _ uint64) error {
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

	err := appStore.LoadLatestVersion()
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
			logger.Error("error pruning app state", "err", err)
		}
	}

	return nil
}

// this essentially "statesyncs" the application db
func SnapshotAndRestoreApp(appDB db.DB, snapshotDB db.DB, dataDir string, dbfmt db.BackendType, pruneHeight uint64) error {
	appPath := filepath.Join(dataDir, "application.db")
	size, err := dirSize(appPath)
	if err != nil {
		logger.Error("cannot calculate app path, bailing")
		return err
	}

	if size < appSizeThreshold {
		logger.Error("size of application database is too small for snapshot restore", "size", size/GiB, "threshold", appSizeThreshold/GiB)
		return nil
	}
	logger.Info("pruning application state via snapshot", "pruneHeight", pruneHeight)

	appStore := rootmulti.NewStore(appDB, logger, metrics.NewNoOpMetrics())
	appStore.SetIAVLDisableFastNode(true)

	ver := rootmulti.GetLatestVersion(appDB)
	logger.Info("latest version", "latest", ver)

	if ver == 0 {
		logger.Info("no versions to prune")
		return nil
	}

	cInfo, err := appStore.GetCommitInfo(ver)
	if err != nil {
		return fmt.Errorf("failed to get commit info: %w", err)
	}

	storeNames := []string{}
	for _, storeInfo := range cInfo.StoreInfos {
		if len(storeInfo.CommitId.Hash) > 0 {
			storeNames = append(storeNames, storeInfo.Name)
			logger.Info("including store", "store", storeInfo.Name)
		} else {
			logger.Info("skipping due to empty hash", "store", storeInfo.Name)
		}
	}

	keys := storetypes.NewKVStoreKeys(storeNames...)
	for _, key := range keys {
		appStore.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	}

	targetVersion := uint64(ver)

	logger.Info("loading version for snapshot", "version", targetVersion)
	if err := appStore.LoadVersion(int64(targetVersion)); err != nil {
		return fmt.Errorf("failed to load version %d: %w", targetVersion, err)
	}

	tmpDir, err := os.MkdirTemp("", "cosmprund-snapshot-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			logger.Error("error (in defer) removing tmpDir", "err", err)
		}
	}()

	snapshotStore, err := snapshots.NewStore(snapshotDB, tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	opts := snapshottypes.NewSnapshotOptions(1500, 2)
	snapshotManager := snapshots.NewManager(snapshotStore, opts, appStore, nil, logger)

	logger.Info("creating snapshot", "height", targetVersion)
	snapshot, err := snapshotManager.Create(targetVersion)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	logger.Info("snapshot created, removing old application.db")

	_ = appDB.Close()
	if err := os.RemoveAll(filepath.Join(dataDir, "application.db")); err != nil {
		return fmt.Errorf("failed to remove application.db: %w", err)
	}
	appDB, err = db.NewDB("application", dbfmt, dataDir)
	if err != nil {
		return fmt.Errorf("failed to recreate application DB: %w", err)
	}

	freshStore := store.NewCommitMultiStore(appDB, logger, metrics.NewNoOpMetrics())
	freshStore.SetIAVLDisableFastNode(true)
	for _, key := range keys {
		freshStore.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	}

	if err := freshStore.LoadLatestVersion(); err != nil {
		return fmt.Errorf("failed to load fresh store: %w", err)
	}
	snapshotManager = snapshots.NewManager(snapshotStore, opts, freshStore, nil, logger)

	logger.Info("proceeding with snapshot restore")
	if err := snapshotManager.RestoreLocalSnapshot(snapshot.Height, snapshot.Format); err != nil {
		return fmt.Errorf("failed to restore local snapshot: %w", err)
	}

	logger.Info("snapshot sucessfully restored", "height")
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
				logger.Error("error writing batch, continuing", "err", err)
			}

			if err := batch.Close(); err != nil {
				logger.Error("error closing batch: continuing", "err", err)
			}
			batch = newDB.NewBatch()
			count = 0
		}
	}
	logger.Info("Finished GC, closing", "db", dbName)

	if count > 0 {
		if err := batch.Write(); err != nil {
			logger.Error("error writing batch, continuing", "err", err)
		}
	}

	_ = iter.Close()

	if err := batch.Close(); err != nil {
		logger.Error("error closing batch, continuing", "err", err)
	}

	if err := dbToGC.Close(); err != nil {
		logger.Error("error closing gc db, continuing", "err", err)
	}

	if err := newDB.Close(); err != nil {
		logger.Error("error closing newdb, continuing", "err", err)
	}

	newPath := filepath.Join(dataDir, fmt.Sprintf("%s_gc.db", dbName))
	if count == 0 {
		logger.Info("gc complete, but empty")
		if err := os.RemoveAll(newPath); err != nil {
			logger.Error("error removing files", "path", newPath, "err", err)
		}
		return nil
	}

	oldPath := filepath.Join(dataDir, fmt.Sprintf("%s.db", dbName))

	if err := os.RemoveAll(oldPath); err != nil {
		logger.Error("error removing files", "path", oldPath, "err", err)
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

// Prune is the main entrypoint for the pruning process.
func Prune(dataDir string, pruneComet, pruneApp bool) error {
	logger.Info("Starting pruning process...")

	curState, err := DbState(dataDir)
	if err != nil {
		return err
	}

	pruneHeight := uint64(curState.LastBlockHeight) - keepBlocks
	logger.Info("Initial state", "ChainId", curState.ChainID, "LastBlockHeight", curState.LastBlockHeight)
	logger.Info("Pruning up to", "targetHeight", pruneHeight)

	pruner := GetPruner(curState.ChainID)

	dbfmt, err := GetFormat(filepath.Join(dataDir, "state.db"))
	if err != nil {
		return err
	}

	var stateStoreDB, blockStoreDB, appStoreDB db.DB

	defer func() {
		if stateStoreDB != nil {
			_ = stateStoreDB.Close()
		}
		if blockStoreDB != nil {
			_ = blockStoreDB.Close()
		}
		if appStoreDB != nil {
			_ = appStoreDB.Close()
		}
	}()

	var wg sync.WaitGroup
	errorChan := make(chan error, 2)

	if pruneApp {
		logger.Info("Pruning application data")
		appStoreDB, err = db.NewDB("application", dbfmt, dataDir)
		if err != nil {
			return err
		}
		snapshotDB, err := db.NewDB("snapshots/metadata", dbfmt, dataDir)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pruner.PruneApp(appStoreDB, snapshotDB, dataDir, dbfmt, pruneHeight); err != nil {
				errorChan <- fmt.Errorf("failed to prune application DB: %w", err)
			}
		}()
	}

	if pruneComet {
		logger.Info("Pruning CometBFT data (blockstore and state)")
		stateStoreDB, err = db.NewDB("state", dbfmt, dataDir)
		if err != nil {
			return err
		}
		blockStoreDB, err = db.NewDB("blockstore", dbfmt, dataDir)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pruner.PruneBlockState(blockStoreDB, stateStoreDB, pruneHeight); err != nil {
				errorChan <- fmt.Errorf("failed to prune blockstore/state DBs: %w", err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	var errs []error
	for err := range errorChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	if runGC {
		g, _ := errgroup.WithContext(context.Background())

		if pruneComet && blockStoreDB != nil && stateStoreDB != nil {
			dbToGCOnBlock := blockStoreDB
			g.Go(func() error {
				if err := gcDB(dataDir, "blockstore", dbToGCOnBlock, dbfmt); err != nil {
					logger.Error("Failed to run gcDB on blockstore", "err", err)
					return err
				}
				return nil
			})
			blockStoreDB = nil

			dbToGCOnState := stateStoreDB
			g.Go(func() error {
				if err := gcDB(dataDir, "state", dbToGCOnState, dbfmt); err != nil {
					logger.Error("Failed to run gcDB on state", "err", err)
					return err
				}
				return nil
			})
			stateStoreDB = nil
		}

		if pruneApp && appStoreDB != nil {
			dbToGCOnApp := appStoreDB
			g.Go(func() error {
				appPath := filepath.Join(dataDir, "application.db")
				size, err := dirSize(appPath)
				if err != nil {
					logger.Error("Failed to get dir size for app.db, skipping GC", "err", err)
					return err
				}
				if size < appSizeThreshold || forceCompressApp {
					logger.Info("Starting application DB GC/compact", "sizeGB", size/GiB, "thresholdGB", appSizeThreshold/GiB, "forced", forceCompressApp)
					if err := gcDB(dataDir, "application", dbToGCOnApp, dbfmt); err != nil {
						logger.Error("Failed to run gcDB on application", "err", err)
						return err
					}
				} else {
					logger.Info("Skipping application DB GC/compact due to size", "sizeGB", size/GiB, "thresholdGB", appSizeThreshold/GiB)
				}
				return nil
			})
			appStoreDB = nil
		}

		if err := g.Wait(); err != nil {
			return fmt.Errorf("GC process failed: %w", err)
		}
	} else {
		logger.Info("Skipping GC pass")
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
