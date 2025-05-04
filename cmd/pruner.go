package cmd

import (
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/types"

	db "github.com/cosmos/cosmos-db"
	"github.com/rs/zerolog"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
	"github.com/google/orderedcode"
)

const GiB uint64 = 1073741824 // 2**30

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
		for i := 0; i < len(versions); i++ {
			v64[i] = int64(versions[i])
		}

		// -1 in case we have exactly 1 block in the DB
		idx := int64(len(v64)) - int64(keepVersions)
		idx = max(idx, int64(len(v64))-1)
		logger.Info("Preparing to prune", "v64", len(v64), "keepVersions", keepVersions, "idx", idx)
		targetHeight := v64[idx] - 1
		logger.Info("Pruning up to", "targetHeight", targetHeight)

		appStore.PruneStores(targetHeight)

		logger.Info("Purging commit info from application.db", "targetHeight", ver-1)
		prunedS, err := deleteHeightRange(appDB, "s/", 0, uint64(targetHeight)-1, asciiHeightParser)
		if err != nil {
			logger.Error("failed to deleteHeightRange")
			return err
		}
		logger.Info("purged", "count", prunedS)

	}

	appPath := path.Join(dataDir, "application.db")
	size, err := dirSize(appPath)
	if size < 10*GiB {
		if runGC {
			logger.Info("Starting application DB GC/compact as it's smaller than 10GB", "sizeGB", size/GiB)
			if err := gcDB(dataDir, "application", appDB, backend); err != nil {
				return err
			}
		}
	} else {
		logger.Info("Skipping application DB GC/compact as it's bigger than 10GB", "sizeGB", size/GiB)
	}

	stat, err := os.Stat(appPath)
	if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
		err = ChownR(appPath, int(stat.Uid), int(stat.Gid))
		if err != nil {
			logger.Error("Failed to run chown, continuing", "err", err)
		}
	}
	return nil
}

// Implement a "GC" pass by copying only live data to a new DB
// This function will CLOSE dbToGC.
func gcDB(dataDir string, dbName string, dbToGC db.DB, dbfmt db.BackendType) error {
	logger.Info("starting garbage collection pass", "db", dbName)
	newDB, err := db.NewDB(fmt.Sprintf("%s_gc", dbName), dbfmt, dataDir)
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
	batchSize := 10_000
	batch := newDB.NewBatch()
	count := 0

	for ; iter.Valid(); iter.Next() {
		batch.Set(iter.Key(), iter.Value())
		count++

		if count >= batchSize {
			batch.Write()
			batch.Close()
			batch = newDB.NewBatch()
			count = 0
		}
	}
	logger.Info("Finished GC, closing")

	if count > 0 {
		batch.Write()
		batch.Close()
	}
	iter.Close()

	dbToGC.Close()
	newDB.Close()

	newPath := filepath.Join(dataDir, fmt.Sprintf("%s_gc.db", dbName))
	if count == 0 {
		logger.Info("gc complete, but empty")
		os.RemoveAll(newPath)
		return nil
	}

	oldPath := filepath.Join(dataDir, fmt.Sprintf("%s.db", dbName))

	oldStat, err := os.Stat(oldPath)
	if err != nil {
		logger.Error("Failed stat pre-GC DB", "err", err)
		return err
	}

	os.RemoveAll(oldPath)
	if err := os.Rename(newPath, oldPath); err != nil {
		logger.Error("Failed to swap GC DB", "err", err)
		return err
	}
	if stat, ok := oldStat.Sys().(*syscall.Stat_t); ok {
		err = ChownR(oldPath, int(stat.Uid), int(stat.Gid))
		if err != nil {
			logger.Error("Failed to run chown, continuing", "err", err)
		}
	}

	return nil
}

func ChownR(path string, uid, gid int) error {
	logger.Info("Running chown", "path", path, "uid", uid, "gid", gid)
	return filepath.Walk(path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			err = os.Chown(name, uid, gid)
		}
		return err
	})
}

func pruneBlockAndStateStore(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error {
	type PrefixAndSplitter struct {
		prefix   string
		splitter heightParser
	}
	for _, key := range []PrefixAndSplitter{{"H:", asciiHeightParser},
		{"C:", asciiHeightParser},
		{"EC:", asciiHeightParser},
		{"SC:", asciiHeightParser},
		{"P:", asciiHeightParserTwoParts},
	} {
		prunedEC, err := deleteHeightRange(blockStoreDB, key.prefix, 0, uint64(pruneHeight), key.splitter)
		if err != nil {
			return err
		}
		logger.Info("Pruned", "store", "block", "key", key.prefix, "count", prunedEC)
	}
	for _, key := range []string{"abciResponsesKey:", "consensusParamsKey:"} {
		prunedS, err := deleteHeightRange(stateStoreDB, key, 0, uint64(pruneHeight), asciiHeightParser)
		if err != nil {
			return err
		}
		logger.Info("Pruned", "store", "state", "key", key, "count", prunedS)
	}
	return nil
}
func pruneSeiBlockAndStateStore(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error {
	for _, key := range []int64{0x0, 0x1, 0x2} { // 0x84 is not height but a hash?
		prunedEC, err := deleteSeiRange(blockStoreDB, key, 0, int64(pruneHeight))
		if err != nil {
			return err
		}
		logger.Info("Pruned", "key", key, "count", prunedEC)
	}
	// 0xe == 14 == prefixFinalizeBlockResponses
	prunedS, err := deleteSeiRange(stateStoreDB, 0xe, 0, int64(pruneHeight))
	if err != nil {
		return err
	}
	logger.Info("Pruned state", "count", prunedS)
	return nil
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

	logger.Info("Initial state", "ChainId", curState.ChainID, "LastBlockHeight", curState.LastBlockHeight)
	pruneHeight := uint64(curState.LastBlockHeight) - keepBlocks
	// PruneBlocks _should_ prune:
	// - Block headers (keys H:<HEIGHT>)
	// - Commit information (keys C:<HEIGHT>)
	// - ExtendedCommit information (keys EC:<HEIGHT>)
	// - SeenCommit (keys SC:<HEIGHT>)
	// - BlockPartKey (keys P:<HEIGHT>:<PART INDEX>)
	// but it does not, so we prune it manually
	// See https://github.com/cometbft/cometbft/blob/4591ef97ce5de702db7d6a3bbcb960ecf635fd76/store/db_key_layout.go#L38
	// https://github.com/cometbft/cometbft/blob/4591ef97ce5de702db7d6a3bbcb960ecf635fd76/store/db_key_layout.go#L68
	// for confirmation of this

	isSei := slices.Contains([]string{"pacific-1", "atlantic-2"}, curState.ChainID)

	if !isSei {
		err = pruneBlockAndStateStore(blockStoreDB, stateStoreDB, pruneHeight)
	} else {
		err = pruneSeiBlockAndStateStore(blockStoreDB, stateStoreDB, pruneHeight)
	}
	if err != nil {
		return err
	}

	if runGC {
		err := gcDB(dataDir, "blockstore", blockStoreDB, dbfmt)
		if err != nil {
			return err
		}
		return gcDB(dataDir, "state", stateStoreDB, dbfmt)
	}
	logger.Info("NOT running GC on state/block stores")
	return nil
}

func deleteSeiRange(db db.DB, key int64, startHeight, endHeight int64) (uint64, error) {
	if key < -64 || key > 64 {
		fmt.Println(key)
		panic("unsupported key") // this allows us to assume that
		// the orderedcode key is always 1 byte
	}

	startKey, err := orderedcode.Append(nil, key, startHeight)
	if err != nil {
		return 0, err
	}
	endKey, err := orderedcode.Append(nil, key, endHeight)
	if err != nil {
		return 0, err
	}

	iter, err := db.Iterator(startKey, endKey)
	if err != nil {
		return 0, err
	}
	pruned := uint64(0)
	for ; iter.Valid(); iter.Next() {
		k := iter.Key()

		var number int64
		remain, err := orderedcode.Parse(string(k[1:]), &number)
		if err != nil {
			fmt.Printf("got err %s\n", err)
			continue
		}
		if len(remain) != 0 && k[0] != 0x81 { // blockPartKey == 0x1 || 0x81 is composed of 2 values.. good enough?
			fmt.Printf("k 0x%x number %d remain %s\n", k[0], number, hex.EncodeToString([]byte(remain)))
			panic("have leftover key material, no idea what to do")
		}
		if number > endHeight {
			continue
		}
		pruned++
		if err := db.Delete(k); err != nil {
			iter.Close()
			return pruned, fmt.Errorf("error deleting key %s: %w", string(k), err)
		}

	}

	iter.Close()
	return pruned, nil
}

type heightParser func(string) (uint64, error)

func asciiHeightParserTwoParts(numberPart string) (uint64, error) {
	parts := strings.SplitN(numberPart, ":", 2)
	return strconv.ParseUint(string(parts[0]), 10, 64)
}

func asciiHeightParser(numberPart string) (uint64, error) {
	return strconv.ParseUint(string(numberPart), 10, 64)
}

// Deletes all keys in the range <key>:<start> to <key>:<end>
// where start and end are left-padded with zeroes to the amount of base-10 digits
// in "end".
// For example, with key="test:", start=0 and end=1000, the keys
// test:0, test:1, ..., test:9, test:10, ..., test:99, test:100, ..., test:999, test:1000 will be deleted
func deleteHeightRange(db db.DB, key string, startHeight, endHeight uint64, heightParser heightParser) (uint64, error) {
	// keys are blobs of bytes, we can't do integer comparison,
	// even if a key looks like C:12345
	// we need to pad the range to match the right amount of digits
	maxDigits := len(fmt.Sprintf("%d", endHeight))
	var pruned uint64 = 0

	logger.Debug("Pruning key", "key", key)
	prunedLastBatch := 0
	for digits := maxDigits; digits >= 1; digits-- {
		rangeStart := uint64(math.Max(float64(startHeight), float64(math.Pow10(digits-1))))
		rangeEnd := uint64(math.Min(float64(endHeight), float64(math.Pow10(digits))-1))

		if rangeStart > rangeEnd {
			continue
		}

		startKey := []byte(fmt.Sprintf("%s%0*d", key, digits, rangeStart))
		endKey := []byte(fmt.Sprintf("%s%0*d", key, digits, rangeEnd))

		iter, err := db.Iterator(startKey, endKey)
		if err != nil {
			return pruned, fmt.Errorf("error creating iterator for digit length %d: %w", digits, err)
		}
		logger.Debug("Pruning range", "Start", string(startKey), "end", string(endKey))

		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			// The keys are of format <key><height>
			// but <height> is an ascii-encoded integer; so when we query by range
			// we _will_ get keys which are beyond the expected maximum.
			// Parse the height from the key, and skip deletion if outside of the range
			numberPart := k[len(key):]
			number, err := heightParser(string(numberPart))
			if err != nil {
				fmt.Printf("got err %s\n", err)
				continue
			}
			if number > endHeight {
				continue
			}
			pruned++
			prunedLastBatch++
			if err := db.Delete(k); err != nil {
				iter.Close()
				return pruned, fmt.Errorf("error deleting key %s: %w", string(k), err)
			}

		}

		if err := iter.Error(); err != nil {
			iter.Close()
			return pruned, fmt.Errorf("iterator error for digit length %d: %w", digits, err)
		}

		iter.Close()
		if prunedLastBatch == 0 {
			break
		}
		prunedLastBatch = 0
	}

	return pruned, nil
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
