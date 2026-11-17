package cmd

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store"
	"cosmossdk.io/store/metrics"
	storetypes "cosmossdk.io/store/types"
	db "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

func buildTestAppDB(t *testing.T, dataDir string) {
	t.Helper()

	appDB, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)

	cms := store.NewCommitMultiStore(appDB, logger, metrics.NewNoOpMetrics())
	key := storetypes.NewKVStoreKey("test")
	cms.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, cms.LoadLatestVersion())

	kv := cms.GetCommitKVStore(key)
	for i := 0; i < 3; i++ {
		kv.Set([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		cms.Commit()
	}

	require.NoError(t, appDB.Close())
}

func TestSnapshotAndRestoreAppClosesAllDBHandles(t *testing.T) {
	logger = log.NewLogger(os.Stderr, setConfig)
	dataDir := t.TempDir()

	buildTestAppDB(t, dataDir)

	// similar to Prune(), before PruneApp
	appStoreDB, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)
	snapshotDB, err := db.NewDB("snapshots/metadata", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)

	snapshotted, err := SnapshotAndRestoreApp(&ApplicationPrunerParams{
		appDB:                    appStoreDB,
		snapshotDB:               snapshotDB,
		dataDir:                  dataDir,
		dbfmt:                    db.GoLevelDBBackend,
		snapshotRestoreThreshold: 0,
	})
	require.NoError(t, err)
	require.True(t, snapshotted, "expected the snapshot-restore path to run")

	_ = appStoreDB.Close()
	_ = snapshotDB.Close()

	// at this point every LevelDB handle must be released
	reopenedApp, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err, "application.db is still locked: a DB handle leaked from SnapshotAndRestoreApp")
	require.NoError(t, reopenedApp.Close())
}

// If maybeRunGC() doesn't run gcDB(), it should not leak the db handle.
func TestMaybeRunGCClosesDBWhenSkipping(t *testing.T) {
	logger = log.NewLogger(os.Stderr, setConfig)
	dataDir := t.TempDir()

	buildTestAppDB(t, dataDir)

	appStoreDB, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)

	require.NoError(t, maybeRunGC(gcRunOptions{
		label:       "application",
		dbName:      "application",
		sizePath:    dataDir + "/application.db",
		dataDir:     dataDir,
		dbfmt:       db.GoLevelDBBackend,
		db:          appStoreDB,
		snapshotted: true, // to skip db compaction
	}))

	reopened, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err, "application.db is still locked: maybeRunGC leaked the handle on the skip path")
	require.NoError(t, reopened.Close())
}

// 5001 leaves = 10001 nodes: one async import batch plus a 1-node final batch, the shape
// where IAVL < v1.2.5 rebuilds the fast-node index before that batch is written.
func TestSnapshotAndRestoreAppRebuildsFastNodes(t *testing.T) {
	logger = log.NewNopLogger()
	dataDir := t.TempDir()
	const leaves = 5001

	appDB, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)
	cms := store.NewCommitMultiStore(appDB, logger, metrics.NewNoOpMetrics())
	key := storetypes.NewKVStoreKey("test")
	cms.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, cms.LoadLatestVersion())
	kv := cms.GetCommitKVStore(key)
	for i := 0; i < leaves; i++ {
		k := make([]byte, 34)
		binary.BigEndian.PutUint64(k[26:], uint64(i)*2654435761)
		kv.Set(k, []byte{1})
	}
	cms.Commit()

	snapshotDB, err := db.NewDB("snapshots/metadata", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)
	_, err = SnapshotAndRestoreApp(&ApplicationPrunerParams{
		appDB:                    appDB,
		snapshotDB:               snapshotDB,
		dataDir:                  dataDir,
		dbfmt:                    db.GoLevelDBBackend,
		snapshotRestoreThreshold: 0,
	})
	require.NoError(t, err)
	require.NoError(t, snapshotDB.Close())

	restored, err := db.NewDB("application", db.GoLevelDBBackend, dataDir)
	require.NoError(t, err)
	defer restored.Close()
	it, err := db.IteratePrefix(db.NewPrefixDB(restored, []byte("s/k:test/")), []byte("f"))
	require.NoError(t, err)
	defer it.Close()
	fastNodes := 0
	for ; it.Valid(); it.Next() {
		fastNodes++
	}
	require.Equal(t, leaves, fastNodes, "restored store is missing fast nodes; latest-height reads would return nil")
}
