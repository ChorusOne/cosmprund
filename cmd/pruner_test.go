package cmd

import (
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
