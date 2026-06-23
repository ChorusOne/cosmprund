//go:build celestia

package cmd

// This file is compiled ONLY into the celestia-dedicated binary, built with:
//
//	go build -modfile=go.celestia.mod -tags 'pebbledb celestia' ...   (make build-celestia)
//
// That build pins the celestia hashing stack (iavl v1.2.8 + celestiaorg store/log
// forks), so SnapshotAndRestoreApp's IAVL export/import reproduces byte-identical inner
// node hashes and the restored app hash matches what celestia-appd committed — same
// principle as state sync. Reclaims the most space (rebuilds application.db to latest
// state only).
//
// The DEFAULT binary keeps celestia/mocha on PruneAppState (see chains.go): its upstream
// iavl v1.2.0 would rehash the tree differently and break the app hash. NEVER run this
// celestia binary against non-celestia chains — their hashing would then mismatch.
//
// The post-restore verify guard (verifyAppState in pruner.go, --verify-after-prune) still
// runs and aborts if the restored hash does not match, so a wrong stack fails safe.
func init() {
	celestiaSnapshotConfig := ChainPruner{
		PruneBlockState:          pruneBlockAndStateStore,
		PruneApp:                 SnapshotAndRestoreApp,
		SnapshotRestoreThreshold: 1 * GiB,
	}
	chainConfigs["celestia"] = celestiaSnapshotConfig
	chainConfigs["mocha-4"] = celestiaSnapshotConfig
}
