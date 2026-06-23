# cosmprund

`cosmprund` prunes data from "old" blocks of a CometBFT / Tendermint / Cosmos-SDK node,
shrinking a node's data directory down to roughly the recent state it needs to keep
running as a validator / non-archive node.

It prunes three databases:

- `blockstore.db` — old block headers, commits, parts
- `state.db` — old ABCI responses, consensus params, validator history
- `application.db` — old IAVL versions of the application state

Supported DB backends: **`goleveldb`** and **`pebbledb`** (auto-detected from the on-disk
magic bytes — no flag needed).

---

## Requirements

- **Go 1.23+** (module pins `go 1.23.5`).
- No CGO. `pebbledb` is pure Go (`cockroachdb/pebble`), so an Alpine / static build works.
- For the **celestia-dedicated** build only: network access to fetch the `celestiaorg`
  fork modules the first time (see [Celestia-dedicated binary](#celestia-dedicated-binary)).

---

## Build & Install

### Standard binary (all chains)

```bash
# clone
git clone https://github.com/anam-145/cosmos-pruner
cd cosmos-pruner

# build → ./build/cosmprund
make build

# or install to $GOBIN
make install
```

Both targets compile with `-tags pebbledb` so the pebble backend is linked in. Building
without that tag drops pebble support and pebble chains will fail with
`unknown db backend`.

Plain `go build` works too, but remember the tag:

```bash
go build -tags pebbledb -o build/cosmprund main.go
```

### Docker

```bash
docker build -t cosmprund .
docker run --rm -v ~/.gaiad/data:/data cosmprund prune /data
```

The image builds with `-tags pebbledb` and ships a static binary on Alpine.

### Celestia-dedicated binary

Celestia (`celestia` / `mocha-4`) can optionally use the aggressive snapshot-restore
strategy (rebuild `application.db` to the latest state only). For the rebuilt app hash to
match what `celestia-appd` committed, the IAVL export/import must run with **celestia's own
hashing stack** (`iavl v1.2.8` + the `celestiaorg` store/log forks). That stack is pinned
in a separate module file, `go.celestia.mod`, and selected with the `celestia` build tag:

```bash
# build → ./build/cosmprund-celestia
make build-celestia

# or install
make install-celestia
```

Equivalent raw command:

```bash
go build -modfile=go.celestia.mod -tags 'pebbledb celestia' -o build/cosmprund-celestia main.go
```

> ⛔ **Only use `cosmprund-celestia` for `celestia` / `mocha-4`.** It links `iavl v1.2.8`;
> running it against other chains can produce a different app hash and corrupt them. Use
> the standard `cosmprund` for everything else.

See [Celestia](#celestia) for the full rationale and when you actually need this build.

---

## Usage

`cosmprund` works on a data directory with the normal Cosmos-SDK / CometBFT layout
(`blockstore.db`, `state.db`, `application.db`, …). By default it keeps the most recent
100 blocks and 10 application-state versions.

```bash
# stop the node first — the DBs must not be open
sudo systemctl stop cosmovisor      # or your node service

# prune
./build/cosmprund prune ~/.gaiad/data
```

The chain is detected from `state.db` (`chain_id`), which selects the pruning strategy
(see [Per-chain configuration](#per-chain-configuration)).

---

## Flags

```
      --cometbft               prune CometBFT data (blockstore + state) (default true)
      --cosmos-sdk             prune the application store (default true)
                               set false if running with CometBFT only
  -b, --keep-blocks uint       number of recent blocks to keep (default 100)
  -v, --keep-versions uint     number of recent application-state versions to keep (default 10)
      --run-gc                 run a GC/compaction pass after pruning (default true)
      --force-compress-app     compact DBs even when larger than 10 GiB (slow: reads the
                               whole DB) (default false)
      --iavl-disable-fastnode  match your app.toml `iavl-disable-fastnode` setting (default false)
      --verify-after-prune     after pruning the application store, reload the kept version
                               and verify every store's root hash still matches the
                               pre-prune commit info; abort on mismatch (default true)
  -h, --help                   help for prune
```

### `--verify-after-prune` (safety)

On by default. After the application DB is pruned/restored, the tool reopens it, reloads
the kept version, and checks that each store's IAVL root hash still equals the value
recorded in the pre-prune commit info. If anything changed, it **aborts** instead of
leaving a node that would silently fail consensus.

For the snapshot-restore strategy this also makes the operation **non-destructive on
failure**: the restore is written to a temporary DB on the same filesystem, verified, and
only then atomically swapped into place. The cost is transient disk usage of roughly the
original DB plus the restored DB at the same time. Set `--verify-after-prune=false` to use
the older destroy-then-restore path (lower peak disk, no fallback if the restore is wrong).

---

## How pruning works

### Block & state stores

Old keys are range-deleted up to `last_block_height - keep-blocks`:

- **blockstore**: block headers (`H:`), commits (`C:`), extended/seen commits (`EC:`/`SC:`),
  block parts (`P:`), the block-hash index (`BH:`), and tx-height index (`TH:`).
- **state**: `abciResponsesKey:`, `consensusParamsKey:`, and validator history
  (`validatorsKey:`, keeping the most recent ~1000).

After deleting old blocks, the blockstore `base` is rewritten to the **retain floor**
(`last_block_height - keep-blocks + 1`) — the lowest height still kept. The tool verifies a
block meta (`H:<height>`) actually exists at that height before writing (scanning upward if
needed), so CometBFT's `LoadBaseMeta(base)` finds a real block and `/status` reports
`earliest_block_*` instead of empty values. The write is `fsync`'d. This is intentionally
the retain floor and **not** the global-minimum surviving `H:` key: digit-boundary
all-nines metas (e.g. `H:9999999`) can survive range deletion as orphans and would
otherwise become a bogus base. Failure to set the base is non-fatal — it only affects
`/status` reporting, not node operation.

> If a node's base was already corrupted by an earlier prune (so `/status` shows empty
> `earliest_*`), re-running `prune` fixes it. Note that pruning physically deletes blocks
> below the retain window, so `earliest_block_*` can at best point at the lowest kept block
> (`last_block_height - keep-blocks`); genesis-era earliest values are only possible on an
> archive node.

### Application store — two strategies

Selected per chain in `cmd/chains.go`:

1. **`PruneAppState` (in-place)** — deletes IAVL versions older than the retained window
   and lets the GC pass reclaim space. The latest commit tree is never rehashed, so the
   app hash on disk stays valid. This is the default for most chains and the safest option.

2. **`SnapshotAndRestoreApp` (rebuild)** — when `application.db` is larger than the chain's
   `SnapshotRestoreThreshold`, the tool snapshots the latest version, rebuilds
   `application.db` from that snapshot (dropping all historical versions / dangling data —
   effectively a local state-sync), reclaiming the most space. This only produces a correct
   app hash when the tool's IAVL/store versions match the chain's (see [Celestia](#celestia)).

### GC pass

After pruning, `--run-gc` (default on) compacts each DB by copying live keys into a fresh
DB and swapping it in, reclaiming space that delete-only pruning leaves behind. It is
skipped for a store that was rebuilt via snapshot restore (already compact) or for DBs over
10 GiB unless `--force-compress-app` is set.

---

## Per-chain configuration

`cmd/chains.go` maps `chain_id` → pruning config:

```go
type ChainPruner struct {
	PruneBlockState          BlockStatePruner // block + state pruning
	PruneApp                 AppPruner        // PruneAppState or SnapshotAndRestoreApp
	SnapshotRestoreThreshold float64          // min application.db size to trigger rebuild
}
```

Chains not listed fall back to `PruneAppState` (in-place). To add custom handling for a
chain, add an entry with the appropriate `PruneApp` function.

---

## Celestia

Celestia runs a forked stack (`iavl v1.2.8`, `celestiaorg/cosmos-sdk` store/log forks).
The IAVL **node-hash algorithm is identical** across the relevant `iavl 1.2.x` versions, so
the **standard binary prunes celestia correctly using `PruneAppState`** — this is the
default config for `celestia` / `mocha-4`, and it works with no special build.

The only reason to use the [celestia-dedicated binary](#celestia-dedicated-binary) is to
use the more aggressive `SnapshotAndRestoreApp` (rebuild to latest state, maximum space
reclaim) on celestia. Because that path re-exports/imports the IAVL tree, it must run with
celestia's exact stack to reproduce the committed app hash — hence the pinned
`go.celestia.mod` and the `celestia` build tag, which flips `celestia`/`mocha-4` to
`SnapshotAndRestoreApp`. The `--verify-after-prune` guard validates the result and aborts
(non-destructively) if the hash does not match.

If a node's `application.db` was already corrupted by a bad rebuild, the pruner cannot
repair it — restore from a backup or re-sync via state sync.

---

## Metadata

```bash
cosmprund db-info <data-dir>
```

returns JSON:

```json
{
  "chain_id": "allora-testnet-1",
  "initial_height": 1,
  "last_block_height": 3039285,
  "app_hash": "1CA3A44F...",
  "last_block_time": "2025-03-17T17:06:31.620566697Z",
  "earliest_block_height": 3039186,
  "earliest_block_hash": "476B64EF...",
  "earliest_app_hash": "8EB76F9D...",
  "earliest_block_time": "2025-03-17T17:05:18.000000000Z"
}
```

useful for getting a correct `last_block_height` for your snapshots.

- `initial_height` is the chain's **genesis** initial height (usually `1`) — it does not
  change with pruning.
- `earliest_block_*` is the **post-prune base block**, read from the blockstore (mirrors
  CometBFT `/status`). It is empty/zero when the base is unset or its meta was pruned.

---

## On speed

If your data directory is large, pruning takes a while. In our tests a Berachain node with
150 GB of data was pruned to ~50 MB in ~10 minutes.

## On size

Results vary by chain; for many chains a (compressed) snapshot lands in the ~10–300 MB
range. Chains kept on `PruneAppState` with a large historical `application.db` shrink less
than a full snapshot rebuild.

## Shortcomings

- Not every store is pruned, but `block`, `state`, and `application` are usually the bulk.
- Within `application.db`, `PruneAppState` purges old IAVL versions; chains that stash extra
  data may need custom logic.
- `SnapshotAndRestoreApp` requires transient free disk for the rebuilt DB (more so with
  `--verify-after-prune`, which keeps the original until the new DB is verified).
