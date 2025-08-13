This project prunes data from 'old' blocks from a CometBFT/Tendermint/Cosmos-sdk node.

Only the `block`, `state` and `application` databases are pruned.

Only `goleveldb` and `pebbledb` backends are supported.


## How to use

Cosmprund works of a data directory that has the same structure of a normal cosmos-sdk/cometbft node. By default it will prune all but 10 blocks from cometbft, and all but 10 versions of application state.

```
# clone & build cosmprund repo
git clone https://github.com/ChorusOne/cosmprund/
cd cosmprund
make build

# stop your daemon/cosmovisor
sudo systemctl stop cosmovisor

# run cosmprund
./build/cosmprund prune ~/.gaiad/data
```

Flags:

```
Flags:
      --cometbft             set to false you dont want to prune cometbft data (default true)
      --cosmos-sdk           set to false if using only with cometbft (default true)
      --force-compress-app   compress application.db even if it's larger than reasonable (10 GB)
                             The entire database needs to be read, so it will be slow
  -h, --help                 help for prune
  -b, --keep-blocks uint     set the amount of blocks to keep (default 100)
  -v, --keep-versions uint   set the amount of versions to keep in the application store (default 10)
      --run-gc               set to false to prevent a GC pass (default true)
```

## Shortcomings

Not all stores are pruned, though usually `block`, `state` and `application` are the bulk of the data.

Within the `application` store, this tool currently only purges old states (`s/` keys). Some chains store extra data here, and each chain required custom logic to prune it.

If you have a chain with a large `application` dir, you can try running statesync every once in a while to reduce it, alternatively, make an issue in this repo and we'll see if it can be pruned externally.

If no historical data is stored in `application.db`, it's usually pruned to a few (~50) MB. If there's significant historical data, then `application.db` will be large (20+GB); these cases are not supported yet,
so using a large value for `--compress-app-under-gb` is not likely to achieve much.

## Metadata

You can also use `cosmprund db-info your-data-dir` and you will get a JSON back in this form:

```
{"chain_id":"allora-testnet-1","initial_height":1,"last_block_height":3039285,"app_hash":"1CA3A44FC14A6D08137245F5FCB32275DD1150FEE76E3AD7F31FC5B388474854","last_block_time":"2025-03-17T17:06:31.620566697Z"}
```

which you can use to get a correct `last_block_height` for your snapshots

## On speed

If your data directory is large, it will take a while to prune.

On our tests, a Berachain node with 150GB of data is pruned to ~50MB in ~10 minutes.


## On size

Depending on the chain, results will vary; for many chains we see a (compressed) snapshot in the 10~300MB range.

## Custom pruning for application DB

In `cmd/chains.go` you'll find mappings of chain_id <-> pruning function. Adding new, custom implementations requires defining a set of chain ID's and a struct which contains two functions: BlockStatePruner (should be similar for chains on the same CometBFT version) and AppPruner (different for each chain).
