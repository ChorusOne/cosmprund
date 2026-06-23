package cmd

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtstore "github.com/cometbft/cometbft/api/cometbft/store/v1"

	"github.com/binaryholdings/cosmos-pruner/cmd/celestia"
	"github.com/cosmos/gogoproto/proto"

	db "github.com/cosmos/cosmos-db"
	"golang.org/x/sync/errgroup"
)

type PrefixAndSplitter struct {
	prefix   string
	splitter heightParser
}

func (p PrefixAndSplitter) String() string {
	return p.prefix
}

// - Block headers (keys H:<HEIGHT>)
// - Commit information (keys C:<HEIGHT>)
// - ExtendedCommit information (keys EC:<HEIGHT>)
// - SeenCommit (keys SC:<HEIGHT>)
// - BlockPartKey (keys P:<HEIGHT>:<PART INDEX>)
// See https://github.com/cometbft/cometbft/blob/4591ef97ce5de702db7d6a3bbcb960ecf635fd76/store/db_key_layout.go#L38
// for confirmation of this
// TODO: see if we can import that as consts?
var blockKeyInfos = []PrefixAndSplitter{
	{"H:", asciiHeightParser},         // block headers
	{"C:", asciiHeightParser},         // commit info
	{"EC:", asciiHeightParser},        // extended commits
	{"SC:", asciiHeightParser},        // seen commits
	{"P:", asciiHeightParserTwoParts}, // block parts
}

var stateKeyInfos = []string{
	"abciResponsesKey:",
	"consensusParamsKey:",
}

func pruneBlockStore(blockStoreDB db.DB, pruneHeight uint64) error {
	if err := pruneKeys(
		blockStoreDB,
		"block",
		blockKeyInfos,
		func(store db.DB, ki PrefixAndSplitter) (uint64, error) {
			return deleteHeightRange(store, ki.prefix, 0, pruneHeight, ki.splitter)
		},
	); err != nil {
		return err
	}
	count, err := deleteAllByPrefix(blockStoreDB, []byte("BH:"), func(key, value []byte) (bool, error) {
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("prune block BH: %w", err)
	}
	logger.Info("Pruned", "store", "block", "key", "BH:", "count", count)

	// Defined
	// https://github.com/celestiaorg/celestia-core/blob/88dae3c1d03e6225c23076c1d2a3cce7ef58f79d/store/store.go#L656
	// Deleted
	// https://github.com/celestiaorg/celestia-core/blob/88dae3c1d03e6225c23076c1d2a3cce7ef58f79d/store/store.go#L386
	// with the transaction's height and index if committed, or its pending, evicted, or unknown status.
	count, err = deleteAllByPrefix(blockStoreDB, []byte("TH:"), func(key, val []byte) (bool, error) {
		var txi celestia.CelestiaTxInfo
		if err = proto.Unmarshal(val, &txi); err != nil {
			return false, err
		}
		return txi.Height < int64(pruneHeight), nil
	})
	if err != nil {
		return fmt.Errorf("prune block TH: %w", err)
	}
	logger.Info("Pruned", "store", "block", "key", "TH:", "count", count)
	return nil
}

// TODO: find out if we can go harder here
func pruneStateStore(stateStoreDB db.DB, pruneHeight uint64) error {
	const validatorHistoryToKeep = 1_000
	if err := pruneValidatorHistory(stateStoreDB, validatorHistoryToKeep); err != nil {
		logger.Error("validator history pruning failed", "err", err)
	}

	return pruneKeys(
		stateStoreDB, "state", stateKeyInfos,
		func(store db.DB, key string) (uint64, error) {
			return deleteHeightRange(store, key, 0, pruneHeight, asciiHeightParser)
		},
	)
}

func pruneValidatorHistory(stateStoreDB db.DB, validatorHistoryToKeep uint64) error {
	latestValHeight, err := findLatestValidatorHeight(stateStoreDB)
	if err != nil {
		return fmt.Errorf("could not determine latest validator height: %w", err)
	}

	if latestValHeight <= validatorHistoryToKeep {
		logger.Info("not enough history to perform validatorKey pruning")
		return nil
	}

	retainHeight := latestValHeight - validatorHistoryToKeep
	anchorHeight := findAnchorCheckpoint(stateStoreDB, retainHeight)
	if anchorHeight == nil {
		logger.Info("could not find safe anchor checkpoint: %w", err)
		return nil
	}

	validatorPruneCutoff := *anchorHeight - 1
	if validatorPruneCutoff == 0 {
		logger.Info("no old validator keys to prune")
		return nil
	}

	logger.Info("pruning validatorsKey", "cutoffHeight", validatorPruneCutoff)
	count, err := deleteHeightRange(stateStoreDB, "validatorsKey:", 0, validatorPruneCutoff, asciiHeightParser)
	if err != nil {
		return fmt.Errorf("failed to prune validatorsKey: %w", err)
	}

	logger.Info("pruned validatorsKey", "count", count)
	return nil
}

func findLatestValidatorHeight(db db.DB) (uint64, error) {
	prefix := []byte("validatorsKey:")
	endPrefix := make([]byte, len(prefix))
	copy(endPrefix, prefix)
	endPrefix[len(endPrefix)-1]++

	iter, err := db.ReverseIterator(prefix, endPrefix)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = iter.Close()
	}()

	if !iter.Valid() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("no validator keys found")
	}

	key := iter.Key()
	keyStr := string(key)
	heightStr := strings.TrimPrefix(keyStr, "validatorsKey:")
	return strconv.ParseUint(heightStr, 10, 64)
}

// findAnchorCheckpoint searches backwards from retainHeight to find a height that has complete validator set data
// in validatorsKey there are two types of values: a validator set, and a value saying that the complete validator set for this height is unchanged, and points to some other key.
func findAnchorCheckpoint(db db.DB, retainHeight uint64) *uint64 {
	for h := retainHeight; h > 0; h-- {
		key := fmt.Appendf(nil, "validatorsKey:%d", h)
		value, err := db.Get(key)
		if err != nil {
			continue
		}

		valInfo := new(cmtstate.ValidatorsInfo)
		if err := proto.Unmarshal(value, valInfo); err != nil {
			continue
		}

		if valInfo.ValidatorSet != nil {
			logger.Info("found anchor checkpoint", "height", h)
			return &h
		}
	}
	return nil
}
func pruneBlockAndStateStore(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error {
	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error { return pruneBlockStore(blockStoreDB, pruneHeight) })
	g.Go(func() error { return pruneStateStore(stateStoreDB, pruneHeight) })

	if err := g.Wait(); err != nil {
		return err
	}

	// Set the blockstore base to the retain floor (pruneHeight+1) so cometbft's
	// LoadBaseMeta(base) finds a real block meta and /status reports earliest_block_*
	// instead of empty values. We intentionally use the retain floor rather than the
	// global-minimum surviving "H:" key: digit-boundary all-nines metas (e.g. H:9999999)
	// can survive deletion as orphans and would wrongly become the base. setBlockStoreBase
	// verifies the meta actually exists before writing.
	if err := setBlockStoreBase(blockStoreDB, pruneHeight+1); err != nil {
		// non-fatal: a wrong/absent base only affects /status earliest_*, not node operation
		logger.Error("failed to set blockstore base, leaving it to the node", "err", err)
	}

	logger.Info("Finished pruning block and state stores")
	return nil
}

var blockStoreKey = []byte("blockStore")

// setBlockStoreBase rewrites BlockStoreState.Base to the lowest KEPT height whose
// block meta ("H:<height>") actually exists, scanning upward from wantBase. Verifying
// the meta exists is what makes cometbft's LoadBaseMeta(base) succeed, so /status
// reports earliest_block_* instead of empty values. Height is left untouched.
func setBlockStoreBase(blockStoreDB db.DB, wantBase uint64) error {
	raw, err := blockStoreDB.Get(blockStoreKey)
	if err != nil {
		return fmt.Errorf("load blockStore key: %w", err)
	}
	if len(raw) == 0 {
		logger.Info("empty blockStore key, skipping base set")
		return nil
	}

	var bss cmtstore.BlockStoreState
	if err := proto.Unmarshal(raw, &bss); err != nil {
		return fmt.Errorf("unmarshal BlockStoreState: %w", err)
	}

	// first height >= wantBase whose meta exists (normally wantBase itself)
	base := wantBase
	for ; int64(base) <= bss.Height; base++ {
		ok, err := blockStoreDB.Has(fmt.Appendf(nil, "H:%d", base))
		if err != nil {
			return fmt.Errorf("probe H:%d: %w", base, err)
		}
		if ok {
			break
		}
	}
	if int64(base) > bss.Height {
		logger.Warn("no surviving block meta at/above retain floor, leaving base unchanged",
			"wantBase", wantBase, "height", bss.Height)
		return nil
	}

	bss.Base = int64(base)
	out, err := proto.Marshal(&bss)
	if err != nil {
		return fmt.Errorf("marshal BlockStoreState: %w", err)
	}
	if err := blockStoreDB.SetSync(blockStoreKey, out); err != nil { // fsync so the write survives
		return fmt.Errorf("write blockStore key: %w", err)
	}
	logger.Info("Set blockstore base", "base", base, "height", bss.Height)
	return nil
}

func pruneKeys[T any](
	store db.DB,
	storeName string,
	keyInfo []T,
	deleteFn func(db.DB, T) (uint64, error),
) error {
	for _, k := range keyInfo {
		count, err := deleteFn(store, k)
		if err != nil {
			return fmt.Errorf("prune %s key %v: %w", storeName, k, err)
		}
		logger.Info("Pruned", "store", storeName, "key", k, "count", count)
	}
	return nil
}

func deleteAllByPrefix(db db.DB, key []byte, cb func([]byte, []byte) (bool, error)) (uint64, error) {
	rangeEnd := make([]byte, len(key))
	copy(rangeEnd, key)
	rangeEnd[len(rangeEnd)-1]++
	iter, err := db.Iterator(key, rangeEnd)
	defer func() {
		_ = iter.Close()
	}()

	if err != nil {
		return 0, err
	}
	batch := db.NewBatch()
	count := uint64(0)

	for ; iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		del, err := cb(k, v)
		if err != nil {
			return count, err
		}
		if del {
			err = batch.Delete(k)
			count++
			if err != nil {
				return count, err
			}
		}
		if count > 0 && count%1_000_000 == 0 {
			if err = batch.Write(); err != nil {
				return count, err
			}
			if err = batch.Close(); err != nil {
				return count, err
			}
			batch = db.NewBatch()
			logger.Info("Deleted many keys, new batch", "count", count, "prefix", string(key))
		}
	}
	if err = batch.Write(); err != nil {
		return count, err
	}
	if err = batch.Close(); err != nil {
		return count, err
	}

	return count, nil
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

		startKey := fmt.Appendf(nil, "%s%0*d", key, digits, rangeStart)
		endKey := fmt.Appendf(nil, "%s%0*d", key, digits, rangeEnd)

		iter, err := db.Iterator(startKey, endKey)
		if err != nil {
			return pruned, fmt.Errorf("error creating iterator for digit length %d: %w", digits, err)
		}
		logger.Debug("Pruning range", "Start", string(startKey), "end", string(endKey))

		batch := db.NewBatch()

		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			// The keys are of format <key><height>
			// but <height> is an ascii-encoded integer; so when we query by range
			// we _will_ get keys which are beyond the expected maximum.
			// Parse the height from the key, and skip deletion if outside of the range
			numberPart := k[len(key):]
			number, err := heightParser(string(numberPart))
			if err != nil {
				logger.Error("Failed to parse height", "key", string(k), "err", err)
				continue
			}
			if number > endHeight {
				continue
			}
			pruned++
			prunedLastBatch++
			if err := batch.Delete(k); err != nil {
				_ = iter.Close()
				_ = batch.Close()
				return pruned, fmt.Errorf("error deleting key %s: %w", string(k), err)
			}

		}

		if err = batch.Write(); err != nil {
			return pruned, err
		}
		if err = batch.Close(); err != nil {
			return pruned, err
		}

		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return pruned, fmt.Errorf("iterator error for digit length %d: %w", digits, err)
		}

		_ = iter.Close()
		if prunedLastBatch == 0 {
			break
		}
		prunedLastBatch = 0
	}

	return pruned, nil
}
