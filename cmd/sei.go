package cmd

import (
	"encoding/hex"
	"fmt"

	db "github.com/cosmos/cosmos-db"
	"github.com/google/orderedcode"
)

// state store keys
const (
	// prefixValidators             = int64(5) // TODO: anchoring
	prefixConsensusParams        = int64(6)
	prefixState                  = int64(8)
	prefixFinalizeBlockResponses = int64(14)
)

// block store keys
const (
	prefixBlockMeta   = int64(0)
	prefixBlockPart   = int64(1)
	prefixBlockCommit = int64(2)
	prefixSeenCommit  = int64(3)
	prefixBlockHash   = int64(4)
	prefixExtCommit   = int64(13)
)

func pruneSeiBlockAndStateStore(blockStoreDB, stateStoreDB db.DB, pruneHeight uint64) error {
	for _, key := range []int64{prefixBlockMeta, prefixBlockPart, prefixBlockCommit} {
		prunedEC, err := deleteSeiRange(blockStoreDB, key, 0, int64(pruneHeight))
		if err != nil {
			return err
		}
		logger.Info("Pruned", "key", key, "count", prunedEC, "store", "block")
	}

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
	batch := db.NewBatch()
	defer batch.Close()
	for ; iter.Valid(); iter.Next() {
		k := iter.Key()

		var number int64
		remain, err := orderedcode.Parse(string(k[1:]), &number)
		if err != nil {
			logger.Error("failed to parse sei key", "err", err)
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
		if err := batch.Delete(k); err != nil {
			iter.Close()
			return pruned, fmt.Errorf("error deleting key %s: %w", string(k), err)
		}

	}

	iter.Close()
	batch.Write()
	return pruned, nil
}
