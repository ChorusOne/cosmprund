package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"github.com/binaryholdings/cosmos-pruner/cmd"
	db "github.com/cosmos/cosmos-db"
)

type ModuleStats struct {
	keyCount            int
	totalSize           int64
	samples             [][]byte
	heightDistribution  map[uint64]int
	keyPatterns         map[string]int
	versionDistribution map[int64]int
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./analyzer <db_name> <data_dir>")
		return
	}
	dbName := os.Args[1]
	dataDir := os.Args[2]

	fmt.Printf("Analyzing database: %s in directory: %s\n", dbName, dataDir)
	dbfmt, err := cmd.GetFormat(filepath.Join(dataDir, fmt.Sprintf("%s.db", dbName)))
	if err != nil {
		panic(err)
	}
	dbf, err := db.NewDB(dbName, dbfmt, dataDir)
	if err != nil {
		panic(err)
	}
	defer dbf.Close()

	fmt.Println("\n=== RAW DATABASE ANALYSIS ===")
	rawStats := analyzeRawDatabase(dbName, dbf)

	fmt.Println("\n=== IAVL STORE ANALYSIS ===")
	analyzeIAVLStructure(dbf)

	fmt.Println("\n=== ORPHANED DATA ANALYSIS ===")
	identifyOrphanedData(dbf, rawStats)
}

func analyzeRawDatabase(name string, db db.DB) map[string]*ModuleStats {
	moduleStats := make(map[string]*ModuleStats)

	var totalDBSize int64
	var totalKeys int

	iter, err := db.Iterator(nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to create iterator: %w", err))
	}
	defer iter.Close()

	fmt.Println("Starting raw database scan...")
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if totalKeys > 0 && totalKeys%100000 == 0 {
			fmt.Printf("\rAnalyzed %d keys (%.2f GB)...", totalKeys, float64(totalDBSize)/1024/1024/1024)
			_ = os.Stdout.Sync()
		}

		prefix := categorizeKey(key)

		stats, ok := moduleStats[prefix]
		if !ok {
			stats = &ModuleStats{
				heightDistribution:  make(map[uint64]int),
				keyPatterns:         make(map[string]int),
				versionDistribution: make(map[int64]int),
			}
			moduleStats[prefix] = stats
		}

		stats.keyCount++
		valueSize := int64(len(value))
		stats.totalSize += valueSize

		if height := extractHeight(key); height > 0 && height < 200_000_000 {
			stats.heightDistribution[height]++
		}

		if version := extractIAVLVersion(key); version > 0 {
			stats.versionDistribution[version]++
		}

		pattern := getKeyPattern(key)
		stats.keyPatterns[pattern]++

		if len(stats.samples) < 5 {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			stats.samples = append(stats.samples, keyCopy)
		}

		totalDBSize += valueSize
		totalKeys++
	}

	fmt.Printf("\nRaw database scan complete.\n\n")

	fmt.Printf("Total DB Size: %.2f GB\n", float64(totalDBSize)/1024/1024/1024)
	fmt.Printf("Total Keys: %d\n", totalKeys)

	type report struct {
		prefix string
		stats  *ModuleStats
	}
	var reports []report
	for p, s := range moduleStats {
		reports = append(reports, report{prefix: p, stats: s})
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].stats.totalSize > reports[j].stats.totalSize
	})

	fmt.Printf("\n%-40s %-12s %-15s %-10s\n", "PREFIX", "KEY COUNT", "SIZE (MB)", "% OF DB")
	fmt.Printf("%s\n", strings.Repeat("-", 80))
	for _, r := range reports[:min(10, len(reports))] {
		fmt.Printf("%-40s %-12d %-15.2f %-10.2f\n",
			r.prefix,
			r.stats.keyCount,
			float64(r.stats.totalSize)/1024/1024,
			float64(r.stats.totalSize)/float64(totalDBSize)*100,
		)
	}

	return moduleStats
}

func analyzeIAVLStructure(appDB db.DB) {
	appStore := rootmulti.NewStore(appDB, nil, metrics.NewNoOpMetrics())
	latestVersion := rootmulti.GetLatestVersion(appDB)
	fmt.Printf("Latest version: %d\n", latestVersion)

	if latestVersion == 0 {
		fmt.Println("No versions found in store")
		return
	}

	cInfo, err := appStore.GetCommitInfo(latestVersion)
	if err != nil {
		fmt.Printf("Failed to get commit info for latest version: %v\n", err)
		fmt.Println("Cannot perform detailed IAVL analysis.")
		return
	}

	fmt.Printf("Number of stores at latest version: %d\n", len(cInfo.StoreInfos))
	for _, storeInfo := range cInfo.StoreInfos {
		if len(storeInfo.CommitId.Hash) > 0 {
			fmt.Printf("  Store: %s, Version: %d\n", storeInfo.Name, storeInfo.CommitId.Version)
		}
	}

	fmt.Println("\nChecking for historical versions (by commit info):")
	versionCheckPoints := []int64{
		1,
		latestVersion / 4,
		latestVersion / 2,
		latestVersion - 100000,
		latestVersion - 10000,
		latestVersion - 1000,
		latestVersion - 100,
		latestVersion,
	}

	var existingVersions []int64
	for _, version := range versionCheckPoints {
		if version <= 0 || version > latestVersion {
			continue
		}
		if _, err := appStore.GetCommitInfo(version); err == nil {
			existingVersions = append(existingVersions, version)
		}
	}
	if len(existingVersions) > 0 {
		fmt.Printf("Found commit info for sample versions: %v\n", existingVersions)
		fmt.Println("This indicates that historical version metadata exists.")
	} else {
		fmt.Println("Could not find commit info for any sample historical versions.")
	}
}

func identifyOrphanedData(appDB db.DB, rawStats map[string]*ModuleStats) {
	fmt.Println("\nThis section identifies data that appears to be part of old, un-pruned IAVL versions.")
	latestVersion := rootmulti.GetLatestVersion(appDB)
	oldVersionThreshold := latestVersion - 10000

	for prefix, stats := range rawStats {
		if strings.HasPrefix(prefix, "n/") || strings.HasPrefix(prefix, "o/") || strings.HasPrefix(prefix, "r/") || strings.HasPrefix(prefix, "s/k:") {
			oldKeysCount := 0
			var minOldVersion, maxOldVersion int64 = -1, -1

			for version, count := range stats.versionDistribution {
				if version < oldVersionThreshold {
					oldKeysCount += count
					if minOldVersion == -1 || version < minOldVersion {
						minOldVersion = version
					}
					if maxOldVersion == -1 || version > maxOldVersion {
						maxOldVersion = version
					}
				}
			}

			if oldKeysCount > 1000 {
				fmt.Printf("\nPrefix '%s' contains a large number of keys from old versions.\n", prefix)
				fmt.Printf("  >> Total keys in prefix: %d (%.2f MB)\n", stats.keyCount, float64(stats.totalSize)/1024/1024)
				fmt.Printf("  >> Keys from old versions (< %d): %d\n", oldVersionThreshold, oldKeysCount)
				if minOldVersion != -1 {
					fmt.Printf("  >> Old version range detected: %d - %d\n", minOldVersion, maxOldVersion)
				}
				fmt.Printf("  >> This suggests that IAVL pruning has not been effective for this data.\n")
			}
		}
	}
}

func categorizeKey(key []byte) string {
	keyStr := string(key)
	if strings.HasPrefix(keyStr, "s/k:") {
		parts := strings.SplitN(keyStr, "/", 3)
		if len(parts) > 1 {
			modulePart := strings.Split(parts[1], ":")
			if len(modulePart) > 1 {
				moduleNameParts := strings.Split(modulePart[1], "/")
				return "s/k:" + moduleNameParts[0]
			}
		}
		return "s/k:"
	} else if strings.HasPrefix(keyStr, "s/") && !strings.Contains(keyStr, ":") {
		return "s/<version>"
	} else if strings.HasPrefix(keyStr, "n/") {
		return "n/<iavl-node>"
	} else if strings.HasPrefix(keyStr, "o/") {
		return "o/<iavl-orphan>"
	} else if strings.HasPrefix(keyStr, "r/") {
		return "r/<iavl-root>"
	} else if strings.HasPrefix(keyStr, "m/") {
		return "m/<metadata>"
	} else if strings.HasPrefix(keyStr, "commitInfo/") {
		return "commitInfo"
	}

	if idx := strings.Index(keyStr, "/"); idx > 0 {
		return keyStr[:idx]
	}
	if idx := strings.Index(keyStr, ":"); idx > 0 {
		return keyStr[:idx]
	}
	return "(uncategorized)"
}

func extractHeight(key []byte) uint64 {
	for i := 0; i <= len(key)-8; i++ {
		potentialHeight := binary.BigEndian.Uint64(key[i : i+8])
		if potentialHeight > 1 && potentialHeight < 200_000_000 {
			return potentialHeight
		}
	}
	return 0
}

func extractIAVLVersion(key []byte) int64 {
	keyStr := string(key)
	if strings.HasPrefix(keyStr, "s/") && !strings.Contains(keyStr, ":") {
		var version int64
		if _, err := fmt.Sscanf(keyStr[2:], "%d", &version); err == nil {
			return version
		}
	}
	if (strings.HasPrefix(keyStr, "n/") || strings.HasPrefix(keyStr, "o/")) && len(key) > 2 {
		version, n := binary.Uvarint(key[2:])
		if n > 0 {
			return int64(version)
		}
	}
	return 0
}

func getKeyPattern(key []byte) string {
	s := string(key)
	runes := []rune(s)
	for i, r := range runes {
		if r < 32 || r > 126 {
			runes[i] = '?'
		}
	}
	simplified := string(runes)
	if len(simplified) > 60 {
		return simplified[:60] + "..."
	}
	return simplified
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
