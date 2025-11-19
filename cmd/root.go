package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"cosmossdk.io/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	Version string
	Commit  string
)

var (
	cosmosSdk           bool
	iavlDisableFastNode bool
	cometbft            bool
	keepBlocks          uint64
	runGC               bool
	forceCompress       bool
	keepVersions        uint64
)

func NewRootCmd() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:     "cosmprund",
		Short:   "cosmprund cleans up databases of Cosmos SDK applications, removing historical data generally not needed for validator nodes",
		Version: Version,
	}

	pruneCmd := &cobra.Command{
		Use:   "prune <data_dir>",
		Short: "Prune database stores",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := args[0]
			logger = log.NewLogger(os.Stderr, setConfig)

			uid, gid, err := Stat(path.Join(dataDir, "state.db"))
			if err != nil {
				logger.Error("can't stat state.db, bailing", "err", err)
				return err
			}

			defer func() {
				err = ChownR(dataDir, uid, gid)
				if err != nil {
					logger.Error("Failed to run chown, continuing", "err", err)
				}
			}()

			err = Prune(dataDir, cometbft, cosmosSdk, iavlDisableFastNode)
			if err != nil {
				return err
			}

			return nil
		},
	}

	rootCmd.AddCommand(pruneCmd)

	// --force-compress flag
	pruneCmd.PersistentFlags().BoolVar(&forceCompress, "force-compress-app", false, fmt.Sprintf("compress databases even if they are larger than reasonable (%f GB)\nThe entire database needs to be read, so it will be slow", gcSizeThreshold/GiB))
	if err := viper.BindPFlag("force-compress-app", pruneCmd.PersistentFlags().Lookup("force-compress-app")); err != nil {
		panic(err)
	}
	// --run-gc flag
	pruneCmd.PersistentFlags().BoolVar(&runGC, "run-gc", true, "set to false to prevent a GC pass")
	if err := viper.BindPFlag("run-gc", pruneCmd.PersistentFlags().Lookup("run-gc")); err != nil {
		panic(err)
	}
	// --keep-blocks flag
	pruneCmd.PersistentFlags().Uint64VarP(&keepBlocks, "keep-blocks", "b", 100, "set the amount of blocks to keep")
	if err := viper.BindPFlag("keep-blocks", pruneCmd.PersistentFlags().Lookup("keep-blocks")); err != nil {
		panic(err)
	}

	// --keep-versions flag
	pruneCmd.PersistentFlags().Uint64VarP(&keepVersions, "keep-versions", "v", 10, "set the amount of versions to keep in the application store")
	if err := viper.BindPFlag("keep-versions", pruneCmd.PersistentFlags().Lookup("keep-versions")); err != nil {
		panic(err)
	}

	// --cosmos-sdk flag
	pruneCmd.PersistentFlags().BoolVar(&cosmosSdk, "cosmos-sdk", true, "set to false if using only with cometbft")
	if err := viper.BindPFlag("cosmos-sdk", pruneCmd.PersistentFlags().Lookup("cosmos-sdk")); err != nil {
		panic(err)
	}

	// --cometbft flag
	pruneCmd.PersistentFlags().BoolVar(&cometbft, "cometbft", true, "set to false you dont want to prune cometbft data")
	if err := viper.BindPFlag("cometbft", pruneCmd.PersistentFlags().Lookup("cometbft")); err != nil {
		panic(err)
	}

	// --iavl-disable-fastnode flag
	pruneCmd.PersistentFlags().BoolVar(&iavlDisableFastNode, "iavl-disable-fastnode", false, "set accordingly with app.toml iavl-disable-fastnode setting")
	if err := viper.BindPFlag("iavl-disable-fastnode", pruneCmd.PersistentFlags().Lookup("iavl-disable-fastnode")); err != nil {
		panic(err)
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Output extended version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("cosmprund version " + Version + " (commit " + Commit + ")")
		},
	}
	rootCmd.AddCommand(versionCmd)

	dbInfoCmd := &cobra.Command{
		Use:   "db-info <data_dir>",
		Short: "Show tendermint state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := args[0]

			dbState, err := DbState(dataDir)
			if err != nil {
				fmt.Printf("failed %v\n", err)
				return err
			}
			marshalled, err := json.Marshal(dbState)
			if err != nil {
				fmt.Printf("failed %v\n", err)
				return err
			}
			fmt.Printf("%s", marshalled)
			return nil
		},
	}
	rootCmd.AddCommand(dbInfoCmd)

	return rootCmd
}

func Execute() {
	cobra.EnableCommandSorting = false

	rootCmd := NewRootCmd()
	rootCmd.SilenceUsage = true
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
