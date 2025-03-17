package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	Version string
	Commit  string
)

var (
	cosmosSdk     bool
	cometbft      bool
	keepBlocks    uint64
	gcApplication bool
	keepVersions  uint64
	appName       = "cosmprund"
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
			gcApplication = viper.GetBool("gc-application")

			if cosmosSdk {
				if err := PruneAppState(dataDir); err != nil {
					return err
				}
			}

			if cometbft {
				if err := PruneCmtData(dataDir); err != nil {
					return err
				}
			}

			return nil
		},
	}

	rootCmd.AddCommand(pruneCmd)

	// --gc-application flag
	pruneCmd.PersistentFlags().Bool("gc-application", false, "whether to run GC on the application DB")
	if err := viper.BindPFlag("gc-application", pruneCmd.PersistentFlags().Lookup("gc-application")); err != nil {
		panic(err)
	}
	// --keep-blocks flag
	pruneCmd.PersistentFlags().Uint64VarP(&keepBlocks, "keep-blocks", "b", 10, "set the amount of blocks to keep")
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

			dbState, err := ShowDbState(dataDir)
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
