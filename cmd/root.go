package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/FactomWyomingEntity/prosper-pool/accounting"
	"github.com/FactomWyomingEntity/prosper-pool/authentication"
	"github.com/FactomWyomingEntity/prosper-pool/config"
	"github.com/FactomWyomingEntity/prosper-pool/database"
	"github.com/FactomWyomingEntity/prosper-pool/engine"
	"github.com/FactomWyomingEntity/prosper-pool/exit"
	"github.com/FactomWyomingEntity/prosper-pool/loghelp"
	"github.com/FactomWyomingEntity/prosper-pool/pegnet"
	"github.com/FactomWyomingEntity/prosper-pool/polling"
	"github.com/FactomWyomingEntity/prosper-pool/profile"
	"github.com/FactomWyomingEntity/prosper-pool/stratum"
	"github.com/pegnet/pegnet/modules/opr"
	"github.com/qor/session/manager"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(testMiner)
	rootCmd.AddCommand(testSync)
	rootCmd.AddCommand(testAccountant)
	rootCmd.AddCommand(testAuth)
	rootCmd.AddCommand(testStratum)
	rootCmd.AddCommand(getConfig)
	rootCmd.AddCommand(datasources)
	rootCmd.AddCommand(datasourcesPull)

	rootCmd.PersistentFlags().Bool("profile", false, "Turn on profiling")
	rootCmd.PersistentFlags().String("config", "$HOME/.prosper/prosper-pool.toml", "Location to config")
	rootCmd.PersistentFlags().String("log", "info", "Change the logging level. Can choose from 'trace', 'debug', 'info', 'warn', 'error', or 'fatal'")
	rootCmd.PersistentFlags().String("phost", "192.168.32.2", "Postgres host url")
	rootCmd.PersistentFlags().Int("pport", 5432, "Postgres host port")
	rootCmd.PersistentFlags().String("fhost", "http://localhost:8088/v2", "Factomd host url")
	rootCmd.PersistentFlags().Bool("testing", false, "Enable testing mode")
	rootCmd.PersistentFlags().Int("testingact", 0, "Set activation height for latest activation testing")
	rootCmd.PersistentFlags().MarkHidden("testingact")
	rootCmd.PersistentFlags().Int("act", 0, "Enable a custom activation height for testing mode")
	rootCmd.PersistentFlags().Bool("rauth", true, "Enable miners to use actual registered usernames")
	rootCmd.PersistentFlags().Int("sport", 1234, "Stratum server host port")
	rootCmd.PersistentFlags().Bool("checkallshares", true, "Check all shares submitted")
}

// Execute is cobra's entry point
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:              "prosper-pool",
	Short:            "Launch the private pool",
	PersistentPreRun: rootPreRunSetup,
	PreRunE:          HardReadConfig,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)

		pool, err := engine.Setup(viper.GetViper())
		if err != nil {
			log.WithError(err).Fatal("failed to launch pool")
		}

		pool.Run(ctx)
	},
}

var getConfig = &cobra.Command{
	Use:    "config <file-to-write>",
	Short:  "Write a example config with defaults",
	PreRun: SoftReadConfig,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(viper.WriteConfigAs(args[0]))
	},
}

var testStratum = &cobra.Command{
	Use:    "stratum",
	Short:  "Launch the stratum server",
	Hidden: true,
	PreRun: SoftReadConfig, // TODO: Do a hard read
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)

		fmt.Println("TODO") // TODO: Config setup + launch of everything
		s, err := stratum.NewServer(viper.GetViper())
		if err != nil {
			log.WithError(err).Fatal("failed to launch stratum server")
		}

		go func() {
			keyboardReader := bufio.NewReader(os.Stdin)
			for {
				userCommand, _ := keyboardReader.ReadString('\n')
				words := strings.Fields(userCommand)
				if len(words) > 0 {
					switch words[0] {
					case "listclients", "listminers":
						fmt.Println(strings.Join(s.Miners.ListMiners()[:], ", "))
					case "showmessage":
						if len(words) > 2 {
							_ = s.ShowMessage(words[1], strings.Join(words[2:], " "))
						}
					case "getversion":
						if len(words) > 1 {
							_ = s.GetVersion(words[1])
						}
					case "notify":
						if len(words) > 3 {
							_ = s.SingleClientNotify(words[1], words[2], words[3], "")
						}
					case "settarget":
						if len(words) > 2 {
							_ = s.SetTarget(words[1], words[2])
						}
					case "reconnect":
						if len(words) > 4 {
							_ = s.ReconnectClient(words[1], words[2], words[3], words[4])
						}
					default:
						fmt.Println("Server command not supported: ", words[0])
					}
				}
			}
		}()

		s.Listen(ctx)
	},
}

// rootPreRunSetup is run before the root command
func rootPreRunSetup(cmd *cobra.Command, args []string) {
	// Catch ctl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		log.Info("Gracefully closing")

		// We will give it 3 seconds to close gracefully.
		// If anything is hanging beyond that, just kill it.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		err := exit.GlobalExitHandler.CloseWithTimeout(ctx)
		if err != nil {
			log.Warn("took too long to close")
			os.Exit(1)
		}
	}()

	config.SetDefaults(viper.GetViper())
	_ = viper.BindPFlag(config.ConfigSQLHost, cmd.Flags().Lookup("phost"))
	_ = viper.BindPFlag(config.ConfigSQLPort, cmd.Flags().Lookup("pport"))
	_ = viper.BindPFlag(config.ConfigFactomdLocation, cmd.Flags().Lookup("fhost"))
	_ = viper.BindPFlag(config.LoggingLevel, cmd.Flags().Lookup("log"))
	_ = viper.BindPFlag(config.ConfigStratumRequireAuth, cmd.Flags().Lookup("rauth"))
	_ = viper.BindPFlag(config.ConfigStratumPort, cmd.Flags().Lookup("sport"))
	_ = viper.BindPFlag(config.ConfigStratumCheckAllWork, cmd.Flags().Lookup("checkallshares"))

	// Handle testing mode
	if ok, _ := cmd.Flags().GetBool("testing"); ok {
		act, _ := cmd.Flags().GetUint32("act")
		config.GradingV2Activation = act
		config.PegnetActivation = act
		config.PEGPricingActivation = act
		config.TransactionConversionActivation = act
		config.FreeFloatingPEGPriceActivation = act
		config.V4OPRActivation = act
		config.V20HeightActivation = act
	}

	if v, _ := cmd.Flags().GetInt("testingact"); v != 0 {
		config.V20HeightActivation = uint32(v)
	}

}

// TODO: Move testMiner to its own binary
var testMiner = &cobra.Command{
	Use:    "miner",
	Short:  "Launch a miner",
	Hidden: true,
	PreRun: SoftReadConfig,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)

		client, err := stratum.NewClient("user", "miner", "password", "invitecode", "payoutaddress", config.CompiledInVersion, nil)
		if err != nil {
			panic(err)
		}

		exit.GlobalExitHandler.AddExit(func() error {
			return client.Close()
		})

		err = client.Connect("localhost:1234")
		if err != nil {
			panic(err)
		}

		_ = client.Handshake()

		keyboardReader := bufio.NewReader(os.Stdin)
		go func() {
			for {
				userCommand, _ := keyboardReader.ReadString('\n')
				words := strings.Fields(userCommand)
				if len(words) > 0 {
					switch words[0] {
					case "getopr":
						if len(words) > 1 {
							_ = client.GetOPRHash(words[1])
						}
					case "suggesttarget":
						if len(words) > 1 {
							_ = client.SuggestTarget(words[1])
						}
					default:
						fmt.Println("Client command not supported: ", words[0])
					}
				}
			}
		}()

		client.Listen(ctx)
	},
}

var testSync = &cobra.Command{
	Use:    "sync",
	Short:  "Run the pegnet sync",
	Hidden: true,
	PreRun: SoftReadConfig,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)
		conf := viper.GetViper()

		db, err := database.New(conf)
		if err != nil {
			panic(err)
		}
		exit.GlobalExitHandler.AddExit(func() error {
			return db.Close()
		})

		p, err := pegnet.NewPegnetNode(conf, db)
		if err != nil {
			panic(err)
		}

		p.DBlockSync(ctx)
		var _ = ctx
	},
}

var testAccountant = &cobra.Command{
	Use:    "accountant",
	Short:  "Run the pool accountant",
	Hidden: true,
	PreRun: SoftReadConfig,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)
		conf := viper.GetViper()

		db, err := database.New(conf)
		if err != nil {
			panic(err)
		}

		a, err := accounting.NewAccountant(conf, db.DB)
		if err != nil {
			panic(err)
		}

		go func() {
			users := 3
			for i := 0; i < 10; i++ {
				if ctx.Err() != nil {
					return
				}

				job := int32(i)
				a.NewJob(job) // Force the new job
				for u := 0; u < users; u++ {
					for w := 0; w < 3; w++ {
						a.ShareChannel() <- &accounting.Share{
							JobID:      job,
							Difficulty: rand.Float64() * 20,
							Accepted:   false,
							MinerID:    fmt.Sprintf("user-%d_%d", u, w),
							UserID:     fmt.Sprintf("user-%d", u),
						}
					}
				}

				time.Sleep(100 * time.Millisecond)
				a.RewardChannel() <- &accounting.Reward{
					JobID:      job,
					PoolReward: 200 * 1e8 * 12,
					Winning:    12,
					Graded:     15,
				}
			}
		}()

		a.Listen(ctx)
		var _ = ctx
	},
}

var testAuth = &cobra.Command{
	Use:    "auth",
	Short:  "Run the pegnet authenticator",
	Hidden: true,
	PreRun: SoftReadConfig,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		exit.GlobalExitHandler.AddCancel(cancel)
		conf := viper.GetViper()

		db, err := database.New(conf)
		if err != nil {
			panic(err)
		}
		exit.GlobalExitHandler.AddExit(func() error {
			return db.Close()
		})

		a, err := authentication.NewAuthenticator(viper.GetViper(), db.DB)
		if err != nil {
			panic(err)
		}

		mux := http.NewServeMux()

		// Mount Auth to Router
		mux.Handle("/auth/", a.NewServeMux())
		var _ = ctx
		srv := http.Server{Addr: ":9000", Handler: manager.SessionManager.Middleware(mux)}
		go func() {
			err := srv.ListenAndServe()
			if err != nil {
				fmt.Println(err)
			}
		}()

	InfiniteLoop:
		for {
			select {
			case <-ctx.Done():
				_ = srv.Close()
				break InfiniteLoop
			}
		}
	},
}

var datasourcesPull = &cobra.Command{
	Use:    "pullsources",
	Short:  "Runs through all configured datasources",
	PreRun: SoftReadConfig,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Default to printing everything
		d := polling.NewDataSources(viper.GetViper(), false)
		all := d.PullAllSources()
		j, err := json.Marshal(all)
		if err != nil {
			return err
		}
		fmt.Println(string(j))

		return nil
	},
}

// Direct copy from Pegnet
var datasources = &cobra.Command{
	Use:   "datasources [assets or datasource]",
	Short: "Reads a config and outputs the data sources and their priorities",
	Long: "When setting up a datasource config, this cmd will help you verify your config is set " +
		"correctly. It will also help you ensure you have redudent data sources. " +
		"This command can also provide all datasources, and what assets they support. As well as the " +
		"opposite; given an asset what datasources include it.",
	Example:   "prosper-pool datasources FCT\nprosper-pool datasources CoinMarketCap",
	Args:      cobra.MaximumNArgs(1),
	PreRun:    SoftReadConfig,
	ValidArgs: append(opr.V2Assets, polling.AllDataSourcesList()...),
	RunE: func(cmd *cobra.Command, args []string) error {
		// User selected a data source or asset
		if len(args) == 1 {
			if AssetListContainsCaseInsensitive(opr.V2Assets, args[0]) {
				// Specified an asset
				asset := strings.ToUpper(args[0])

				// Find all exchanges for the asset
				fmt.Printf("Asset : %s\n", asset)

				var sources []string
				for k, v := range polling.AllDataSources {
					if AssetListContains(v.SupportedPegs(), asset) {
						sources = append(sources, k)
					}
				}
				fmt.Printf("Datasources : %v\n", sources)
			} else if AssetListContainsCaseInsensitive(polling.AllDataSourcesList(), args[0]) {
				// Specified an exchange
				source := polling.CorrectCasing(args[0])
				s, ok := polling.AllDataSources[source]
				if !ok {
					return fmt.Errorf("%s is not a supported datasource", args[0])
				}

				fmt.Printf("Datasource : %s\n", s.Name())
				fmt.Printf("Datasource URL : %s\n", s.Url())
				fmt.Printf("Supported peg pricing\n")
				for _, asset := range s.SupportedPegs() {
					fmt.Printf("\t%s\n", asset)
				}
			} else {
				// Should never happen
				fmt.Println("This should never happen. The provided argument is invalid")
			}
			return nil
		}

		// Default to printing everything
		d := polling.NewDataSources(viper.GetViper(), false)

		// Time to print
		fmt.Println("Data sources in priority order")
		fmt.Printf("\t%s\n", d.PriorityListString())

		fmt.Println()
		fmt.Println("Assets and their data source order. The order left to right is the fallback order.")
		for _, asset := range opr.V2Assets {
			str := d.AssetPriorityString(asset)
			fmt.Printf("\t%4s (%d) : %s\n", asset, len(d.AssetSources[asset]), str)
		}
		return nil
	},
}

func setConfigLoc(cmd *cobra.Command, args []string) (string, bool) {
	configPath, _ := cmd.Flags().GetString("config")
	path := os.ExpandEnv(configPath)

	dir := filepath.Dir(path)
	name := filepath.Base(path)
	viper.AddConfigPath(dir)

	ext := filepath.Ext(name)
	viper.SetConfigName(strings.TrimSuffix(name, ext))

	info, err := os.Stat(path)
	exists := info != nil && !os.IsNotExist(err)
	return path, exists
}

// SoftReadConfig will not fail. It can be used for a command that needs the config,
// but is happy with the defaults
func SoftReadConfig(cmd *cobra.Command, args []string) {
	loadProfiler(cmd)
	path, exists := setConfigLoc(cmd, args)
	var _, _ = path, exists

	err := viper.ReadInConfig()
	if err != nil {
		log.WithError(err).Debugf("failed to load config")
	}

	initLogger()
}

func loadProfiler(cmd *cobra.Command) {
	if pro, _ := cmd.Flags().GetBool("profile"); pro {
		go profile.StartProfiler(false, 6040) // Only localhost, on 6040
	}
}

// HardReadConfig requires a config file
func HardReadConfig(cmd *cobra.Command, args []string) error {
	loadProfiler(cmd)
	path, exists := setConfigLoc(cmd, args)
	if !exists {
		return fmt.Errorf("config does not exist at %s", path)
	}

	initLogger()

	return viper.ReadInConfig()
}

func initLogger() {
	switch strings.ToLower(viper.GetString(config.LoggingLevel)) {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	}

	log.StandardLogger().Hooks.Add(&loghelp.ContextHook{})
}
