package main

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/rpc"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rpcCmd = &cobra.Command{
	Use:   "rpc",
	Short: "Control the miner service with RPC",
	Args: cobra.ExactValidArgs(1),
	ValidArgs: []string{"getStatus", "isRunning", "start", "stop"},
	Run: func(cmd *cobra.Command, args []string) {
		method := args[0]
		ctx := context.Background()
		clientpipe, err := rpc.DialIPC(ctx, NamedPipeName)
		defer clientpipe.Close()
		if err != nil {
			log.Fatal("Failed to connect to the named pipe")
		}
		switch method {
		case "getStatus":
			var result MiningStatus
			if err := clientpipe.Call(&result, "mining_getStatus"); err != nil {
				log.WithError(err).Fatal("Failed to call mining_getStatus")
			}
			fmt.Println("mining_getStatus:")
			fmt.Printf("\tisRunning:\t%t\n", result.IsRunning)
			fmt.Printf("\tisConnected:\t%t\n", result.IsConnected)
			fmt.Printf("\tpoolHostAndPort:\t%s\n", result.PoolHostAndPort)
			fmt.Printf("\tdurationConnected:\t%d\n", result.DurationConnected)
			fmt.Printf("\tblocksSubmitted:\t%d\n", result.BlocksSubmitted)
		case "isRunning":
			var result bool
			if err := clientpipe.Call(&result, "mining_isRunning"); err != nil {
				log.WithError(err).Fatal("Failed to call mining_isRunning")
			}
			fmt.Printf("mining_isRunning: %t\n", result)
		case "stop":
			clientpipe.Call(nil, "mining_stop")
		case "start":
			clientpipe.Call(nil, "mining_start")
		}
	},
}
