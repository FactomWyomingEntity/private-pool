package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/rpc"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var feedCmd = &cobra.Command{
	Use:    "feed",
	Short:  "Show notifications from the miner service",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		clientpipe, err := rpc.DialIPC(ctx, NamedPipeName)
		if err != nil {
			log.Fatal("Failed to connect to the named pipe")
		}
		defer clientpipe.Close()
		hashRateChannel := make(chan float64)
		submissionChannel := make(chan int)
		statusChannel := make(chan *MiningStatus)
		subscriptions := map[*rpc.ClientSubscription]bool{}

		subRate, err := clientpipe.Subscribe(ctx, "mining", hashRateChannel, "hashRateSubscription")
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe to mining:hashRateSubscription")
		}
		subscriptions[subRate] = true
		subStatus, err := clientpipe.Subscribe(ctx, "mining", statusChannel, "statusSubscription")
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe to mining:statusSubscription")
		}
		subscriptions[subStatus] = true
		subSubmission, err := clientpipe.Subscribe(ctx, "mining", submissionChannel, "submissionSubscription")
		if err != nil {
			log.WithError(err).Fatal("Failed to subscribe to mining:submissionSubscription")
		}
		subscriptions[subSubmission] = true

		go func() {
			for {
				var status *MiningStatus
				select {
				case hashRate := <-hashRateChannel:
					fmt.Printf("Current hash rate: %f\n", hashRate)
				case status = <-statusChannel:
					if status.IsConnected {
						fmt.Printf("Connected to %s\n", status.PoolHostAndPort)
					} else {
						fmt.Print("Disconnected from pool host\n")
					}
					status = nil
				case <-submissionChannel:
					fmt.Println("A share was submitted")
				case <-subRate.Err():
					fmt.Println("Hash rate subscription ended")
					delete(subscriptions, subRate)
					if len(subscriptions) == 0 {
						os.Exit(1)
					}
				case <-subStatus.Err():
					fmt.Println("Status subscription ended")
					delete(subscriptions, subStatus)
					if len(subscriptions) == 0 {
						os.Exit(1)
					}
				case <-subSubmission.Err():
					fmt.Println("Submission subscription ended")
					delete(subscriptions, subSubmission)
					if len(subscriptions) == 0 {
						os.Exit(1)
					}
				}
			}
		}()

		// Allow the user to exit by pressing Enter
		fmt.Println("Press Enter to terminate the feed")
		keyboardReader := bufio.NewReader(os.Stdin)
		keyboardReader.ReadString('\n')
		subRate.Unsubscribe()
		subSubmission.Unsubscribe()
		return
	},
}
