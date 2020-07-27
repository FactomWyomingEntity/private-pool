package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var serviceCmd = &cobra.Command{
	Use:   "service",
	Short: "Manage the miner service",
	Args: cobra.ExactValidArgs(1),
	ValidArgs: []string{"debug", "install", "restart", "start", "status", "stop", "uninstall"},
	Run: func(cmd *cobra.Command, args []string) {
		action := args[0]
		if action == "debug" {
			runMinerService()
			return
		}
		s, _, err := getMinerService()
		if err != nil {
			log.Fatal(err)
			return
		}
		switch action {
		case "install":
			err = s.Install()
		case "restart":
			err = s.Restart()
		case "start":
			err = s.Start()
		case "status":
			var status service.Status
			status, err = s.Status()
			if err == nil {
				var statusText string
				var exitCode int
				if status == service.StatusRunning {
					statusText = "Running"
				} else {
					statusText = "Not running"
					exitCode = 1
				}
				fmt.Println(statusText)
				os.Exit(exitCode)
			}
		case "stop":
			err = s.Stop()
		case "uninstall":
			err = s.Uninstall()
		}
		if err != nil {
			log.Fatal(err)
		}
	},
}
