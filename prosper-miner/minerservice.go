package main

import (
	"time"

	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
)

type program struct {
	mining Mining
	logger service.Logger
}

func (p *program) Start (s service.Service) error {
	go p.run()
	return nil
}

// Run the miner service
//
// Miner service states:
// 1. No configuration, not mining.
//    a. Transitions to 2. by JSON-RPC request to start mining.
// 2. Seeking configuration, not mining.
//    a. Transitions to 1. by JSON-RPC request to stop mining.
//    b. Transitions to self by looking for, but not finding a configuration.
//    c. Transitions to 3. by finding a configuration.
//    d. Transitions to 5. by service termination.
// 3. Configured, not mining.
//    a. Transitions to self by failing to initialize the miner
//    b. Transitions to 4. by initializing the miner
//    c. Transitions to 5. by service termination.
// 4. Configured, mining.
//    a. Transitions to 1. by JSON-RPC request to stop mining.
//    b. Transitions to 2. by repeated network errors.
//    c. Transitions to 2. by failing to connect to pool.
//    d. Transitions to 5. by service termination.
// 5. Stopped.
//    a. Transitions to 2. by starting the service.
//
// The miner service starts in state 2.
//
// First, the Mining object is created.  It holds all shared data.  Next,
// startRPC() runs the JSON-RPC server.   Finally, the main loop/state machine
// is run.
func (p *program) run() {
	networkErrors := 0
	mining := NewMining()
	startRPCServer(mining)
	log.WithFields(log.Fields{"platform": service.Platform()}).Info("Service started")
	for {
		if !mining.IsRunning() {
			// State 1: Not running
			p.logger.Info("State 1")
			if mining.HasMinerConfig() {
				log.Warn("Miner configuration is not nil while not running")
			}
			if mining.HasStratumClient() {
				log.Warn("Stratum client is not nil while not running")
			}
			time.Sleep(time.Second)
		} else if !mining.HasMinerConfig() {
			// State 2: Seeking configuration
			p.logger.Info("State 2")
			if !mining.IsRunning() {
				log.Warn("Mining is not running while seeking configuration")
			}
			if mining.HasStratumClient() {
				log.Warn("Stratum client is not nil while seeking configuration")
			}
			if err := mining.GetMinerConfig(); err != nil {
				time.Sleep(time.Second)
			}
		} else if !mining.HasStratumClient() {
			// State 3: Initialize miners
			p.logger.Info("State 3")
			if !mining.IsRunning() {
				log.Warn("Mining is not running while starting to mine")
			}
			if !mining.HasMinerConfig() {
				log.Warn("Miner configuration is not found while starting to mine")
			}
			if err := mining.InitializeMiners(); err != nil {
				time.Sleep(time.Second)
			}
		} else if mining.IsReadyToMine() {
			// State 4: Mining
			// As long as mining is happening, this thread stays in
			// mining.MineUntilStopped().  Control is through the
			// JSON-RPC server or through a Stop command to the
			// service.
			p.logger.Info("State 4")
			if err := mining.MineUntilStopped(); err != nil {
				p.logger.Error(err)
				networkErrors++
				if networkErrors > 5 {
					mining.Reset()
					networkErrors = 0
				}
				time.Sleep(time.Second)
			}
		} else {
			p.logger.Error("Unknown state")
			// Unknown state: This is a bug.  Log it and stop.
			log.WithFields(log.Fields{"mining.running": mining.running, "mining.mc": mining.mc, "mining.client": mining.client, "mining.disconnect": mining.disconnect}).Fatal("Mining service entered unexpected state")
		}
	}
}

func (p *program) Stop(s service.Service) error {
	log.Info("Stopping")
	p.mining.Stop()
	return nil
}

func getMinerService() (service.Service, service.Logger, error) {
	svcOptions := make(service.KeyValue)
	svcOptions["Restart"] = "on-success"
	svcOptions["SuccessExitStatus"] = "1 2 8 SIGKILL"
	svcConfig := &service.Config{
		Name: "ProsperPoolMinerService",
		DisplayName: "Prosper Pool Miner Service",
		Description: "Prosper Pool PegNet miners",
		Option: svcOptions,
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		return nil, nil, err
	}
	errs := make(chan error, 5)
	logger, err := s.Logger(errs)
	if err != nil {
		log.Fatal(err)
	}
	prg.logger = logger
/*	go func() {
		for {
			err := <-errs
			if err != nil {
				log.Error(err)
			}
		}
	}() */
	return s, logger, nil
}

func runMinerService() {
	s, logger, err := getMinerService()
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
}
