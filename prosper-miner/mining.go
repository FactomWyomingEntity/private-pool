package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/FactomWyomingEntity/prosper-pool/config"
	"github.com/FactomWyomingEntity/prosper-pool/exit"
	"github.com/FactomWyomingEntity/prosper-pool/stratum"
	log "github.com/sirupsen/logrus"
)

type Mining struct {
	paused bool
	mc *minerConfig
	notificationChannels *stratum.NotificationChannels
	blocksSubmitted uint64
	client *stratum.Client
	connectedAt *time.Time
	disconnect context.CancelFunc
	sync.RWMutex
}

func NewMining () *Mining {
	log.Trace("NewMining()")
	mining := &Mining{}
	mining.paused = false
	mining.notificationChannels = stratum.NewNotificationChannels()
	return mining
}

func (m *Mining) GetHashRateChannel() (chan float64) {
	if m.notificationChannels != nil {
		return m.notificationChannels.HashRateChannel
	} else {
		return nil
	}
}

func (m *Mining) GetMinerConfig() error {
	log.Trace("Mining.GetMinerConfig()")
	m.Lock()
	defer m.Unlock()
	var err error = nil
	m.mc, err = getMinerConfig("")
	return err
}

type MiningStatus struct {
	IsConnected bool	`json:"isConnected"`
	IsPaused bool		`json:"isPaused"`
	PoolHostAndPort string	`json:"poolHostAndPort,omitempty"`
	DurationConnected uint64	`json:"durationConnected,omitempty"`
	BlocksSubmitted uint64	`json:"blocksSubmitted,omitempty"`
}

func (m *Mining) GetStatus() *MiningStatus {
	log.Trace("Mining.GetStatus()")
	var status = &MiningStatus{}
	m.RLock()
	defer m.RUnlock()
	status.IsPaused = m.paused
	status.IsConnected = m.connectedAt != nil
	if status.IsConnected {
		status.PoolHostAndPort = m.client.RemoteAddr()
		status.DurationConnected = uint64(time.Since(*m.connectedAt).Seconds())
		status.BlocksSubmitted = m.client.TotalSuccesses() + m.blocksSubmitted
	} else {
		status.BlocksSubmitted = m.blocksSubmitted
	}
	return status
}

func (m *Mining) GetSubmissionChannel() (chan int) {
	if m.notificationChannels != nil {
		return m.notificationChannels.SubmissionChannel
	} else {
		return nil
	}
}

func (m *Mining) HasMinerConfig() bool {
	m.RLock()
	defer m.RUnlock()
	return m.mc != nil
}

func (m *Mining) HasStratumClient() bool {
	m.RLock()
	defer m.RUnlock()
	return m.client != nil
}

func (m *Mining) InitializeMiners () error {
	log.Trace("Mining.InitializeMiners()")
	if !m.HasMinerConfig() {
		log.Error("Cannot start mining without miner configuration")
		return errors.New("Miner configuration is missing")
	}
	m.RLock()
	initialize := m.client == nil
	m.RUnlock()
	if initialize {
		var err error = nil
		// Create the Stratum client
		// No need to pass the password, invitation code or payout address.
		// The service should not handle the sign-up process.
		m.Lock()
		m.client, err = stratum.NewClient(m.mc.emailaddress, m.mc.minerid, "", "", "", config.CompiledInVersion, m.notificationChannels)
		concurrentminers := m.mc.concurrentminers
		hashtabledirectory := m.mc.hashtabledirectory
		m.Unlock()
		if err != nil {
			m.RLock()
			log.WithFields(log.Fields{ConfigEmailAddressKey: m.mc.emailaddress,  ConfigMinerIdKey: m.mc.minerid, "CompiledInVersion": config.CompiledInVersion}).Debug("Failed to create a Stratum client")
			m.RUnlock()
			log.Error(err)
			return errors.New("Failed to create new Stratum client")
		}
		m.client.InitMiners(concurrentminers, hashtabledirectory)
	}
	return nil
}

func (m *Mining) IsReadyToMine() bool {
	m.RLock()
	defer m.RUnlock()
	return m.paused == false && m.mc != nil && m.client != nil
}

func (m *Mining) IsPaused() bool {
	m.RLock()
	defer m.RUnlock()
	return m.paused
}

func (m *Mining) MineUntilStopped() error {
	log.Trace("Mining.MineUntilStopped()")
	ctx, cancel := context.WithCancel(context.Background())
	exit.GlobalExitHandler.AddCancel(cancel)
	m.Lock()
	poolhostandport := m.mc.poolhostandport
	m.disconnect = cancel
	m.Unlock()
	m.client.RunMiners(ctx)
	exit.GlobalExitHandler.AddExit(func() error {
		m.Lock()
		m.resetWhileLocked()
		m.Unlock()
		return nil
	})
	err := m.client.Connect(poolhostandport)
	if err == nil {
		m.Lock()
		t := time.Now()
		m.connectedAt = &t
		m.Unlock()
	} else {
		log.WithFields(log.Fields{ConfigPoolHostAndPortKey: poolhostandport}).Debug("Failed to connect to the pool host")
		log.Error(err)
		m.Lock()
		m.disconnect()
		m.disconnect = nil
		m.Unlock()
		return err
	}
	m.client.Handshake()
	err = m.client.Listen(ctx)
	m.Lock()
	cancel()
	m.disconnect = nil
	m.connectedAt = nil
	m.Unlock()
	return err
}

func (m *Mining) Reset() {
	log.Trace("Mining.Reset()")
	m.Lock()
	m.resetWhileLocked()
	m.Unlock()
}

func (m *Mining) Start() {
	log.Trace("Mining.Start()")
	m.Lock()
	defer m.Unlock()
	m.paused = false
}

func (m *Mining) Stop() {
	log.Trace("Mining.Stop()")
	m.Lock()
	defer m.Unlock()
	m.paused = true
	m.resetWhileLocked()
}

// private methods

func (m *Mining) resetWhileLocked() {
	log.Trace("Mining.resetWhileLocked()")
	if m.disconnect != nil {
		log.Warn("Disconnecting")
		m.disconnect()
		m.disconnect = nil
	}
	if m.client != nil {
		successes := m.client.TotalSuccesses()
		log.WithFields(log.Fields{"successes": successes}).Debug("Adding blocksSubmitted")
		m.blocksSubmitted += m.client.TotalSuccesses()
	}
	m.mc = nil
	m.client = nil
}
