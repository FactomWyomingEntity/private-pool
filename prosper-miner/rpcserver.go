package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	log "github.com/sirupsen/logrus"
)

var NamedPipeName = `\\.\pipe\ProsperPoolService`

type hashRateChannel <-chan float64
type hashRateSubscriptionMap map[*hashRateSubscription]bool
type statusChannel <-chan *MiningStatus
type statusSubscriptionMap map[*statusSubscription]bool
type submissionChannel <-chan int
type submissionSubscriptionMap map[*submissionSubscription]bool

type MinerRPCService struct {
	m *Mining
	hashRateSubscriptions hashRateSubscriptionMap
	statusSubscriptions statusSubscriptionMap
	submissionSubscriptions submissionSubscriptionMap
	sync.RWMutex
}

type hashRateSubscription struct {
	subscription *rpc.Subscription
	notifier     *rpc.Notifier
	subscriptions *hashRateSubscriptionMap
	channel       hashRateChannel
}

type statusSubscription struct {
	subscription *rpc.Subscription
	notifier     *rpc.Notifier
	subscriptions *statusSubscriptionMap
	channel       statusChannel
}

type submissionSubscription struct {
	subscription *rpc.Subscription
	notifier     *rpc.Notifier
	subscriptions *submissionSubscriptionMap
	channel       submissionChannel
}

// RPC methods

// Exposed as mining_getStatus
func (s MinerRPCService) GetStatus() *MiningStatus {
	var status *MiningStatus
	status = s.m.GetStatus()
	return status
}

// Exposed as mining_isRunning
func (s MinerRPCService) IsRunning() bool {
	return s.m.IsRunning()
}

// Exposed as mining_start
func (s MinerRPCService) Start() {
	s.m.Start()
}

// Exposed as mining_stop
func (s MinerRPCService) Stop() {
	s.m.Stop()
}

// Subscriptions
//
// Each subscription has a channel that notifies the service that there
// is a new event.  This event needs to be turned into a call to
// notifier.Notify() for each subscribed client.

func (s MinerRPCService) HashRateSubscription(ctx context.Context) (*rpc.Subscription, error) {
	log.Trace("MinerRPCService.HashRateSubscription")
	notifier, subscription, err := getNotifierAndSubscription(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("Adding hash rate subscription")
	s.addHashRateSubscription(subscription, notifier)

	return subscription, nil
}

func (s MinerRPCService) StatusSubscription(ctx context.Context) (*rpc.Subscription, error) {
	log.Trace("MinerRPCService.StatusSubscription")
	notifier, subscription, err := getNotifierAndSubscription(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("Adding status subscription")
	s.addStatusSubscription(subscription, notifier)

	return subscription, nil
}

func (s MinerRPCService) SubmissionSubscription(ctx context.Context) (*rpc.Subscription, error) {
	log.Trace("MinerRPCService.SubmissionSubscription")
	notifier, subscription, err := getNotifierAndSubscription(ctx)
	if err != nil {
		return nil, err
	}
	log.Debug("Adding submission subscription")
	s.addSubmissionSubscription(subscription, notifier)

	return subscription, nil
}

// Private methods

func (s MinerRPCService) addHashRateSubscription(subscription *rpc.Subscription, notifier *rpc.Notifier) error {
	log.Trace("MinerRPCService.addHashRateSubscription")
	sub := &hashRateSubscription{subscription, notifier, &s.hashRateSubscriptions, s.m.GetHashRateChannel()}
	if sub.channel != nil {
		s.Lock()
		(*sub.subscriptions)[sub] = true
		s.Unlock()
		log.Debug("Starting hash rate monitor")
		go s.hashRateSubscriptionMonitor(sub)
		if len(*sub.subscriptions) == 1 {
			log.Debug("Starting hash rate worker")
			go s.hashRateSubscriptionWorker(sub.channel)
		}
	} else {
		return errors.New("Unable to get hash rate channel")
	}
	return nil
}

func (s MinerRPCService) addStatusSubscription(subscription *rpc.Subscription, notifier *rpc.Notifier) error {
	log.Trace("MinerRPCService.addStatusSubscription")
	sub := &statusSubscription{subscription, notifier, &s.statusSubscriptions, s.m.GetStatusChannel()}
	if sub.channel != nil {
		s.Lock()
		(*sub.subscriptions)[sub] = true
		s.Unlock()
		log.Debug("Starting status subscription monitor")
		go s.statusSubscriptionMonitor(sub)
		if len(*sub.subscriptions) == 1 {
			log.Debug("Starting status subscription worker")
			go s.statusSubscriptionWorker(sub.channel)
		}
	} else {
		return errors.New("Unable to get status channel")
	}
	return nil
}

func (s MinerRPCService) addSubmissionSubscription(subscription *rpc.Subscription, notifier *rpc.Notifier) error {
	log.Trace("MinerRPCService.addSubmissionSubscription")
	sub := &submissionSubscription{subscription, notifier, &s.submissionSubscriptions, s.m.GetSubmissionChannel()}
	if sub.channel != nil {
		s.Lock()
		(*sub.subscriptions)[sub] = true
		s.Unlock()
		log.Debug("Starting submission subscription monitor")
		go s.submissionSubscriptionMonitor(sub)
		if len(*sub.subscriptions) == 1 {
			log.Debug("Starting submission subscription worker")
			go s.submissionSubscriptionWorker(sub.channel)
		}
	} else {
		return errors.New("Unable to get submission channel")
	}
	return nil
}

func (s MinerRPCService) hashRateSubscriptionMonitor(sub *hashRateSubscription) {
	log.Trace("MinerRPCService.hashRateSubscriptionMonitor")
	subscription := sub.subscription
	notifier := sub.notifier
	for {
		select {
		case <-notifier.Closed():
			log.Debug("Canceling hash rate subscription on notifier.Closed")
			s.removeHashRateSubscription(sub)
			return
		case <-subscription.Err():
			log.Debug("Canceling hash rate subscription on subscription.Err")
			s.removeHashRateSubscription(sub)
			return
		default:
			time.Sleep(2 * time.Second)
		}
	}
}

func (s MinerRPCService) hashRateSubscriptionWorker(channel hashRateChannel) {
	log.Trace("MinerRPCService.hashRateSubscriptionWorker")
	for {
		select {
		case i := <-channel:
			s.RLock()
			if len(s.hashRateSubscriptions) == 0 {
				s.RUnlock()
				break
			} else {
				for sub, _ := range s.hashRateSubscriptions {
					sub.notifier.Notify(sub.subscription.ID, i)
				}
			}
			s.RUnlock()
		}
	}
}

func (s MinerRPCService) removeHashRateSubscription(sub *hashRateSubscription) {
	log.Trace("MinerRPCService.removeHashRateSubscription")
	s.Lock()
	defer s.Unlock()
	delete(s.hashRateSubscriptions, sub)
}

func (s MinerRPCService) removeStatusSubscription(sub *statusSubscription) {
	log.Trace("MinerRPCService.removeStatusSubscription")
	s.Lock()
	defer s.Unlock()
	delete(s.statusSubscriptions, sub)
}

func (s MinerRPCService) removeSubmissionSubscription(sub *submissionSubscription) {
	log.Trace("MinerRPCService.removeSubmissionSubscription")
	s.Lock()
	defer s.Unlock()
	delete(s.submissionSubscriptions, sub)
}

func (s MinerRPCService) statusSubscriptionMonitor(sub *statusSubscription) {
	log.Trace("MinerRPCService.statusSubscriptionMonitor")
	subscription := sub.subscription
	notifier := sub.notifier
	for {
		select {
		case <-notifier.Closed():
			log.Debug("Canceling status subscription on notifier.Closed")
			s.removeStatusSubscription(sub)
			return
		case <-subscription.Err():
			log.Debug("Canceling status subscription on subscription.Err")
			s.removeStatusSubscription(sub)
			return
		default:
			time.Sleep(2 * time.Second)
		}
	}
}

func (s MinerRPCService) statusSubscriptionWorker(channel statusChannel) {
	log.Trace("MinerRPCService.statusSubscriptionWorker")
	for {
		select {
		case i := <-channel:
			s.RLock()
			if len(s.statusSubscriptions) == 0 {
				s.RUnlock()
				break
			} else {
				for sub, _ := range s.statusSubscriptions {
					sub.notifier.Notify(sub.subscription.ID, i)
				}
			}
			s.RUnlock()
		}
	}
}

func (s MinerRPCService) submissionSubscriptionMonitor(sub *submissionSubscription) {
	log.Trace("MinerRPCService.submissionSubscriptionMonitor")
	subscription := sub.subscription
	notifier := sub.notifier
	for {
		select {
		case <-notifier.Closed():
			log.Debug("Canceling submission subscription on notifier.Closed")
			s.removeSubmissionSubscription(sub)
			return
		case <-subscription.Err():
			log.Debug("Canceling submission subscription on subscription.Err")
			s.removeSubmissionSubscription(sub)
			return
		default:
			time.Sleep(2 * time.Second)
		}
	}
}

func (s MinerRPCService) submissionSubscriptionWorker(channel submissionChannel) {
	log.Trace("MinerRPCService.submissionSubscriptionWorker")
	for {
		select {
		case i := <-channel:
			s.RLock()
			if len(s.submissionSubscriptions) == 0 {
				s.RUnlock()
				break
			} else {
				for sub, _ := range s.submissionSubscriptions {
					sub.notifier.Notify(sub.subscription.ID, i)
				}
			}
			s.RUnlock()
		}
	}
}

func startRPCServer(mining *Mining) {
	log.Trace("startRPCServer")
	newApi := rpc.API{}
	newApi.Namespace = "mining"
	newApi.Version = "1"
	newApi.Service = MinerRPCService{mining, hashRateSubscriptionMap{}, statusSubscriptionMap{}, submissionSubscriptionMap{}, sync.RWMutex{}}
	newApi.Public = true
	_, _, err := rpc.StartIPCEndpoint(NamedPipeName, []rpc.API{newApi})
	if err != nil {
		// This may happen if the service is already running
		log.WithError(err).Fatal("Failed to start the RPC Server on the named pipe")
	}
}

func getNotifierAndSubscription(ctx context.Context) (*rpc.Notifier, *rpc.Subscription, error) {
	log.Trace("getNotifierAndSubscription")
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, nil, rpc.ErrNotificationsUnsupported
	}
	subscription := notifier.CreateSubscription()
	return notifier, subscription, nil
}
