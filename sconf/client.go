// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"
)

// Client represents a configuration client which can be used to
// communicate with a config endpoint. The client needs to be able to notify the
// endpoint via the NewConfig and DeadConfig options as well as perform basic
// syncing operations via the PushConfigs and PullConfigs functions.
type Client interface {
	Handler
	PushConfigs(*Configs)
	PullConfigs() *Configs
}

// ClientFactory defines a function type used to create new Client
// objects from a given URL string. Factories should be registered with the
// RegisterClient function.
type ClientFactory func(URL string) (Client, error)

var clientRegistry map[string]ClientFactory

// RegisterClient registers a factory function with the given URL
// scheme. Once registered, Client objects can be created via the
// NewClient function.
func RegisterClient(name string, factory ClientFactory) {
	if clientRegistry == nil {
		clientRegistry = map[string]ClientFactory{
			"null": NewNullClient,
		}
	}

	if _, ok := clientRegistry[name]; ok {
		log.Panicf("duplicate config client: %s", name)
	}

	clientRegistry[name] = factory
}

// NewClient creates a new Client object from the given URL string
// or returns an error if the no ClientFacotry was registered for the
// URL's scheme.
func NewClient(rawURL string) (client Client, err error) {
	URL, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	factory, ok := clientRegistry[URL.Scheme]
	if !ok {
		err = fmt.Errorf("no config clients registered for scheme: %s", URL.Scheme)
		return
	}

	client, err = factory(rawURL)
	return
}

// NullClient is a noop config client.
type NullClient struct{}

// NewNullClient creates a new noop config client.
func NewNullClient(URL string) (Client, error) {
	return &NullClient{}, nil
}

// NewConfig does nothing.
func (*NullClient) NewConfig(*Config) {}

// DeadConfig does nothing.
func (*NullClient) DeadConfig(*Tombstone) {}

// PushConfigs does nothing.
func (*NullClient) PushConfigs(*Configs) { return }

// PullConfigs returns an nil Configs object.
func (*NullClient) PullConfigs() (configs *Configs) { return }

// Poller periodically polls a configuration endpoint via a Client
// to either push or pull a set configs into a Router.
type Poller struct {

	// Local indicates the Router object which will act as the config
	// container and notification handler while polling. Must be set before
	// calling Init and can't be modified afterwards.
	Local Client

	// URL indicates where the config endpoint can be reached. It is used to
	// create a new Client using the NewClient function. Either URL
	// or Client must be set before calling Init and can't be changed
	// afterwards.
	URL string

	// Remote is used to forward the config notifications to a config
	// endpoint. Either URL or Client must be set before calling Init and can't
	// be changed afterwards.
	Remote Client

	// Push indicates that the poller will push all of Router's configs into
	// the configuration endpoint. At least one of Push or Pull must be set
	// before calling Init and can't be changed afterwards.
	Push bool

	// Pull indicates that the poller will pull all the configs from the
	// configuration endpoint and push them into the Router accordingly. At
	// least one of Push or Pull must be set before calling Init and can't be
	// changed afterwards.
	Pull bool

	// Rate indicates the frequency at which the configuration endpoint should
	// be pulled. Defaults to once every hour.
	Rate time.Duration

	initialize sync.Once
	isRunning  bool

	stopC chan int
}

// Init initializes the object.
func (poller *Poller) Init() {
	poller.initialize.Do(poller.init)
}

func (poller *Poller) init() {
	if len(poller.URL) != 0 {
		client, err := NewClient(poller.URL)
		if err != nil {
			log.Panicf("unable to init Poller: %s", err.Error())
		}
		poller.Remote = client
	}

	if poller.Remote == nil {
		log.Panic("Remote or URL must be set in Poller")
	}

	if poller.Local == nil {
		log.Panic("Local must be set in Poller")
	}

	if !poller.Push && !poller.Pull {
		log.Panic("Push and/or Pull must be set in Poller")
	}

	if poller.Rate == 0 {
		poller.Rate = 1 * time.Hour
	}

	poller.stopC = make(chan int)
}

// Start begins the periodic polling process. Polling is done in a background
// goroutine.
func (poller *Poller) Start() {
	poller.Init()

	if poller.isRunning {
		return
	}
	poller.isRunning = true

	go func() {
		poller.poll()
		tickC := time.Tick(poller.Rate)

		for {
			select {

			case <-tickC:
				poller.poll()

			case <-poller.stopC:
				poller.isRunning = false
				return

			}
		}
	}()
}

// Stop ends the periodic polling process and kills the background goroutine.
func (poller *Poller) Stop() {
	if poller.isRunning {
		poller.stopC <- 1
	}
}

func (poller *Poller) poll() {
	if poller.Push {
		poller.Remote.PushConfigs(poller.Local.PullConfigs())
	}

	if poller.Pull {
		poller.Local.PushConfigs(poller.Remote.PullConfigs())
	}
}
