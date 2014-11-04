// Copyright (c) 2014 Datacratic. All rights reserved.
//
// sconf also provides several HTTP utilities to communicate configuration
// events between sconf enabled process. The utilties are based on the gorest
// package.

package sconf_test

import (
	"github.com/datacratic/gorest/rest"
	"github.com/datacratic/gosconf/sconf"

	"time"
)

// This example explains how the various HTTP components are used to synchronize
// the live set of configuration between two sconf aware endpoints.
func Example_HTTP() {

	// We'll start by creating 2 sconf.HTTPEndpoint and make them available
	// through the gorest package. Both endpoints will be associate with
	// sconf.Router which will be used to serve configurations and receive
	// configuration events.

	routerA := new(sconf.Router)
	defer routerA.Close()

	endpointA := new(rest.TestEndpoint)
	endpointA.AddRoutable(&sconf.HTTPEndpoint{Router: routerA})
	endpointA.ListenAndServe()

	routerB := new(sconf.Router)
	defer routerB.Close()

	endpointB := new(rest.TestEndpoint)
	endpointB.AddRoutable(&sconf.HTTPEndpoint{Router: routerB})
	endpointB.ListenAndServe()

	// Here we create an sconf.HTTPClient which is used as a proxy to an
	// sconf.HTTPEndpoint.
	client := &sconf.HTTPClient{URL: endpointB.URL() + sconf.DefaultHTTPEndpointPath}

	// Since sconf.HTTPClient implements the sconf.Handler interface we can use
	// it as a notification handler in our router such that all configuration
	// events are forwarded over HTTP to our second router.
	//
	// Note that this communication protocol is not meant to be robust in the
	// presence of partitions. This means that lost events will not be detected
	// or retransmitted. You can think of this forwarding mechanism as a
	// fast-path to quickly propagate new configuration events when everything
	// is running smoothly.
	routerA.Handlers = append(routerA.Handlers, client)

	// To make sure that 2 sconf processes are always eventually synced, it's a
	// good idea to instanciate a sconf.Poller object which will periodically
	// sync the local router with the remote endpoint. This ensures that all
	// configuration events will eventually be transmitted.
	push := &sconf.Poller{Push: true, Local: routerA, Remote: client}
	push.Start()
	defer push.Stop()

	// Pollers can operate in two modes: push or pull. That is that a poller can
	// periodically push its state into the remote client or pull the remote
	// state. This is done periodically.
	pull := &sconf.Poller{Pull: true, Local: routerB, URL: endpointA.URL() + sconf.DefaultHTTPEndpointPath}
	pull.Start()
	defer pull.Stop()

	// Finally a simple handler to make sure that everything works.
	routerB.Handlers = append(routerB.Handlers, new(MyHandler))

	routerA.NewConfig(NewMyConfig("foo", 10).Wrap("id-foo", 1))
	routerA.NewConfig(NewMyConfig("bar", 10).Wrap("id-bar", 1))

	time.Sleep(10 * time.Millisecond)

	// Output:
	// NewConfig:  {config type='my-type', id='id-foo', ver=1 }
	// NewConfig:  {config type='my-type', id='id-bar', ver=1 }
}
