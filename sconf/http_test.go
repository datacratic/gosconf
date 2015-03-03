// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/gorest/rest/resttest"

	"testing"
	"time"
)

func (t TestRouterUtils) Endpoint(router *Router) *resttest.Server {
	return resttest.NewRootedService("/v1/configs/", &HTTPEndpoint{
		Name:       "config-endpoint",
		Router:     router,
		PathPrefix: "/",
	})
}

func TestConfigNotificationHTTP(t *testing.T) {
	test := NewTestRouterUtils(t)

	outHandler := test.NewHandler()
	outRouter := test.NewRouter(outHandler)
	endpoint := test.Endpoint(outRouter)
	defer endpoint.Close()

	inHandler, _ := NewClient(endpoint.RootedURL())
	inRouter := test.NewRouter(inHandler)

	test.Run("push-http", inRouter, outHandler)
}

func TestConfigSyncPullHTTP(t *testing.T) {
	test := NewTestRouterUtils(t)

	inRouter := test.NewRouter()
	endpoint := test.Endpoint(inRouter)
	defer endpoint.Close()

	handler := test.NewHandler()
	outRouter := test.NewRouter(handler)
	poller := Poller{
		Pull:  true,
		Local: outRouter,
		URL:   endpoint.RootedURL(),
		Rate:  5 * time.Millisecond,
	}
	poller.Start()
	defer poller.Stop()

	test.Run("syncPullTest", inRouter, handler)
}

func TestConfigSyncPushHTTP(t *testing.T) {
	test := NewTestRouterUtils(t)

	handler := test.NewHandler()
	outRouter := test.NewRouter(handler)
	endpoint := test.Endpoint(outRouter)
	defer endpoint.Close()

	inRouter := test.NewRouter()
	poller := Poller{
		Push:  true,
		Local: inRouter,
		URL:   endpoint.RootedURL(),
		Rate:  5 * time.Millisecond,
	}
	poller.Start()
	defer poller.Stop()

	test.Run("syncPushTest", inRouter, handler)
}
