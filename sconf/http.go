// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/gorest/rest"

	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// DefaultHTTPEndpointPath contains the default path prefix used by
// HTTPEndpoint.
var DefaultHTTPEndpointPath = "/v1/configs"

// HTTPEndpointMetrics contains the result of a config event received by an
// HTTPEndpoint.
type HTTPEndpointMetrics struct {

	// Request indicates that a new HTTP request was received.
	Request bool

	// RequestLatency mesures how long it took to process the HTTP request.
	RequestLatency time.Duration

	// NewConfig indicates that a new config was received.
	NewConfig bool

	// NewConfigLatency mesures how long it took the process the NewConfig
	// event.
	NewConfigLatency time.Duration

	// DeadConfig indicates that a config tombstone was received.
	DeadConfig bool

	// DeadConfigLatency mesures how long it took to process the DeadConfig
	// event.
	DeadConfigLatency time.Duration

	// PullConfigs indicates a request for the list of all configs and
	// tombstones.
	PullConfigs bool

	// PullConfigsLatency mesures how long it took to list all the configs and
	// tombstones.
	PullConfigsLatency time.Duration

	// PushConfigs indicates that the endpoint received a list of configs to
	// merge.
	PushConfigs bool

	// PushConfigs mesures how long it took to merge an incoming list of configs
	// and tombstones.
	PushConfigsLatency time.Duration
}

// HTTPEndpoint is an HTTP endpoint used to process various config related
// events. The endpoint uses a Router to access the list of existing
// configs and to push new configs or tombstones.
type HTTPEndpoint struct {
	Component

	// PathPrefix is the HTTP path used to reach this endpoint. Defaults to
	// DefaultHTTPEndpointPath.
	PathPrefix string

	// Router will be used to process config events received by this endpoint.
	Router *Router
}

// RESTRoutes returns the REST routes for the config endpoint.
func (endpoint *HTTPEndpoint) RESTRoutes() rest.Routes {
	path := endpoint.PathPrefix
	if len(path) == 0 {
		path = DefaultHTTPEndpointPath
	}

	return rest.Routes{
		rest.NewRoute("GET", path, endpoint.PullConfigs),
		rest.NewRoute("PUT", path, endpoint.PushConfigs),
		rest.NewRoute("POST", path, endpoint.NewConfig),
		rest.NewRoute("DELETE", path, endpoint.DeadConfig),
	}
}

// PullConfigs returns all the configs and tombstones managed by this endpoint.
func (endpoint *HTTPEndpoint) PullConfigs() *Configs {
	t0 := time.Now()

	configs := endpoint.Router.PullConfigs()

	endpoint.RecordMetrics(&HTTPEndpointMetrics{
		Request: true, PullConfigs: true, PullConfigsLatency: time.Since(t0)})

	return configs
}

// PushConfigs merges the given configs with the configs managed by the endpoint.
func (endpoint *HTTPEndpoint) PushConfigs(configs *Configs) {
	t0 := time.Now()

	endpoint.Router.PushConfigs(configs)

	endpoint.RecordMetrics(&HTTPEndpointMetrics{
		Request: true, PushConfigs: true, PushConfigsLatency: time.Since(t0)})
}

// NewConfig adds the given config to the configs managed by this endpoint.
func (endpoint *HTTPEndpoint) NewConfig(config *Config) {
	t0 := time.Now()

	endpoint.Router.NewConfig(config)

	endpoint.RecordMetrics(&HTTPEndpointMetrics{
		Request: true, NewConfig: true, NewConfigLatency: time.Since(t0)})
}

// DeadConfig adds the given tombstone to the configs managed by this endpoint.
func (endpoint *HTTPEndpoint) DeadConfig(tombstone *Tombstone) {
	t0 := time.Now()

	endpoint.Router.DeadConfig(tombstone)

	endpoint.RecordMetrics(&HTTPEndpointMetrics{
		Request: true, DeadConfig: true, DeadConfigLatency: time.Since(t0)})
}

// HTTPClientMetrics contains the result of an HTTP config event sent by an
// HTTPClient.
type HTTPClientMetrics struct {

	// Request indicates that an HTTP request made to a config endpoint.
	Request bool

	// NewConfig indicates that a new config was sent.
	NewConfig bool

	// DeadConfig indicates that a config tombstone was sent.
	DeadConfig bool

	// PushConfigs indicates that a set of configs and tombstones was sent.
	PushConfigs bool

	// PullConfigs indicates that a request was made to retrieve a set of
	// configs and tombstones.
	PullConfigs bool

	// Error indicates the outcome of the request.
	Error rest.ErrorType

	Latency time.Duration
}

// HTTPClient is used to handle config events over HTTP.
type HTTPClient struct {
	Component

	// URL indicates the URL to forward the event to.
	URL string

	// HTTPClient can optionally be used to set the http.Client object used for
	// communication.
	HTTPClient *http.Client

	initialize sync.Once

	RESTClient *rest.Client
}

// NewHTTPClient creates a new Client that can be used to
// communicate with an HTTP config endpoing located at the given URL.
func NewHTTPClient(rawURL string) (Client, error) {
	URL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return &HTTPClient{
		Component: Component{Name: "http-config-client-" + URL.Host},
		URL:       rawURL,
	}, nil
}

// Init initializes the object.
func (client *HTTPClient) Init() {
	client.initialize.Do(client.init)
}

func (client *HTTPClient) init() {
	if len(client.URL) == 0 {
		log.Panic("URL must be set for HTTPClient")
	}

	if _, err := url.Parse(client.URL); err != nil {
		log.Panicf("Invalid URL '%s': %s", client.URL, err.Error())
	}

	if client.HTTPClient == nil {
		client.HTTPClient = http.DefaultClient
	}

	client.RESTClient = &rest.Client{
		Client: client.HTTPClient,
		Root:   client.URL,
	}
}

// NewConfig sends a new config to the config endpoint.
func (client *HTTPClient) NewConfig(config *Config) {
	client.sendRequest("POST", config, nil, &HTTPClientMetrics{NewConfig: true})
}

// DeadConfig sends a config tombstone to the config endpoint.
func (client *HTTPClient) DeadConfig(tombstone *Tombstone) {
	client.sendRequest("DELETE", tombstone, nil, &HTTPClientMetrics{DeadConfig: true})
}

// PushConfigs sends the given set of configs and tombstones to the config
// endpoint.
func (client *HTTPClient) PushConfigs(configs *Configs) {
	client.sendRequest("PUT", configs, nil, &HTTPClientMetrics{PushConfigs: true})
}

// PullConfigs retrieves the set of configs and tombstones from the config
// endpoint.
func (client *HTTPClient) PullConfigs() *Configs {
	configs := &Configs{}
	client.sendRequest("GET", nil, configs, &HTTPClientMetrics{PullConfigs: true})
	return configs
}

func (client *HTTPClient) sendRequest(method string, input, output interface{}, metrics *HTTPClientMetrics) {
	client.Init()

	t0 := time.Now()
	metrics.Request = true

	resp := client.RESTClient.NewRequest(method).SetBody(input).Send()

	if err := resp.GetBody(output); err != nil {
		metrics.Error = err.Type
		client.Error(err)
	}

	metrics.Latency = time.Since(t0)
	client.RecordMetrics(metrics)
}

func init() {
	RegisterClient("http", NewHTTPClient)
}
