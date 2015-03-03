// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/goblueprint/blueprint"
	"github.com/datacratic/gometer/meter"
	"github.com/datacratic/gorest/rest"

	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// DefaultHTTPEndpointPath contains the default path prefix used by
// HTTPEndpoint.
var DefaultHTTPEndpointPath = "/v1/configs"

type httpMetrics struct {
	Requests *meter.Counter
	Errors   *meter.Counter
	Latency  *meter.Histogram
}

// HTTPEndpoint is an HTTP endpoint used to process various config related
// events. The endpoint uses a Router to access the list of existing
// configs and to push new configs or tombstones.
type HTTPEndpoint struct {
	Name string

	// PathPrefix is the HTTP path used to reach this endpoint. Defaults to
	// DefaultHTTPEndpointPath.
	PathPrefix string

	// Router will be used to process config events received by this endpoint.
	Router *Router

	initialize sync.Once

	metrics struct {
		GetConfig   httpMetrics
		ListConfigs httpMetrics
		PullConfigs httpMetrics
		PushConfigs httpMetrics
		NewConfig   httpMetrics
		DeadConfig  httpMetrics
	}
}

// RESTRoutes returns the REST routes for the config endpoint.
func (endpoint *HTTPEndpoint) RESTRoutes() rest.Routes {
	path := endpoint.PathPrefix
	if len(path) == 0 {
		path = DefaultHTTPEndpointPath
	}

	return rest.Routes{
		rest.NewRoute(path, "GET", endpoint.PullConfigs),
		rest.NewRoute(path, "PUT", endpoint.PushConfigs),
		rest.NewRoute(path, "POST", endpoint.NewConfig),
		rest.NewRoute(path, "DELETE", endpoint.DeadConfig),

		rest.NewRoute(path+"/list", "GET", endpoint.ListConfigs),
		rest.NewRoute(path+"/:type/:id", "GET", endpoint.GetConfig),
	}
}

func (endpoint *HTTPEndpoint) Init() {
	endpoint.initialize.Do(endpoint.init)
}

func (endpoint *HTTPEndpoint) init() {
	if endpoint.Name == "" {
		endpoint.Name = "configEndpoint"
	}

	meter.Load(&endpoint.metrics, endpoint.Name)
}

// GetConfig returns the config associated by the given ID and type managed by
// this endpoint. Returns a 404 REST error if the config doesn't exist.
func (endpoint *HTTPEndpoint) GetConfig(typ, ID string) (result ConfigResult, err error) {
	t0 := time.Now()
	endpoint.metrics.GetConfig.Requests.Hit()

	var ok bool
	result, ok = endpoint.Router.PullConfigs().Get(typ, ID)

	if !ok {
		endpoint.metrics.GetConfig.Errors.Hit()
		err = fmt.Errorf("ID '%s' doesn't exist for type '%s'", ID, typ)
		err = &rest.CodedError{Code: http.StatusNotFound, Sub: err}
	}

	endpoint.metrics.GetConfig.Latency.RecordSince(t0)
	return
}

// ListConfigs returns a mappiong of config IDs to config version managed by
// this endpoint.
func (endpoint *HTTPEndpoint) ListConfigs() ConfigList {
	t0 := time.Now()
	endpoint.metrics.ListConfigs.Requests.Hit()

	list := endpoint.Router.PullConfigs().List()

	endpoint.metrics.ListConfigs.Latency.RecordSince(t0)
	return list
}

// PullConfigs returns all the configs and tombstones managed by this endpoint.
func (endpoint *HTTPEndpoint) PullConfigs() *Configs {
	t0 := time.Now()
	endpoint.metrics.PullConfigs.Requests.Hit()

	configs := endpoint.Router.PullConfigs()

	endpoint.metrics.PullConfigs.Latency.RecordSince(t0)
	return configs
}

// PushConfigs merges the given configs with the configs managed by the endpoint.
func (endpoint *HTTPEndpoint) PushConfigs(configs *Configs) {
	t0 := time.Now()
	endpoint.metrics.PushConfigs.Requests.Hit()

	endpoint.Router.PushConfigs(configs)

	endpoint.metrics.PushConfigs.Latency.RecordSince(t0)
}

// NewConfig adds the given config to the configs managed by this endpoint.
func (endpoint *HTTPEndpoint) NewConfig(config *Config) {
	t0 := time.Now()
	endpoint.metrics.NewConfig.Requests.Hit()

	endpoint.Router.NewConfig(config)

	endpoint.metrics.NewConfig.Latency.RecordSince(t0)
}

// DeadConfig adds the given tombstone to the configs managed by this endpoint.
func (endpoint *HTTPEndpoint) DeadConfig(tombstone *Tombstone) {
	t0 := time.Now()
	endpoint.metrics.DeadConfig.Requests.Hit()

	endpoint.Router.DeadConfig(tombstone)

	endpoint.metrics.DeadConfig.Latency.RecordSince(t0)
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
	blueprint.Register(HTTPEndpoint{})
}
