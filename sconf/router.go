// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/goreports"

	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Handler represents an object that is interested in receiving
// notifications of new configs or of killed configs.
type Handler interface {
	NewConfig(*Config)
	DeadConfig(*Tombstone)
}

// Routable allows an object to indicate which type of configs it is interested
// in receiving.
type Routable interface {

	// AllowedConfigTypes returns the list of configuration types that should be
	// routed to the handler/object. Returning an empty list indicates that all
	// configuration types are allowed.
	AllowedConfigTypes() []string
}

// Configurable represents an object that can be mutated via configuration
// events through a copy-on-write mechanism.
type Configurable interface {

	// Copy returns a copy of the object which will be used by subsequent calls
	// to NewConfig and DeadConfig to mutate the object without data-races.
	Copy() Configurable

	// NewConfig mutates the object's state to include the new configuration.
	NewConfig(newConfig *Config) error

	// DeadConfig mutates the object's state to remove the old configuration.
	DeadConfig(oldConfig *Config) error
}

// ConfigurableHandler represents a handler that holds a Configurable
// state. This is used a convenience interface to chain calls to RegisterState.
type ConfigurableHandler interface {

	// RegisterState creates and registers the handler's state with the given
	// router.
	RegisterState(*Router)
}

// RouterState holds the current consistent state of the router.
type RouterState struct {

	// Configs contains the list of configs currently managed by the router.
	Configs *Configs

	// States contains the list of states currently managed by the router.
	States map[string]Configurable
}

type keyedConfigurable struct {
	Key    string
	Object Configurable
}

// DefaultRouterQueueSize represents the number of events that the router can
// buffer before forcing the batch processing of events.
const DefaultRouterQueueSize = 1 << 8

// Router routes configuration events to handlers and objects. If an object or a
// handler implements the Routable interface then only the configuration events
// for the desired types will be routed to that handler/object.
//
// Configuration events are first merged into the internal Configs object and
// only new events are forwarded to the handlers and objects.  All configuration
// event notifications are defered to the router's goroutine where all event
// processing takes place.
type Router struct {
	Component

	// Configs is used to initialize the list of configurations for this
	// router. States will be updated to include these configs but no handlers
	// will be invoked. Can be set during construction but can't be changed
	// afterwards. It is not updated as new configuration events are processed.
	Configs *Configs

	// States is used to initialize the list of configurable objects for this
	// router. Can be set during construction but can only be changed via the
	// RegisterState function. It is not updated as new configuration events are
	// processed.
	States map[string]Configurable

	// Handlers is the list of handlers that will be executed for each new
	// configuration events. Can be set during construction but can't be changed
	// afterwards.
	Handlers []Handler

	// QueueSize indicates the number of events that can be buffered before
	// forcing the batch processing of events.
	QueueSize int

	initialize sync.Once

	state unsafe.Pointer

	closeC           chan int
	newConfigC       chan *Config
	deadConfigC      chan *Tombstone
	pushConfigsC     chan *Configs
	registerStateC   chan keyedConfigurable
	unregisterStateC chan string
}

// Init initializes the router. Note that calling this function explicitly is
// optional.
func (router *Router) Init() {
	router.initialize.Do(router.init)
}

func (router *Router) init() {
	state := newRouterState(router.Configs, router.Handlers)
	if router.States != nil {
		for key, obj := range router.States {
			state.RegisterState(key, obj)
		}
	}
	router.state = unsafe.Pointer(state)

	queueSize := router.QueueSize
	if queueSize < 1 {
		queueSize = DefaultRouterQueueSize
	}

	router.closeC = make(chan int)

	// If we start falling behind, the bigger queues allows us to catch up by
	// batching multiple updates which avoids copies.
	router.newConfigC = make(chan *Config, queueSize)
	router.deadConfigC = make(chan *Tombstone, queueSize)
	router.pushConfigsC = make(chan *Configs, queueSize)
	router.registerStateC = make(chan keyedConfigurable, queueSize)
	router.unregisterStateC = make(chan string, queueSize)

	go func() {
		for {
			select {

			case msg := <-router.registerStateC:
				router.registerState(msg.Key, msg.Object)

			case key := <-router.unregisterStateC:
				router.unregisterState(key)

			case config := <-router.newConfigC:
				router.newConfig(config)

			case tombstone := <-router.deadConfigC:
				router.deadConfig(tombstone)

			case configs := <-router.pushConfigsC:
				router.pushConfigs(configs)

			case <-router.closeC:
				return

			}
		}
	}()
}

// Close terminates the router's goroutine.
func (router *Router) Close() {
	router.Init()
	router.closeC <- 1
}

// RegisterState registers the given Configurable object with the given key
// which must be unique. Once called, the object will start receiving
// configuration event. If the object implements the Routable interface then
// only the desired configuration types will be routed to the object.
func (router *Router) RegisterState(key string, state Configurable) {
	router.Init()
	assertf(len(key) > 0, "RegisterState's key parameter must not be nil in Router")

	router.registerStateC <- keyedConfigurable{key, state}
}

// UnregisterState removes the Configurable object associated with the given
// key.
func (router *Router) UnregisterState(key string) {
	router.Init()
	router.unregisterStateC <- key
}

// Register is a convenience function which checks whether the given handler
// implements the ConfigurableHandler interface and calls RegisterState if it
// does.
func (router *Router) Register(handler interface{}) {
	if h, ok := handler.(ConfigurableHandler); ok {
		h.RegisterState(router)
	}
}

// NewConfig pushes a given configuration into the router and generates the
// required events if the configuration is new.
func (router *Router) NewConfig(config *Config) {
	router.Init()
	router.newConfigC <- config
}

// DeadConfig pushes the given configuration tombstones into the router and
// generates the required events if the tombstone is new.
func (router *Router) DeadConfig(tombstone *Tombstone) {
	router.Init()
	router.deadConfigC <- tombstone
}

// PushConfigs adds a configs object to the router and generates the required
// events for all new configurations or tombstones.
func (router *Router) PushConfigs(configs *Configs) {
	router.Init()
	router.pushConfigsC <- configs
}

// PullConfigs returns the current list of active configs managed by the
// router. The returned object should not be modified.
func (router *Router) PullConfigs() *Configs {
	router.Init()
	return router.get().Configs
}

// State returns the current state of the router. The state is read atomically
// and is guaranteed to be consistent.
func (router *Router) State() RouterState {
	router.Init()
	state := router.get()
	return RouterState{state.Configs, state.KeyedStates}
}

func (router *Router) get() *routerState {
	return (*routerState)(atomic.LoadPointer(&router.state))
}

func (router *Router) set(state *routerState) {
	atomic.StorePointer(&router.state, unsafe.Pointer(state))
}

func (router *Router) registerState(key string, obj Configurable) {
	state := router.get().Copy()

	state.RegisterState(key, obj)
	router.processMore(state)

	router.set(state)
}

func (router *Router) unregisterState(key string) {
	state := router.get().Copy()

	state.UnregisterState(key)
	router.processMore(state)

	router.set(state)
}

func (router *Router) newConfig(config *Config) {
	state := router.get().Copy()

	if err := state.NewConfig(config); err != nil {
		router.error(err, config)
	}
	router.processMore(state)

	router.set(state)
}

func (router *Router) deadConfig(tombstone *Tombstone) {
	state := router.get().Copy()

	if err := state.DeadConfig(tombstone); err != nil {
		router.error(err, tombstone)
	}
	router.processMore(state)

	router.set(state)
}

func (router *Router) pushConfigs(configs *Configs) {
	state := router.get().Copy()

	state.PushConfigs(configs)
	router.processMore(state)

	router.set(state)
}

func (router *Router) processMore(state *routerState) {
	for i := 0; i < 16; i++ {
		select {

		case msg := <-router.registerStateC:
			state.RegisterState(msg.Key, msg.Object)

		case key := <-router.unregisterStateC:
			state.UnregisterState(key)

		case config := <-router.newConfigC:
			if err := state.NewConfig(config); err != nil {
				router.error(err, config)
			}

		case tombstone := <-router.deadConfigC:
			if err := state.DeadConfig(tombstone); err != nil {
				router.error(err, tombstone)
			}

		case configs := <-router.pushConfigsC:
			state.PushConfigs(configs)

		default:
			return

		}
	}
}

func (router *Router) error(err error, obj interface{}) {
	if data, err := json.Marshal(obj); err == nil {
		router.Error(err, report.Data{Name: "config", Blob: data})
	} else {
		log.Panic(err.Error())
	}
}

type routerState struct {
	Configs *Configs

	// Only keyed is visible to the outside world is the only one that should be
	// CoW-ed. Unfortunately, when we copy Keyed we also have to rebuild the
	// Typed and Untyped fields because they will no longer point to the current
	// Configurable.
	KeyedStates   map[string]Configurable
	typedStates   map[string][]Configurable
	untypedStates []Configurable

	// Read-only
	untypedHandlers []Handler
	typedHandlers   map[string][]Handler
}

func newRouterState(configs *Configs, handlers []Handler) *routerState {
	if configs == nil {
		configs = new(Configs)
	}

	state := &routerState{
		Configs:       configs,
		KeyedStates:   make(map[string]Configurable),
		typedStates:   make(map[string][]Configurable),
		typedHandlers: make(map[string][]Handler),
	}

	for _, handler := range handlers {

		var types []string
		if routable, ok := handler.(Routable); ok {
			types = routable.AllowedConfigTypes()
		}

		if len(types) == 0 {
			state.untypedHandlers = append(state.untypedHandlers, handler)
		} else {
			for _, typ := range types {
				state.typedHandlers[typ] = append(state.typedHandlers[typ], handler)
			}
		}
	}

	return state
}

func (state *routerState) Copy() *routerState {
	newState := &routerState{
		Configs: state.Configs.Copy(),

		KeyedStates: make(map[string]Configurable),
		typedStates: make(map[string][]Configurable),

		untypedHandlers: state.untypedHandlers,
		typedHandlers:   state.typedHandlers,
	}

	for key, state := range state.KeyedStates {
		newState.registerState(key, state.Copy(), false)
	}

	return newState
}

func (state *routerState) RegisterState(key string, obj Configurable) {
	state.registerState(key, obj, true)
}

func (state *routerState) registerState(key string, obj Configurable, notify bool) {
	if _, ok := state.KeyedStates[key]; ok {
		log.Panicf("state '%s' was already registered in Router", key)
	}
	state.KeyedStates[key] = obj

	var types []string
	if routable, ok := obj.(Routable); ok {
		types = routable.AllowedConfigTypes()
	}

	if len(types) == 0 {
		state.untypedStates = append(state.untypedStates, obj)
		if notify {
			for _, config := range state.Configs.ConfigArray() {
				obj.NewConfig(config)
			}
		}

	} else {
		for _, typ := range types {
			state.typedStates[typ] = append(state.typedStates[typ], obj)
			if configs, ok := state.Configs.Types[typ]; notify && ok {
				for _, config := range configs.Configs {
					obj.NewConfig(config)
				}
			}
		}
	}
}

func (state *routerState) UnregisterState(target string) {
	targetObj, ok := state.KeyedStates[target]
	assertf(ok, "key '%s' was not registered in Router", target)

	delete(state.KeyedStates, target)

	removeTarget := func(list []Configurable) []Configurable {
		for i, item := range list {
			if item == targetObj {
				return append(list[0:i], list[(i+1):len(list)]...)
			}
		}
		log.Panicf("unable to find object for key '%s'", target)
		return nil
	}

	var types []string
	if routable, ok := targetObj.(Routable); ok {
		types = routable.AllowedConfigTypes()
	}

	if len(types) == 0 {
		state.untypedStates = removeTarget(state.untypedStates)

	} else {
		for _, typ := range types {
			state.typedStates[typ] = removeTarget(state.typedStates[typ])
		}
	}
}

func (state *routerState) NewConfig(config *Config) (err error) {
	oldConfig, isNew := state.Configs.NewConfig(config)
	if !isNew {
		return
	}

	for _, handler := range state.untypedHandlers {
		handler.NewConfig(config)
	}

	if handlers, ok := state.typedHandlers[config.Type]; ok {
		for _, handler := range handlers {
			handler.NewConfig(config)
		}
	}

	var errors []error

	for _, obj := range state.untypedStates {
		if oldConfig != nil {
			errors = appendError(errors, obj.DeadConfig(oldConfig))
		}
		errors = appendError(errors, obj.NewConfig(config))
	}

	if typed, ok := state.typedStates[config.Type]; ok {
		for _, obj := range typed {
			if oldConfig != nil {
				errors = appendError(errors, obj.DeadConfig(oldConfig))
			}
			errors = appendError(errors, obj.NewConfig(config))
		}
	}

	return combineErrors(errors...)
}

func (state *routerState) DeadConfig(tombstone *Tombstone) (err error) {
	oldConfig, isNew := state.Configs.DeadConfig(tombstone)
	if !isNew {
		return
	}

	for _, handler := range state.untypedHandlers {
		handler.DeadConfig(tombstone)
	}

	if handlers, ok := state.typedHandlers[tombstone.Type]; ok {
		for _, handler := range handlers {
			handler.DeadConfig(tombstone)
		}
	}

	if oldConfig == nil {
		return
	}

	var errors []error

	for _, obj := range state.untypedStates {
		errors = appendError(errors, obj.DeadConfig(oldConfig))
	}

	if typed, ok := state.typedStates[tombstone.Type]; ok {
		for _, obj := range typed {
			errors = appendError(errors, obj.DeadConfig(oldConfig))
		}
	}

	return combineErrors(errors...)
}

func (state *routerState) PushConfigs(configs *Configs) (err error) {
	var errors []error

	for _, typed := range configs.Types {
		for _, config := range typed.Configs {
			errors = appendError(errors, state.NewConfig(config))
		}

		for _, tombstone := range typed.Tombstones {
			errors = appendError(errors, state.DeadConfig(tombstone))
		}
	}

	return combineErrors(errors...)
}
