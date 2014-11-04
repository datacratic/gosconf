// Copyright (c) 2014 Datacratic. All rights reserved.
//
// sconf.Router provides two notification mechanisms to propagate configuration
// events to handlers and states.

package sconf_test

import (
	"github.com/datacratic/gosconf/sconf"

	"fmt"
	"time"
)

// MyHandler represents a processor to be triggered whenever a configuration
// event occurs. This is useful to implement stateless processing such as
// persisting the configuration event.
type MyHandler struct{}

// AllowedConfigTypes is part of the sconf.Routable interface and is used to
// indicate the types of config event we're interesting in receiving.
func (handler *MyHandler) AllowedConfigTypes() []string {
	return []string{MyConfigType}
}

// NewConfig is part of the sconf.Handler interface and will be called whenever
// a new config is added.
func (handler *MyHandler) NewConfig(config *sconf.Config) {
	fmt.Println("NewConfig: ", config)
}

// DeadConfig is part of the sconf.Handler interface and will be called whenever
// a new tombstone is added.
func (handler *MyHandler) DeadConfig(tombstone *sconf.Tombstone) {
	fmt.Println("DeadConfig:", tombstone)
}

// In this example we'll use and sconf.Router to notify a MyHandler object
// everytime a new configuration is added or killed.
func Example_Router_Handler() {

	// Start by initializing our sconf.Router. Note that the various public
	// attributes of sconf.Router must be set before we start issuing
	// configuration events or querying the sconf.Router.
	router := new(sconf.Router)

	// While not strictly necessary, a router can be closed to release any
	// ressources associated with it.
	defer router.Close()

	// Add our handler to the list of handlers managed by our router. The router
	// will automatically look for the sconf.Routable interface and ensure that
	// the handler only receives the events it's interesting in receiving.
	router.Handlers = append(router.Handlers, new(MyHandler))

	// We can add and remove configs via the NewConfig and DeadConfig functions
	// which, unlike their sconf.Configs counterpart, does not return the result
	// of the operation. The sconf.Router object will forward the events to the
	// relevant handlers. In this example, that would be the MyHandler object we
	// added earlier.
	router.NewConfig(NewMyConfig("foo", 10).Wrap("id-foo", 1))
	router.NewConfig(NewMyConfig("bar", 10).Wrap("id-bar", 1))

	// sconf.Router processes events asynchrnously which means that we have to
	// take a little nap to ensure that all our events processed to ensure that
	// our examples make sense. In a regular program, this would not be
	// necessary.
	time.Sleep(1 * time.Millisecond)

	// Similar to the sconf.Configs container, adding old configs and tombstones
	// will not generate handler events...
	router.NewConfig(NewMyConfig("foo", 10).Wrap("id-foo", 0))
	router.DeadConfig(NewMyConfig("bar", 10).Wrap("id-bar", 0).Tombstone())

	// ... but we can still update the content so long as the version keep going
	// up.
	router.NewConfig(NewMyConfig("foo", 20).Wrap("id-foo", 2))

	time.Sleep(1 * time.Millisecond)

	// Output:
	// NewConfig:  {config type='my-type', id='id-foo', ver=1 }
	// NewConfig:  {config type='my-type', id='id-bar', ver=1 }
	// NewConfig:  {config type='my-type', id='id-foo', ver=2 }
}

// MyState represents a state that should be mutated whenever a new
// configuration event is received.
type MyState struct {
	Data map[string]int
}

// AllowedConfigTypes is part of the sconf.Routable interface and is used to
// indicate the types of config event we're interesting in receiving.
func (state *MyState) AllowedConfigTypes() []string {
	return []string{MyConfigType}
}

// Convenience function to create a initialize our state.
func NewMyState() *MyState {
	return &MyState{Data: make(map[string]int)}
}

// Copy is part of the sconf.Configurable interface and is used to copy the
// state prior to calling NewConfig and DeadConfig.
func (state *MyState) Copy() sconf.Configurable {
	newState := NewMyState()
	for name, count := range state.Data {
		newState.Data[name] = count
	}
	return newState
}

// NewConfig is part of the sconf.Configurable interface and is used to add the
// given configuration to the state.
//
// sconf.Router will only invoke this function on a copy returned by the
// sconf.Configurable.Copy() which is not shared with any other goroutine. The
// function can therefore safely mutate the object without synchronization.
func (state *MyState) NewConfig(newConfig *sconf.Config) error {
	myConfig := newConfig.Data.(*MyConfig)

	state.Data[myConfig.Name] = myConfig.Count

	return nil
}

// DeadConfig is part of the sconf.Configurable interface and is used to remove
// the given configuration to the state. Similarly to NewConfig, it can mutate
// the state without synchronization.
func (state *MyState) DeadConfig(oldConfig *sconf.Config) error {
	myConfig := oldConfig.Data.(*MyConfig)

	delete(state.Data, myConfig.Name)

	return nil
}

// In this example we'll use sconf.Router to register a MyState object which
// will mutate by generating configuration events.
func Example_Router_State() {

	// Start by creating a new router.
	router := new(sconf.Router)
	defer router.Close()

	// Next we'll create a new MyState object which we'll associate with the key
	// 'my-state' in our router. Note that calls to RegisterState are processed
	// asynchronously but the state will still be configured with all active
	// configurations.
	router.RegisterState("my-state", NewMyState())

	// Next we'll post a few configurations to mutate our state.
	router.NewConfig(NewMyConfig("foo", 1).Wrap("id-foo", 1))
	router.NewConfig(NewMyConfig("bar", 2).Wrap("id-bar", 1))

	// Since sconf.Router processes events asynchronously, we need to take a
	// little nap before we can inspect the state for our example. In a regular
	// program this sleep is obviously not required.
	time.Sleep(1 * time.Millisecond)
	fmt.Println()

	// Next we'll fetch a snapshot of our state which we can inspect to access
	// our modified state.
	//
	// The state is guaranteed by sconf.Router to not be modified and can
	// therefor be read without synchronization. Note that the state is
	// read-only and should not be modified.
	state0 := router.State().States["my-state"].(*MyState)
	fmt.Printf("state0[foo]: %d\n", state0.Data["foo"]) // 1
	fmt.Printf("state0[bar]: %d\n", state0.Data["bar"]) // 2

	// The object can be modified at will and only new configurations and
	// tombstones will be forwarded to the state.
	router.NewConfig(NewMyConfig("foo", 10).Wrap("id-foo", 2))
	router.DeadConfig(NewMyConfig("bar", 2).Wrap("id-bar", 1).Tombstone())

	// More sleeping for the sake of our test.
	time.Sleep(1 * time.Millisecond)
	fmt.Println()

	// As expected, the state was modified.
	state1 := router.State().States["my-state"].(*MyState)
	fmt.Printf("state1[foo]: %d\n", state1.Data["foo"]) // 10
	fmt.Printf("state1[bar]: %d\n", state1.Data["bar"]) // 0

	fmt.Println()

	// A bit more surprising is that the previous state was left untouched. This
	// is the result of the copy-on-write scheme employed by the sconf.Router to
	// ensure that states can be mutated wihout having to synchronize with read
	// operations.
	fmt.Printf("state0[foo]: %d\n", state0.Data["foo"])
	fmt.Printf("state0[bar]: %d\n", state0.Data["bar"])

	// Output:
	// state0[foo]: 1
	// state0[bar]: 2
	//
	// state1[foo]: 10
	// state1[bar]: 0
	//
	// state0[foo]: 1
	// state0[bar]: 2
}
