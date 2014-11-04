// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf_test

import (
	"github.com/datacratic/gosconf/sconf"

	"fmt"
	"reflect"
)

// MyConfig represents a configuration blob which can be serialized to
// JSON. This is the payload type we will be working with in all the sconf
// examples.
type MyConfig struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func NewMyConfig(name string, count int) *MyConfig {
	return &MyConfig{Name: name, Count: count}
}

// We associate our config object with a type by registering it with sconf.
const MyConfigType string = "my-type"

func init() {
	sconf.RegisterType(MyConfigType, reflect.TypeOf(MyConfig{}))
}

// Convenience function to ID and version a MyConfig object.
func (config *MyConfig) Wrap(ID string, version uint64) *sconf.Config {
	return &sconf.Config{ID: ID, Version: version, Type: MyConfigType, Data: config}
}

// This example goes through the basic operations of adding sconf.Config and
// sconf.Tombstone objects to the sconf.Configs container.
func Example_Basics() {

	// Container that implements the CRDT data-model for sconf.
	configs := new(sconf.Configs)

	// Let's create our initial config object and associate it with the id
	// 'my-id' and version it at 0.
	config0 := NewMyConfig("bob", 1).Wrap("my-id", 0)

	// We can add it the new config to sconf.Configs container via the NewConfig
	// function which will return a boolean to indicate whether the config being
	// added is new or not. The oldConfig return value represents the config
	// that was overwritten by the new config, if any.
	oldConfig, isNew := configs.NewConfig(config0) // isNew = true
	fmt.Printf("NewConfig  %s -> %s, %t\n", config0, oldConfig, isNew)

	// Adding the same config twice will have no effect on our configs container
	// which is indicated by the false isNew return value.
	oldConfig, isNew = configs.NewConfig(config0) // isNew = false
	fmt.Printf("NewConfig  %s -> %s, %t\n", config0, oldConfig, isNew)

	// If we want to remove a config, we first have to generate a tombstone for
	// it. This can be done manually or using the convenience
	// sconf.Config.Tombstone() function which copies the header of the current
	// config to create the tombstone.
	tomb0 := config0.Tombstone()

	// The tombstone can then be added to our container via the DeadConfig
	// function which follows the same semantics as the NewConfig function. In
	// this case, our tombstone is replacing our live config config0 so it is
	// returned by the DeadConfig function.
	oldConfig, isNew = configs.DeadConfig(tomb0) // isNew = true, oldConfig = config0
	fmt.Printf("DeadConfig %s -> %s, %t\n", tomb0, oldConfig, isNew)

	// Adding a config after it was killed will also have no effect on our
	// configs container.
	oldConfig, isNew = configs.NewConfig(config0) // isNew = false
	fmt.Printf("NewConfig  %s -> %s, %t\n", config0, oldConfig, isNew)

	fmt.Println()

	// Now let's try updating our config. Note that sconf treats all configs as
	// immutable so to update our config, we'll have to create a new one from
	// scratch but with the version 1 this time.
	//
	//This is an important property to maintain since sconf is meant to be used
	//in heavily concurrent settings.
	config1 := NewMyConfig("bob", 2).Wrap(config0.ID, 1)

	// We again add the config via the NewConfig function and, since the version
	// number of config1 is greater then config0, the new config will be added
	// to our container which is indicated by isNew returning true.
	//
	// Note that config0 was previously killed which means that there are no
	// live configs for our id and oldConfig will therefor be nil.
	oldConfig, isNew = configs.NewConfig(config1) // isNew = true
	fmt.Printf("NewConfig  %s -> %s, %t\n", config1, oldConfig, isNew)

	// Adding a config with a lower version will, again, have no effect on our
	// container.
	oldConfig, isNew = configs.NewConfig(config0) // isNew = false
	fmt.Printf("NewConfig  %s -> %s, %t\n", config0, oldConfig, isNew)

	// Adding a tombstone with a lower version will also have no effect on our
	// container.
	oldConfig, isNew = configs.DeadConfig(tomb0) // isNew = false
	fmt.Printf("DeadConfig  %s -> %s, %t\n", tomb0, oldConfig, isNew)

	fmt.Println()

	// Let's create one last config to update our live config.
	config2 := NewMyConfig("bob", 3).Wrap(config0.ID, 2)

	// Updating can be done via NewConfig without having to kill the config
	// first. Since config1 is still live, it's returned by NewConfig as
	// oldConfig.
	oldConfig, isNew = configs.NewConfig(config2) // isNew = true, oldConfig = config1
	fmt.Printf("NewConfig  %s -> %s, %t\n", config2, oldConfig, isNew)

	// Output:
	// NewConfig  {config type='my-type', id='my-id', ver=0 } -> <nil>, true
	// NewConfig  {config type='my-type', id='my-id', ver=0 } -> <nil>, false
	// DeadConfig {tomb type='my-type', id='my-id', ver=0 } -> {config type='my-type', id='my-id', ver=0 }, true
	// NewConfig  {config type='my-type', id='my-id', ver=0 } -> <nil>, false
	//
	// NewConfig  {config type='my-type', id='my-id', ver=1 } -> <nil>, true
	// NewConfig  {config type='my-type', id='my-id', ver=0 } -> <nil>, false
	// DeadConfig  {tomb type='my-type', id='my-id', ver=0 } -> <nil>, false
	//
	// NewConfig  {config type='my-type', id='my-id', ver=2 } -> {config type='my-type', id='my-id', ver=1 }, true
}
