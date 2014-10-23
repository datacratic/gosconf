// Copyright (c) 2014 Datacratic. All rights reserved.
//
// The Config and Tombstone objects are assembled into a config set which
// implements a commutative merge function. This happens to implement one of the
// requirements of the CRDT data model that guarantees that our state will
// eventually converge in the presence of partitions. In other words, we are
// eventually consistent.
//
// The merge function is implemented by assigning an ordered version number to
// all configs such that when merging we will only keep the highest versioned
// config around. To delete a config, we replace a config by a tombstone which
// indicates that the config of type ID was killed at a given version. When
// merging, A tombstone at version X will replace a config at version Y if X >=
// Y and a config at version X will replace a tombstone at version Y if X > Y.
//
// Additionally, configs are seperated by types which is used for serialization
// and for routing.

package sconf

import (
	"encoding/json"
	"fmt"
)

// Tombstone indicates that a config of the same ID and type was killed at
// a given version. Tombstones are assumed to be immutable by the configuration
// system.
type Tombstone struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Version uint64 `json:"ver"`
}

// String returns a string representation of a tombstone suitable for debugging.
func (tombstone *Tombstone) String() string {
	return fmt.Sprintf("{tomb type='%s', id='%s', ver=%d }",
		tombstone.Type, tombstone.ID, tombstone.Version)
}

// Config associates configuration data to a configuration ID, type and
// version. Configs are assumed to be immutable by the configuration system.
type Config struct {
	Type    string      `json:"type"`
	ID      string      `json:"id"`
	Version uint64      `json:"ver"`
	Data    interface{} `json:"data,omitempty"`
}

// Tombstone returns a Tombstone that will kill the config object.
func (config *Config) Tombstone() *Tombstone {
	return &Tombstone{
		Type:    config.Type,
		ID:      config.ID,
		Version: config.Version,
	}
}

// UnmarshalJSON deserializes the given json blob as a Config object. Makes use
// of the config type registry to deserialize the config object and returns an
// error if the type was not registered with the config type registry.
func (config *Config) UnmarshalJSON(body []byte) (err error) {
	var configJSON struct {
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		Version uint64          `json:"ver"`
		Data    json.RawMessage `json:"data,omitempty"`
	}

	if err = json.Unmarshal(body, &configJSON); err != nil {
		return
	}

	config.Type = configJSON.Type
	config.ID = configJSON.ID
	config.Version = configJSON.Version
	if configJSON.Data == nil {
		return
	}

	if config.Data, err = NewConfig(configJSON.Type); err != nil {
		return
	}

	return json.Unmarshal(configJSON.Data, config.Data)
}

// String returns a string representation of the config suitable for debugging.
func (config *Config) String() string {
	return fmt.Sprintf("{config type='%s', id='%s', ver=%d }",
		config.Type, config.ID, config.Version)
}
