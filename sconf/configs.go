// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ConfigResult is a convenience struct used to return the result of a query for
// the config ID. Both Config and Tombstone can be nil in the case where the
// config ID was never seen.
type ConfigResult struct {

	// Config is non-nil if the config is active. At most Config or
	// Tombstone will be set.
	Config *Config `json:"live"`

	// Tombstone is non-nil if the config was killed. At most Config or
	// tombstone will be set.
	Tombstone *Tombstone `json:"dead"`
}

// Configs maintains a set of configs and tombstones indexed by types. All
// configs and tombstones held by this container are assumed to be immutable.
type Configs struct {

	// Types contains the set of configs and tombstones associated with a given
	// tombstones.
	Types map[string]*TypeConfigs
}

// Copy performs a deep copy of the object.
func (configs *Configs) Copy() (other *Configs) {
	other = &Configs{}
	other.Types = make(map[string]*TypeConfigs)

	if configs.Types == nil || len(configs.Types) == 0 {
		return
	}

	for typ, state := range configs.Types {
		other.Types[typ] = state.Copy()
	}

	return
}

func (configs *Configs) getState(typ string) *TypeConfigs {
	if configs.Types == nil {
		configs.Types = make(map[string]*TypeConfigs)
	}

	if state, ok := configs.Types[typ]; ok {
		return state
	}

	state := &TypeConfigs{}
	configs.Types[typ] = state
	return state
}

// Len returns the number of configs and tombstones for all types.
func (configs *Configs) Len() (size int) {
	for _, state := range configs.Types {
		size += state.Len()
	}

	return
}

// Get returns the config or tombstone associated with the given type and ID and
// a bool indicating whether the ID is present in the container for the given
// type.
func (configs *Configs) Get(typ, ID string) (ConfigResult, bool) {
	if state, ok := configs.Types[typ]; ok {
		return state.Get(ID)
	}
	return ConfigResult{}, false
}

// NewConfig adds the config and returns a boolean to indicate whether the
// config is new. A config is new if its version is strictly superior to the
// version of an existing config or tombstone of the same type and ID. If the
// config is new and it replaces an existing config then the old config being
// replaced is returned.
func (configs *Configs) NewConfig(config *Config) (oldConfig *Config, isNew bool) {
	return configs.getState(config.Type).NewConfig(config)
}

// DeadConfig adds the config tombstones and returns a boolean to indicate
// whether the config is new. A tombstone is new if its version is superior or
// equal to an existing config of the same type and ID or is strictly greater
// then the version of an existing tombstone. If the tombstone is new and it
// killed an live config then the config being replaced is returned.
func (configs *Configs) DeadConfig(tombstone *Tombstone) (oldConfig *Config, isNew bool) {
	return configs.getState(tombstone.Type).DeadConfig(tombstone)
}

// Merge invokes NewConfig on each config of other and invokes DeadConfig on
// each tombstone of other. This operation is commutative. Returns the list of
// configs that were added successfully by the calls to NewConfig and the list
// of configs that were replaced or killed by the calls to deadConfig. This
// function does not return the configs that were replaced by the calls to
// NewConfig.
func (configs *Configs) Merge(other *Configs) (newConfigs []*Config, deadConfigs []*Tombstone) {
	for typ, state := range other.Types {
		live, dead := configs.getState(typ).Merge(state)

		newConfigs = append(newConfigs, live...)
		deadConfigs = append(deadConfigs, dead...)
	}

	return
}

// Diff returns the configs and tombstones that would be added if invoked by any
// of the mutating functions. This does not modify the object and the Data field
// is never looked at in the Config objects.
func (configs *Configs) Diff(other *Configs) (newConfigs []*Config, deadConfigs []*Tombstone) {
	for typ, state := range other.Types {
		live, dead := configs.getState(typ).Diff(state)

		newConfigs = append(newConfigs, live...)
		deadConfigs = append(deadConfigs, dead...)
	}

	return
}

// ConfigArray returns an array of all the configs in this container.
func (configs *Configs) ConfigArray() (result []*Config) {
	for _, state := range configs.Types {
		for _, config := range state.Configs {
			result = append(result, config)
		}
	}
	return
}

// TombstoneArray returns an array of all the tombstones in this container.
func (configs *Configs) TombstoneArray() (result []*Tombstone) {
	for _, state := range configs.Types {
		for _, tombstone := range state.Tombstones {
			result = append(result, tombstone)
		}
	}
	return
}

// String returns a string representation of the container suitable for
// debugging.
func (configs *Configs) String() string {
	buffer := new(bytes.Buffer)
	buffer.WriteString("{ ")

	for typ, state := range configs.Types {
		buffer.WriteString(fmt.Sprintf("%s=%s ", typ, state.String()))
	}

	buffer.WriteString("}")
	return buffer.String()
}

// TypeConfigs container for configs and tombstone of a given type.
type TypeConfigs struct {

	// Configs contains a mapping of config ID to configs. An ID present in this
	// map will not be present in Tombstones.
	Configs map[string]*Config

	// Tombstones contains a mapping of config ID to tombstones. An ID present
	// in this map will not be present in Configs.
	Tombstones map[string]*Tombstone
}

// Copy performs a deep copy of the container. Note that config and tombstones
// are assumed to be immutable so they will not be copied.
func (configs *TypeConfigs) Copy() *TypeConfigs {
	result := &TypeConfigs{}

	if configs.Configs != nil && len(configs.Configs) > 0 {
		result.Configs = make(map[string]*Config)

		for ID, config := range configs.Configs {
			result.Configs[ID] = config
		}
	}

	if configs.Tombstones != nil && len(configs.Tombstones) > 0 {
		result.Tombstones = make(map[string]*Tombstone)

		for _, tombstone := range configs.Tombstones {
			result.Tombstones[tombstone.ID] = tombstone
		}
	}

	return result
}

// Len returns the number of configs and tombstones.
func (configs *TypeConfigs) Len() (size int) {
	if configs.Configs != nil {
		size += len(configs.Configs)
	}

	if configs.Tombstones != nil {
		size += len(configs.Tombstones)
	}

	return
}

// Get returns the config or tombstone associated with the given ID and a bool
// indicating whether the ID is present in the container.
func (configs *TypeConfigs) Get(ID string) (ConfigResult, bool) {
	if configs.Configs != nil {
		if config, ok := configs.Configs[ID]; ok {
			return ConfigResult{Config: config}, true
		}
	}

	if configs.Tombstones != nil {
		if tombstone, ok := configs.Tombstones[ID]; ok {
			return ConfigResult{Tombstone: tombstone}, true
		}
	}

	return ConfigResult{}, false
}

func (configs *TypeConfigs) isNewConfig(ID string, version uint64) bool {
	if configs.Configs != nil {
		if config, ok := configs.Configs[ID]; ok {
			return version > config.Version
		}
	}

	if configs.Tombstones != nil {
		if tombstone, ok := configs.Tombstones[ID]; ok {
			return version > tombstone.Version
		}
	}

	return true
}

func (configs *TypeConfigs) isNewTombstone(ID string, version uint64) bool {
	if configs.Configs != nil {
		if config, ok := configs.Configs[ID]; ok {
			return version >= config.Version
		}
	}

	if configs.Tombstones != nil {
		if tombstone, ok := configs.Tombstones[ID]; ok {
			return version > tombstone.Version
		}
	}

	return true
}

// NewConfig adds the config and returns a boolean to indicate whether the
// config is new. A config is new if its version is strictly superior to the
// version of an existing config or tombstone of the same ID. If the config is
// new and it replaces an existing config then the old config being replaced is
// returned.
func (configs *TypeConfigs) NewConfig(config *Config) (oldConfig *Config, isNew bool) {
	if isNew = configs.isNewConfig(config.ID, config.Version); !isNew {
		return
	}

	if configs.Configs == nil {
		configs.Configs = make(map[string]*Config)
	} else {
		oldConfig = configs.Configs[config.ID]
	}

	configs.Configs[config.ID] = config

	if configs.Tombstones != nil {
		delete(configs.Tombstones, config.ID)
	}

	return
}

// DeadConfig adds the config tombstones and returns a boolean to indicate
// whether the config is new. A tombstone is new if its version is superior or
// equal to an existing config of the same ID or is strictly greater then the
// version of an existing tombstone. If the tombstone is new and it killed an
// live config then the config being replaced is returned.
func (configs *TypeConfigs) DeadConfig(tombstone *Tombstone) (oldConfig *Config, isNew bool) {
	if isNew = configs.isNewTombstone(tombstone.ID, tombstone.Version); !isNew {
		return
	}

	if configs.Tombstones == nil {
		configs.Tombstones = make(map[string]*Tombstone)
	}

	configs.Tombstones[tombstone.ID] = tombstone

	if configs.Configs != nil {
		oldConfig = configs.Configs[tombstone.ID]
		delete(configs.Configs, tombstone.ID)
	}

	return
}

// Merge invokes NewConfig on each config of other and invokes DeadConfig on
// each tombstone of other. This operation is commutative. Returns the list of
// configs that were added successfully by the calls to NewConfig and the list
// of configs that were replaced or killed by the calls to deadConfig. This
// function does not return the configs that were replaced by the calls to
// NewConfig.
func (configs *TypeConfigs) Merge(other *TypeConfigs) (newConfigs []*Config, deadConfigs []*Tombstone) {

	for _, config := range other.Configs {
		if _, isNew := configs.NewConfig(config); isNew {
			newConfigs = append(newConfigs, config)
		}
	}

	for _, tombstone := range other.Tombstones {
		if _, isNew := configs.DeadConfig(tombstone); isNew {
			deadConfigs = append(deadConfigs, tombstone)
		}
	}

	return
}

// Diff returns the configs and tombstones that would be added if invoked by any
// of the mutating functions. This does not modify the object and the Data field
// is never looked at in the Config objects.
func (configs *TypeConfigs) Diff(other *TypeConfigs) (newConfigs []*Config, deadConfigs []*Tombstone) {
	for _, config := range other.Configs {
		if configs.isNewConfig(config.ID, config.Version) {
			newConfigs = append(newConfigs, config)
		}
	}

	for _, tombstone := range other.Tombstones {
		if configs.isNewTombstone(tombstone.ID, tombstone.Version) {
			deadConfigs = append(deadConfigs, tombstone)
		}
	}

	return
}

// MarshalJSON serializes the config object into a simpler json representation
// where the maps are replaced by arrays to avoid duplicating the IDs in the
// serialized object.
func (configs *TypeConfigs) MarshalJSON() ([]byte, error) {
	var configsJSON struct {
		Configs    []*Config    `json:"live,omitempty"`
		Tombstones []*Tombstone `json:"dead,omitempty"`
	}

	for _, config := range configs.Configs {
		configsJSON.Configs = append(configsJSON.Configs, config)
	}
	for _, tombstone := range configs.Tombstones {
		configsJSON.Tombstones = append(configsJSON.Tombstones, tombstone)
	}

	return json.Marshal(&configsJSON)
}

// UnmarshalJSON deserializes the config object from it's simpler json
// representation and builds the ID to object maps which are useful for
// in-memory accesses.
func (configs *TypeConfigs) UnmarshalJSON(body []byte) (err error) {
	var configsJSON struct {
		Configs    []*Config    `json:"live,omitempty"`
		Tombstones []*Tombstone `json:"dead,omitempty"`
	}

	if err = json.Unmarshal(body, &configsJSON); err != nil {
		return
	}

	configs.Configs = make(map[string]*Config)
	for _, config := range configsJSON.Configs {
		if _, ok := configs.Configs[config.ID]; ok {
			return fmt.Errorf("duplicate config: %v", *config)
		}

		configs.Configs[config.ID] = config
	}

	configs.Tombstones = make(map[string]*Tombstone)
	for _, tombstone := range configsJSON.Tombstones {
		if _, ok := configs.Tombstones[tombstone.ID]; ok {
			return fmt.Errorf("duplicate tombstone: %v", *tombstone)
		}

		configs.Tombstones[tombstone.ID] = tombstone
	}

	return
}

// String returns a string representation of the configs suitable for debugging.
func (configs *TypeConfigs) String() string {
	buffer := new(bytes.Buffer)
	buffer.WriteString("[ ")

	for _, config := range configs.Configs {
		buffer.WriteString(config.String())
		buffer.WriteString(" ")
	}

	for _, tombstone := range configs.Tombstones {
		buffer.WriteString(tombstone.String())
		buffer.WriteString(" ")
	}

	buffer.WriteString("]")
	return buffer.String()
}
