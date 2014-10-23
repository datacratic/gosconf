// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"reflect"
	"testing"
)

const TestConfigType string = "test"

type TestConfig struct {
	Data string `json:"data"`
}

func (config *TestConfig) Wrap(ID string, version uint64) *Config {
	return &Config{
		ID:      ID,
		Version: version,
		Type:    TestConfigType,
		Data:    config,
	}
}

func init() {
	RegisterType(TestConfigType, reflect.TypeOf(TestConfig{}))
}

type TestConfigUtils struct{ *testing.T }

func (TestConfigUtils) ConfigT(typ, ID string, version uint64) *Config {
	return &Config{ID: ID, Version: version, Type: typ}
}

func (t TestConfigUtils) Config(ID string, version uint64) *Config {
	return t.ConfigT(TestConfigType, ID, version)
}

func (t TestConfigUtils) TombT(typ, ID string, version uint64) *Tombstone {
	return t.ConfigT(typ, ID, version).Tombstone()
}

func (t TestConfigUtils) Tomb(ID string, version uint64) *Tombstone {
	return t.Config(ID, version).Tombstone()
}

func (t TestConfigUtils) Diff(title string, configs []*Config, exp ...*Config) {
	a := make(map[string]map[string]*Config)
	b := make(map[string]map[string]*Config)

	add := func(m map[string]map[string]*Config, config *Config) {
		if _, ok := m[config.Type]; !ok {
			m[config.Type] = make(map[string]*Config)
		}

		if other, ok := m[config.Type][config.ID]; ok {
			t.Errorf("FAIL(%s): duplicate config %s <-> %s", title, config.String(), other.String())
		} else {
			m[config.Type][config.ID] = config
		}
	}

	for _, config := range configs {
		add(a, config)
	}
	for _, config := range exp {
		add(b, config)
	}

	diff := func(str string, a, b map[string]map[string]*Config) {
		for typ, aConfigs := range a {
			bConfigs, ok := b[typ]

			if !ok {
				for _, config := range aConfigs {
					t.Errorf("FAIL(%s): %s config %s", title, str, config.String())
				}

			} else {
				for ID, aConfig := range aConfigs {
					bConfig, ok := bConfigs[ID]
					if !ok {
						t.Errorf("FAIL(%s): %s config %s", title, str, aConfig.String())

					} else if bConfig.Version != aConfig.Version {
						t.Errorf("FAIL(%s); version mis-match %s != %s", title, aConfig.String(), bConfig.String())
					}

				}
			}
		}
	}

	diff("extra", a, b)
	diff("missing", b, a)
}

func (t TestConfigUtils) DiffConfigs(title string, state *TypeConfigs, exp ...*Config) {
	var configs []*Config
	for _, config := range state.Configs {
		configs = append(configs, config)
	}

	t.Diff(title, configs, exp...)
}

func (t TestConfigUtils) DiffTombs(title string, state *TypeConfigs, exp ...*Tombstone) {
	toConfig := func(tomb *Tombstone) *Config {
		return &Config{Type: tomb.Type, ID: tomb.ID, Version: tomb.Version}
	}

	var tombs []*Config
	for _, tomb := range state.Tombstones {
		tombs = append(tombs, toConfig(tomb))
	}

	var expTombs []*Config
	for _, tomb := range exp {
		expTombs = append(expTombs, toConfig(tomb))
	}

	t.Diff(title, tombs, expTombs...)
}

type TestConfigsUtils struct{ TestConfigUtils }

func NewTestConfigsUtils(t *testing.T) TestConfigsUtils {
	return TestConfigsUtils{TestConfigUtils{t}}
}

func (t TestConfigsUtils) Check(configs *Configs, config, oldConfig, expConfig *Config, isNew, expIsNew bool) {
	if isNew != expIsNew {
		t.Errorf("FAIL: add%v -> isNew=%t != exp=%t", *config, isNew, expIsNew)
	}

	if (expConfig == nil) != (oldConfig == nil) {
		t.Errorf("FAIL: add%v -> old=%t != exp=%t", *config, oldConfig == nil, expConfig == nil)
	}

	if expConfig == nil {
		return
	}

	if oldConfig.ID != expConfig.ID || oldConfig.Version != expConfig.Version {
		t.Errorf("FAIL: add%v -> old=%v != exp=%v", *config, *oldConfig, *expConfig)
	}
}

func (t TestConfigsUtils) Add(configs *Configs, config, expConfig *Config, expIsNew bool) {
	oldConfig, isNew := configs.NewConfig(config)
	t.Check(configs, config, oldConfig, expConfig, isNew, expIsNew)
}

func (t TestConfigsUtils) Rmv(configs *Configs, config, expConfig *Config, expIsNew bool) {
	oldConfig, isNew := configs.DeadConfig(config.Tombstone())
	t.Check(configs, config, oldConfig, expConfig, isNew, expIsNew)
}

func TestConfigs(test *testing.T) {
	t := NewTestConfigsUtils(test)

	c := &Configs{}

	t.Add(c, t.Config("c", 1), nil, true)
	t.Add(c, t.Config("c", 0), nil, false)
	t.Add(c, t.Config("c", 2), t.Config("c", 1), true)

	t.Rmv(c, t.Config("c", 1), nil, false)
	t.Rmv(c, t.Config("c", 2), t.Config("c", 2), true)
	t.Rmv(c, t.Config("c", 3), nil, true)

	t.Add(c, t.Config("c", 3), nil, false)
	t.Add(c, t.Config("c", 4), nil, true)

	t.Rmv(c, t.Config("c0", 0), nil, true)
}
