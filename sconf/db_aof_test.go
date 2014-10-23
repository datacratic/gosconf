// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

type ConfigPersistUtilsTest struct{ TestConfigUtils }

func NewConfigPersistUtilsTest(t *testing.T) ConfigPersistUtilsTest {
	return ConfigPersistUtilsTest{TestConfigUtils{t}}
}

func (t ConfigPersistUtilsTest) NewFile() string {
	return fmt.Sprintf("%s/rtbkit-config-test.%x.aof", os.TempDir(), rand.Uint32())
}

func (t ConfigPersistUtilsTest) Config(ID string, version uint64, data string) *Config {
	config := &TestConfig{Data: data}
	return config.Wrap(ID, version)
}

func (t ConfigPersistUtilsTest) Tomb(ID string, version uint64) *Tombstone {
	return t.Config(ID, version, "").Tombstone()
}

func (t ConfigPersistUtilsTest) Load(title string, aof *AOFConfigDB) *TypeConfigs {
	configs, err := aof.Load()
	if err != nil {
		t.Errorf("FAIL(%s): unable to load db: %s", title, err)
		return nil
	}

	state, ok := configs.Types[TestConfigType]
	if !ok {
		t.Errorf("FAIL(%s): missing config type: %s", title, TestConfigType)
		return nil
	}

	return state
}

func TestConfigPersistAOF(t *testing.T) {
	test := NewConfigPersistUtilsTest(t)

	file := test.NewFile()
	defer os.Remove(file)

	aof0 := &AOFConfigDB{File: file}

	aof0.NewConfig(test.Config("c0", 0, "d0"))
	aof0.NewConfig(test.Config("c1", 0, "d1"))
	aof0.NewConfig(test.Config("c2", 0, "d2"))
	aof0.DeadConfig(test.Tomb("c1", 1))
	aof0.DeadConfig(test.Tomb("c2", 0))
	aof0.NewConfig(test.Config("c2", 1, "d3"))
	aof0.Close()

	aof1 := &AOFConfigDB{File: file}
	test.DiffConfigs("aof1", test.Load("aof1", aof1),
		test.Config("c0", 0, "d0"),
		test.Config("c2", 1, "d3"))
	test.DiffTombs("aof1", test.Load("aof1", aof1),
		test.Tomb("c1", 1))

	aof1.NewConfig(test.Config("c1", 3, "d4"))
	aof1.DeadConfig(test.Tomb("c2", 2))
	aof1.NewConfig(test.Config("c3", 2, "d5"))
	aof1.Close()

	aof2 := &AOFConfigDB{File: file}
	test.DiffConfigs("aof2", test.Load("aof2", aof2),
		test.Config("c0", 0, "d0"),
		test.Config("c1", 3, "d4"),
		test.Config("c3", 2, "d5"))
	test.DiffTombs("aof2", test.Load("aof2", aof2),
		test.Tomb("c2", 2))
	aof2.Close()
}
