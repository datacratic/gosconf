// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

// MemoryConfigDB defines an in-memory config database which is not persisted.
type MemoryConfigDB struct {
	state *Configs
}

// NewConfig adds the given config to the database.
func (db *MemoryConfigDB) NewConfig(config *Config) {
	if db.state == nil {
		db.state = &Configs{}
	}
	db.state.NewConfig(config)
}

// DeadConfig adds the given config tombstone to the database.
func (db *MemoryConfigDB) DeadConfig(tombstone *Tombstone) {
	if db.state == nil {
		db.state = &Configs{}
	}
	db.state.DeadConfig(tombstone)
}

// Load returns a copy of the database.
func (db *MemoryConfigDB) Load() (state *Configs, err error) {
	if db.state == nil {
		db.state = &Configs{}
	}
	return db.state.Copy(), nil
}
