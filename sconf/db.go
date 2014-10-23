// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

// ConfigDB defines a simple persistance layer for config files. Entries are
// added via the NewConfig and DeadConfig functions and the database can be
// loaded via the Load function.
type ConfigDB interface {
	Handler
	Load() (*Configs, error)
	Close() error
}

// NullConfigDB defines a noop configuration database.
type NullConfigDB struct{}

// NewConfig does nothing.
func (db *NullConfigDB) NewConfig(_ *Config) {}

// DeadConfig does nothing.
func (db *NullConfigDB) DeadConfig(_ *Tombstone) {}

// Load returns an empty config object.
func (db *NullConfigDB) Load() (*Configs, error) { return &Configs{}, nil }

// Close does nothing.
func (db *NullConfigDB) Close() (err error) { return }
