// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	magicAOF string = "e74e1902"
)

// ErrCorruptedAOF is the error returned by AOFConfigDB when a corrupted database
// is encountered.
var ErrCorruptedAOF = errors.New("CorruptedAOF")

// AOFConfigDB implements a configuration database as a append-only file. New
// entries are added to the database via the NewConfig and DeadConfig functions
// and the database can be read via the Load function.
//
// Note that corruptions are detected and reported but will not stop the loading
// process.
//
// \todo This struct is currently not go-routine safe.
type AOFConfigDB struct {
	Component

	// File indicates the file path where the AOF database should be
	// stored. Either File or AOF should be set prior to calling Init and can't
	// be changed afterwrads.
	File string

	// AOF indicates the file to use as the AOF database. Either File or AOF
	// must be set prior to calling Init and can't be changed afterwards.
	AOF *os.File

	initialized sync.Once

	configs   *Configs
	loadError error
}

// Init initializes the object.
func (db *AOFConfigDB) Init() {
	db.initialized.Do(db.init)
}

func (db *AOFConfigDB) init() {
	db.configs = &Configs{}

	if len(db.File) > 0 {
		var err error
		db.AOF, err = os.OpenFile(db.File, os.O_RDWR|os.O_CREATE, 0664)
		if err != nil {
			log.Panicf("unable to open aof file '%s': %s", db.File, err.Error())
		}
	}

	if db.AOF == nil {
		log.Panicf("AOF or File must be set for AOFConfigDB '%s'", db.Name)
	}

	db.load()
}

// Close closes the database flushing any pending writes. Returns an error if
// the AOF could not be closed properly.
func (db *AOFConfigDB) Close() error {
	if err := db.AOF.Sync(); err != nil {
		log.Panicf("failed to sync the AOF db: %s", err)
	}
	return db.AOF.Close()
}

func (db *AOFConfigDB) write(head byte, body []byte) (err error) {
	crc := crc32.ChecksumIEEE(body)
	_, err = db.AOF.WriteString(fmt.Sprintf("%s%08x%c%s\n", magicAOF, crc, head, body))
	return
}

func (db *AOFConfigDB) loadLine(line []byte) (err error) {
	magic := string(line[0:8])
	crcStr := string(line[8:16])
	head := line[16]
	body := line[17 : len(line)-1]

	if magic != magicAOF {
		err = fmt.Errorf("invalid aof magic: %s != %s", magic, magicAOF)
		return
	}

	crc, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		err = fmt.Errorf("unable to read crc: %s -> %s", crcStr, err)
		return
	}

	if bodyCRC := crc32.ChecksumIEEE(body); bodyCRC != uint32(crc) {
		err = fmt.Errorf("CRC mismatch: %s -> %c != %x", body, bodyCRC, crc)
		return
	}

	switch head {
	case 'n':
		err = db.loadNewConfig(body)
	case 't':
		err = db.loadDeadConfig(body)
	default:
		err = fmt.Errorf("unknown aof header: %d", head)
	}

	return
}

func (db *AOFConfigDB) load() {
	reader := bufio.NewReader(db.AOF)
	for {
		line, err := reader.ReadBytes('\n')
		if err == nil {
			err = db.loadLine(line)
		}

		if err == io.EOF {
			return
		}

		if err != nil {
			db.Error(err)
			db.loadError = ErrCorruptedAOF
		}
	}
}

// Load returns a copy of the database and a CorruptedAOF error if a corruption
// was detected while loading the database.
func (db *AOFConfigDB) Load() (*Configs, error) {
	db.Init()
	return db.configs.Copy(), db.loadError
}

func (db *AOFConfigDB) loadNewConfig(body []byte) (err error) {
	config := &Config{}
	if err = json.Unmarshal(body, config); err != nil {
		return
	}

	db.configs.NewConfig(config)
	return
}

// NewConfig adds the given config to the database.
func (db *AOFConfigDB) NewConfig(config *Config) {
	db.Init()

	if _, isNew := db.configs.NewConfig(config); !isNew {
		return
	}

	body, err := json.Marshal(config)
	if err != nil {
		db.Error(fmt.Errorf("unable to encode config %v: %s", *config, err))
		return
	}

	err = db.write('n', body)
	if err != nil {
		db.Error(fmt.Errorf("unable to write config %v: %s", *config, err))
		return
	}
}

func (db *AOFConfigDB) loadDeadConfig(body []byte) (err error) {
	tombstone := &Tombstone{}
	if err = json.Unmarshal(body, tombstone); err != nil {
		return
	}

	db.configs.DeadConfig(tombstone)
	return
}

// DeadConfig adds the given config tombstone to the database.
func (db *AOFConfigDB) DeadConfig(tombstone *Tombstone) {
	db.Init()

	if _, isNew := db.configs.DeadConfig(tombstone); !isNew {
		return
	}

	body, err := json.Marshal(tombstone)
	if err != nil {
		db.Error(fmt.Errorf("unable to encode tombstone %v: %s", *tombstone, err))
		return
	}

	err = db.write('t', body)
	if err != nil {
		db.Error(fmt.Errorf("unable to write tombstone %v: %s", *tombstone, err))
		return
	}
}
