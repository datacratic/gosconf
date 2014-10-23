// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"fmt"
	"log"
	"reflect"
)

var typeRegistry map[string]reflect.Type

// RegisterType associates the config type name with the given runtime
// reflected type. This is used to unmarshal config object based on the type
// name.
func RegisterType(name string, typ reflect.Type) {
	if typeRegistry == nil {
		typeRegistry = make(map[string]reflect.Type)
	}

	if typ.Kind() == reflect.Ptr {
		log.Panicf("config type for '%s' should not be a pointer", name)
	}

	if _, ok := typeRegistry[name]; ok {
		log.Panicf("duplicated config registration of type '%s'", name)
	}

	typeRegistry[name] = typ
}

// NewConfig creates a new config object for the given config type name
// registered via the RegisterType function.
func NewConfig(name string) (interface{}, error) {
	if typ, ok := typeRegistry[name]; ok {
		return reflect.New(typ).Interface(), nil
	}

	return nil, fmt.Errorf("unknown config type '%s'", name)
}
