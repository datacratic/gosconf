// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"bytes"
	"errors"
	"log"
)

func assertf(ok bool, format string, args ...interface{}) {
	if !ok {
		log.Panicf(format, args...)
	}
}

func appendError(result []error, list ...error) []error {
	for _, err := range list {
		if err != nil {
			result = append(result, err)
		}
	}
	return result
}

func combineErrors(list ...error) error {
	hasError := false
	var buffer bytes.Buffer

	for _, err := range list {
		if err != nil {
			hasError = true
			buffer.WriteString(err.Error())
			buffer.WriteString("\n")
		}
	}

	if !hasError {
		return nil
	}
	return errors.New(buffer.String())
}
