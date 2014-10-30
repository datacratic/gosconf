// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/gometrics"
	"github.com/datacratic/goreports"
)

// DefaultComponentMonitor defines a default metric monitor that outputs to stdout.
var DefaultComponentMonitor = &metric.Monitor{
	Name: "default",
}

// DefaultComponentReporter defines a default reporter that outputs to stdout.
var DefaultComponentReporter = &report.Reporter{
	Name: "default",
}

func init() {
	DefaultComponentMonitor.Start()
	DefaultComponentReporter.Start()
}

// Component defines a set of basic operations that are useful for most components.
type Component struct {
	// Name contains the friendly name of the component.
	Name string
	// Reporter is used to publish events and errors with or without associated binary data. Will use DefaultComponentReporter if nil.
	Reporter *report.Reporter
	// Monitor is used to record metrics for this component. Will use DefaultComponentMonitor if nil.
	Monitor *metric.Monitor
}

// RecordMetrics posts a set of metrics to the attached monitor service.
func (component *Component) RecordMetrics(data interface{}) {
	if component.Monitor != nil {
		component.Monitor.RecordMetrics(component.Name, data)
	} else {
		DefaultComponentMonitor.RecordMetrics(component.Name, data)
	}
}

// Log posts a report based on the supplied text and data to the attached reporter service.
func (component *Component) Log(text string, data ...report.Data) {
	if component.Reporter != nil {
		component.Reporter.Log(component.Name, text, data...)
	} else {
		DefaultComponentReporter.Log(component.Name, text, data...)
	}
}

// Error posts a report based on the supplied error and data to the attached reporter service.
func (component *Component) Error(err error, data ...report.Data) {
	if component.Reporter != nil {
		component.Reporter.Error(component.Name, err, data...)
	} else {
		DefaultComponentReporter.Error(component.Name, err, data...)
	}
}
