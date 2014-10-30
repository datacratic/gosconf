// Copyright (c) 2014 Datacratic. All rights reserved.

package sconf

import (
	"github.com/datacratic/goset"

	"sync/atomic"
	"testing"
	"time"
)

type TestRouterUtils struct{ TestConfigUtils }

func NewTestRouterUtils(t *testing.T) TestRouterUtils {
	return TestRouterUtils{TestConfigUtils{t}}
}

func (test TestRouterUtils) WaitForPropagation() {
	time.Sleep(50 * time.Millisecond)
}

type TestHandler struct {
	T TestRouterUtils

	newC  chan *Config
	deadC chan *Config
}

func (h *TestHandler) NewConfig(config *Config) {
	h.newC <- config
}

func (h *TestHandler) DeadConfig(tombstone *Tombstone) {
	h.deadC <- &Config{
		Type:    tombstone.Type,
		ID:      tombstone.ID,
		Version: tombstone.Version,
	}
}

func (h *TestHandler) expect(title string, c chan *Config, exp ...*Config) {
	var received []*Config
	tick := time.Tick(100 * time.Millisecond)

	for done := false; !done; {
		select {
		case <-tick:
			done = true
		case config := <-c:
			h.T.Logf("%s: config=%s", title, config.String())
			received = append(received, config)

			if len(received) == len(exp) {
				done = true
			}
		}
	}

	h.T.Diff(title, received, exp...)
}

func (h *TestHandler) ExpectNew(exp ...*Config) {
	h.expect("expect-new", h.newC, exp...)
}

func (h *TestHandler) ExpectDead(exp ...*Config) {
	h.expect("expect-dead", h.deadC, exp...)
}

func (router *Router) Expect(t TestRouterUtils, exp ...*Config) {
	t.Diff("expect", router.PullConfigs().ConfigArray(), exp...)
}

func (test TestRouterUtils) NewHandler() *TestHandler {
	return &TestHandler{
		T:     test,
		newC:  make(chan *Config, 100),
		deadC: make(chan *Config, 100),
	}
}

func (test TestRouterUtils) NewRouter(handlers ...Handler) *Router {
	router := &Router{}
	router.Handlers = append(router.Handlers, handlers...)
	return router
}

func (test TestRouterUtils) Run(title string, in *Router, out *TestHandler) {
	test.Logf("")
	test.Logf("[ %s ]-------------------------------------------------------", title)

	test.Logf("setup")
	in.NewConfig(test.Config("c1", 1))
	in.NewConfig(test.Config("c2", 1))
	in.NewConfig(test.Config("c3", 1))
	out.ExpectNew(
		test.Config("c1", 1),
		test.Config("c2", 1),
		test.Config("c3", 1))
	out.ExpectDead()
	in.Expect(test,
		test.Config("c1", 1),
		test.Config("c2", 1),
		test.Config("c3", 1))
	test.Logf("")

	test.Logf("add")
	in.NewConfig(test.Config("c1", 0))
	in.NewConfig(test.Config("c2", 1))
	in.NewConfig(test.Config("c3", 2))
	out.ExpectNew(
		test.Config("c3", 2))
	out.ExpectDead()
	in.Expect(test,
		test.Config("c1", 1),
		test.Config("c2", 1),
		test.Config("c3", 2))
	test.Logf("")

	test.Logf("remove")
	in.DeadConfig(test.Tomb("c1", 0))
	in.DeadConfig(test.Tomb("c2", 1))
	in.DeadConfig(test.Tomb("c3", 3))
	out.ExpectNew()
	out.ExpectDead(
		test.Config("c2", 1),
		test.Config("c3", 3))
	in.Expect(test,
		test.Config("c1", 1))
	test.Logf("")

	test.Logf("setup")
	in.DeadConfig(test.Tomb("c2", 2))
	in.DeadConfig(test.Tomb("c3", 2))
	in.DeadConfig(test.Tomb("c4", 0))
	out.ExpectNew()
	out.ExpectDead(
		test.Config("c2", 2),
		test.Config("c4", 0))
	in.Expect(test,
		test.Config("c1", 1))
	test.Logf("")

	test.Logf("t.Tombstones")
	in.NewConfig(test.Config("c2", 2))
	in.NewConfig(test.Config("c3", 3))
	in.NewConfig(test.Config("c4", 1))
	out.ExpectNew(
		test.Config("c4", 1))
	out.ExpectDead()
	in.Expect(test,
		test.Config("c1", 1),
		test.Config("c4", 1))
	test.Logf("")

	test.Logf("setup")
	in.NewConfig(test.Config("c5", 1))
	in.NewConfig(test.Config("c6", 1))
	in.NewConfig(test.Config("c7", 1))
	in.NewConfig(test.Config("c8", 1))
	out.ExpectNew(
		test.Config("c5", 1),
		test.Config("c6", 1),
		test.Config("c7", 1),
		test.Config("c8", 1))
	out.ExpectDead()
	in.Expect(test,
		test.Config("c1", 1),
		test.Config("c4", 1),
		test.Config("c5", 1),
		test.Config("c6", 1),
		test.Config("c7", 1),
		test.Config("c8", 1))
	test.Logf("")

	test.Logf("diff")
	configs := &Configs{
		Types: map[string]*TypeConfigs{
			TestConfigType: &TypeConfigs{
				Configs: map[string]*Config{
					"c1": test.Config("c1", 0),
					"c4": test.Config("c4", 1),
					"c5": test.Config("c5", 2),
					"c9": test.Config("c9", 1),
				},
				Tombstones: map[string]*Tombstone{
					"c6":  test.Tomb("c6", 0),
					"c7":  test.Tomb("c7", 1),
					"c8":  test.Tomb("c8", 2),
					"c10": test.Tomb("c10", 1),
				},
			},
		},
	}

	in.PushConfigs(configs)
	out.ExpectNew(
		test.Config("c5", 2),
		test.Config("c9", 1))
	out.ExpectDead(
		test.Config("c7", 1),
		test.Config("c8", 2),
		test.Config("c10", 1))

	test.WaitForPropagation()
	in.Expect(test,
		test.Config("c1", 1),
		test.Config("c4", 1),
		test.Config("c5", 2),
		test.Config("c6", 1),
		test.Config("c9", 1))
	test.Logf("")
}

func TestRouterHandlers(t *testing.T) {
	test := NewTestRouterUtils(t)

	handler := test.NewHandler()
	router := test.NewRouter(handler)

	test.Run("push-local", router, handler)
}

type TestConfigurable struct {
	T TestRouterUtils

	Name  string
	Types []string

	copies      int32
	newConfigC  chan string
	deadConfigC chan string
}

func (test TestRouterUtils) NewConfigurable(name string, types ...string) *TestConfigurable {
	return &TestConfigurable{T: test, Name: name, Types: types}
}

func (obj *TestConfigurable) RegisterState(router *Router) {
	obj.newConfigC = make(chan string, 100)
	obj.deadConfigC = make(chan string, 100)

	router.RegisterState(obj.Name, obj)
}

func (obj *TestConfigurable) AllowedConfigTypes() []string {
	return obj.Types
}

func (obj *TestConfigurable) Copy() Configurable {
	atomic.AddInt32(&obj.copies, 1)
	return obj
}

func (obj *TestConfigurable) NewConfig(newConfig *Config) (err error) {
	obj.newConfigC <- newConfig.ID
	return
}

func (obj *TestConfigurable) DeadConfig(oldConfig *Config) (err error) {
	obj.deadConfigC <- oldConfig.ID
	return
}

func (obj *TestConfigurable) expect(title string, c chan string, exp ...string) {
	a := set.NewString(exp...)
	b := set.NewString()

	done := false
	timeout := time.After(50 * time.Millisecond)

	for len(b) < len(exp) && !done {
		select {
		case ID := <-c:
			b.Put(ID)
		case <-timeout:
			done = true
		}
	}

	if diff := a.Difference(b); len(diff) > 0 {
		obj.T.Errorf("FAIL(%s.%s): missing configs %s", obj.Name, title, diff)
	}

	if diff := b.Difference(a); len(diff) > 0 {
		obj.T.Errorf("FAIL(%s.%s): extra configs %s", obj.Name, title, diff)
	}
}

func (obj *TestConfigurable) Expect(title string, configs, tombs []string, copy bool) {
	obj.expect(title+".new", obj.newConfigC, configs...)
	obj.expect(title+".dead", obj.deadConfigC, tombs...)

	if copies := atomic.LoadInt32(&obj.copies); copy && copies == 0 {
		obj.T.Errorf("FAIL(%s.%s.copy): no copies", title, obj.Name)
	}
}

func TestRouterConfigurable(t *testing.T) {
	test := NewTestRouterUtils(t)

	o0 := test.NewConfigurable("o0")
	o1 := test.NewConfigurable("o1", "t1")
	o2 := test.NewConfigurable("o2", "t1", "t2")
	o3 := test.NewConfigurable("o3", "t2", "t3")

	router := new(Router)

	o0.RegisterState(router)
	o1.RegisterState(router)
	o2.RegisterState(router)
	test.WaitForPropagation()

	router.NewConfig(test.ConfigT("t0", "c0", 1))
	router.NewConfig(test.ConfigT("t1", "c1", 1))
	router.NewConfig(test.ConfigT("t2", "c2", 1))
	router.NewConfig(test.ConfigT("t3", "c3", 1))

	o0.Expect("s1", []string{"c0", "c1", "c2", "c3"}, []string{}, true)
	o1.Expect("s1", []string{"c1"}, []string{}, true)
	o2.Expect("s1", []string{"c1", "c2"}, []string{}, true)

	router.NewConfig(test.ConfigT("t0", "c0", 0))
	router.NewConfig(test.ConfigT("t1", "c1", 2))
	router.DeadConfig(test.ConfigT("t2", "c2", 2).Tombstone())
	router.DeadConfig(test.ConfigT("t3", "c3", 0).Tombstone())

	o0.Expect("s2", []string{"c1"}, []string{"c1", "c2"}, true)
	o1.Expect("s2", []string{"c1"}, []string{"c1"}, true)
	o2.Expect("s2", []string{"c1"}, []string{"c1", "c2"}, true)

	o3.RegisterState(router)
	test.WaitForPropagation()

	o0.Expect("s3", []string{}, []string{}, true)
	o1.Expect("s3", []string{}, []string{}, true)
	o2.Expect("s3", []string{}, []string{}, true)
	o3.Expect("s3", []string{"c3"}, []string{}, false)

	router.DeadConfig(test.ConfigT("t0", "c0", 3).Tombstone())
	router.DeadConfig(test.ConfigT("t1", "c1", 0).Tombstone())
	router.NewConfig(test.ConfigT("t2", "c2", 0))
	router.NewConfig(test.ConfigT("t3", "c3", 3))

	o0.Expect("s4", []string{"c3"}, []string{"c0", "c3"}, true)
	o1.Expect("s4", []string{}, []string{}, true)
	o2.Expect("s4", []string{}, []string{}, true)
	o3.Expect("s4", []string{"c3"}, []string{"c3"}, true)
}
