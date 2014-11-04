# gosconf #

Distributed configuration system for golang. Usage examples are available in the
following test suites:

* [example_test.go](sconf/example_test.go): basic introduction to the sconf
  data-model.
* [example_router_test.go](sconf/example_router_test.go): demonstrates how the
  sconf.Router class is used to manage sconf configuration objects.
* [example_http_test.go](sconf/example_http_test.go): introduction to the HTTP
  components used to link multiple sconf-aware processes.

## Why another configuration library ##

sconf was built for the version of RTBkit where one of the key aspects of RTBkit
plugins is that they should be highly dynamic. The main use case behind this is
implementing a pacer as part of a bidder plugin which are usually built on top
of a control system. Keeping the feedback loop on the control system short will
allow bidders to react quicker and be more flexible.

Now if we're running a big multi-tenant system with over 1000 bidders and that
our control system gets updated every second then we can expect 1000
configuration updates per second to flow through the configuration system from
the bidders alone. This kind of write throughput, is where systems based on RAFT
or paxos will usually start falling apart. This is due to the high overhead
associated with reaching a distributed consensus for every write.

Additionally, our plugin architecture dictates that plugins should be passive
which mean that the RTBkit core needs to periodically poll the configuration
endpoint of the plugin. To avoid single-points of failure or performance
bottlenecks, we will need multiple instances of this component which introduces
a number of rare race-conditions which are dependent on the sequence of events
between the pollers. Even if we were to use a consensus-based system behind the
pollers, we would not be able to eliminate the race-conditions since the
configuration endpoints can only participate in the consensus protocol through a
proxy which is sensitive on timing.

In the end, the only advantage we could find for using a system like etcd or
Zookeeper would be the notification mechanism they provide which we can get from
queueing systems like NSQ or Kafka.
