# gosconf #

Distributed configuration system for golang.


## Installation ##

You can download the code via the usual go utilities:

```
go get github.com/datacratic/gosconf/sconf
```

To build the code and run the test suite along with several static analysis tools,
use the provided Makefile:

```
make test
```

Note that the usual go utilities will work just fine but we require that all
commits pass the full suite of tests and static analysis tools.


## Examples ##

Examples are available in the following test suites:

* [**Data-Model**](sconf/example_test.go): introduction to the sconf data-model.
* [**Router**](sconf/example_router_test.go): introduction to the sconf.Router
  class which is used to manage sconf configuration objects.
* [**HTTP**](sconf/example_http_test.go): introduction to the HTTP components
  used to synchronize configurations across multiple sconf-aware processes.


## Why Another Configuration Library? ##

sconf was built for the new version of RTBkit where one of the key aspects of
RTBkit plugins is that they should be highly dynamic. The main use case behind
this is implementing a pacer as part of a bidder plugin which are usually built
on top of a control system. Keeping the feedback loop on the control system
short will allow bidders to react quicker and be more flexible.

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


## License ##

The source code is available under the Apache License. See the LICENSE file for
more details.
