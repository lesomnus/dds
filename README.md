# dds

A DDS participant in pure Go (no cgo), speaking DDSI-RTPS over UDP.

It began as an observer: it watches a ROS 2 system for monitoring without
joining it as a node. It shows which participants are up and what process each
is, what every one of them publishes and subscribes to with which QoS, and which
ROS nodes they host. It now also **publishes and subscribes**. Payloads are the
user's, serialized, so it moves them without knowing their type. It is tested
against [Cyclone DDS](https://cyclonedds.io) 0.10.x as ROS 2 Jazzy runs it
(`rmw_cyclonedds_cpp`).

The DDS packages know nothing of ROS. Topic names like `rt/chatter`, type names
like `std_msgs::msg::dds_::String_` and CDR payloads are what a user hands them.
ROS's conventions live in [`ros2`](ros2), on top, and a test
([`internal/boundary`](internal/boundary)) keeps it that way.

| | |
|---|---|
| SPDP: participants discovered, updated, lost — disposed, or lease expired | ✓ |
| SEDP: every endpoint's topic, type and QoS | ✓ |
| `ros_discovery_info`: ROS nodes, several to a process, and their endpoints | ✓ |
| Reliable reading: ordering, gaps, heartbeats, fragments (NACK_FRAG), loss | ✓ |
| Traffic, read passively: each writer's sample rate, bytes, retransmit requests | ✓ (Linux) |
| Publishing: reliable and best-effort, keep-last and keep-all, volatile and transient-local, fragments | ✓ (keyless topics) |
| Subscribing to user topics | ✓ (keyless topics) |

## Using it

```go
ifi, addr, _ := transport.InterfaceByAddr(netip.MustParsePrefix("192.168.10.0/24"))

graph := ros2.NewGraph(func(e any) {
	switch e := e.(type) {
	case ros2.NodeAppeared:
		fmt.Println("+", e.Node.FullName(), "in", e.Node.Participant)
	case ros2.NodeDisappeared:
		fmt.Println("-", e.Node.FullName(), e.Reason) // removed, participant_lost
	}
})
p, _ := participant.New(participant.Config{Domain: 138, Interface: ifi, Addr: addr}, func(e discovery.Event) {
	graph.HandleDiscovery(e)
	switch e := e.(type) {
	case discovery.ParticipantDiscovered:
		fmt.Println("+", e.Participant.ProcessName(), e.Participant.PID())
	case discovery.ParticipantLost:
		fmt.Println("-", e.Participant.ProcessName(), e.Reason) // disposed, lease_expired
	case discovery.EndpointDiscovered:
		fmt.Println("  +", e.Endpoint.Kind, e.Endpoint.Topic, e.Endpoint.QoS.Reliability)
	}
})
_ = graph.Subscribe(p)
_ = p.Run(ctx)
```

`discovery.SPDPObserver` does the participant part alone, sending nothing at
all.

Publishing and subscribing:

```go
w, _ := p.Publish(participant.Publication{
	Topic: "rt/chatter", Type: "std_msgs::msg::dds_::String_",
	QoS:   participant.WriterQoS{Reliability: rtps.Reliable, Depth: 10},
})
// payload: serialized, encapsulation header first (00 01 00 00: CDR little endian)
_, _ = w.Write(payload)

_ = p.Subscribe(participant.Subscription{
	Topic: "rt/chatter", Type: "std_msgs::msg::dds_::String_",
	QoS:   participant.ReaderQoS{Reliability: rtps.Reliable, Depth: 10},
	Handle: func(writer rtps.GUID, s participant.Sample) { /* s.Payload */ },
})
```

A reader takes what a writer sends only once it has learned of the writer
itself. `Writer.Ready` names the readers known to have done so. A reliable
reader is known once it has acknowledged anything. Wait for it before the
first sample if that sample matters.

Cyclone DDS announces `__Hostname`, `__Pid` and `__ProcessName` with each
participant. The PID is in the sender's PID namespace: in a container it is not
the host's.

### ddsspy

```console
$ ddsspy -domain 138 -iface 192.168.10.0/24 -local          # listen only
$ ddsspy -domain 138 -iface 192.168.10.0/24 -sedp -ros      # endpoints and nodes too
+ 0110aca2198b5406904d051f cyclonedds host=work pid=39 proc=planner_cpp_node lease=10s ...
  + 0110aca2198b5406904d051f.00001703 writer rt/chatter [std_msgs::msg::dds_::String_] reliable volatile keep_last/7
  + node /motion_planner_node in 0110aca2198b5406904d051f: 9 readers, 12 writers
```

`-json` prints one event per line.

### Traffic

`traffic` reads what writers send rather than what they say: a packet socket
(`AF_PACKET`, needs `CAP_NET_RAW`) with a BPF filter that keeps only the first
256 bytes of each RTPS datagram. It does not need to take part, and it takes
nothing from the writers.

- **Samples** come from sequence numbers. A writer numbers its samples
  1, 2, 3, …, and every DATA, DATA_FRAG and HEARTBEAT carries one. The count is
  the highest seen minus the first seen, so a missed or duplicated packet
  changes nothing.
- **Bytes** come from the submessage lengths, which the header gives even when
  the packet is cut short. Samples whose size was not seen are counted at the
  average.
- **Retransmit requests** are the sequence numbers in ACKNACKs and the
  fragments in NACK_FRAGs, deduplicated by the readers' counts.

Capture both the DDS interface and the loopback one. Participants on one host
reach each other over loopback, at their own addresses.

Only what goes over the network is seen. A writer with no matched reader sends
nothing. Shared-memory delivery (iceoryx) is invisible.

```console
$ ddsspy -domain 138 -iface 192.168.10.0/24 -sedp -traffic 5s
  > 0110aca2198b5406904d051f.00001703 rt/chatter: 5 samples (1.00/s), 140 bytes, 0+0 asked again
```

One captured message costs about 130 ns.

## OpenTelemetry Collector

[`otelcol/ddsreceiver`](otelcol/ddsreceiver) is a collector receiver over all
of this, in its own module: participants, endpoints and nodes as events and
state, and each writer's traffic as metrics.

## How it stays out of the way

- **By default it talks only to this host.** In `ScopeHost`, the default, a
  `participant.Participant` announces itself only to the participants its
  filter keeps. By default those are the ones whose packets come from an
  address of this host, and it reaches them by unicast to their addresses on
  this host. Nothing is multicast. Every datagram leaves through one function
  that refuses any destination the scope does not allow. Another machine's
  participants are received (SPDP multicast reaches the socket) and ignored;
  they never learn of it. The E2E tests check this from the other machine with
  tcpdump.
- **`ScopeNetwork` reaches the DDS interface's network, and no further.** It
  announces itself to the SPDP multicast group (TTL 1) and to `Peers`. It sends
  only to addresses in the interface's subnets, to `Peers`, and to that group.
  It takes the first free participant index and its well-known port
  (7410 + 250·domain + 2·index), the range a firewall opens for a domain.
  It sends datagrams of Cyclone DDS's default sizes (14720 octets, fragments
  of 1344), optionally paced (`MaxSendRate`).
- **It writes only when asked.** An observer announces readers and no
  writers. Nothing publishes `ros_discovery_info`, so a participant is never a
  ROS node: `ros2 node list` does not show it.
- **It says goodbye.** On the way out it disposes itself, so peers forget it at
  once rather than when its lease (10 s) runs out.

## Packages

| | |
|---|---|
| [`rtps`](rtps) | messages, submessages, parameter lists; SPDP and SEDP data; writing messages |
| [`transport`](transport) | the `Transport` interface a participant sends and receives through; the RTPS port mapping; pacing (`Paced`) |
| [`transport/udp`](transport/udp) | the UDPv4 transport: one interface or several, ephemeral or well-known ports, SPDP multicast |
| [`transport/memtest`](transport/memtest) | a network in memory with loss and duplication, for testing participants without sockets |
| [`discovery`](discovery) | participants (SPDP) and endpoints (SEDP), with events |
| [`participant`](participant) | the participant: discovery on this host, reliable readers and writers, subscriptions and publications |
| [`ros2`](ros2) | `ros_discovery_info`, the node graph, ROS names of DDS topics and types |
| [`cdr`](cdr) | XCDR1 marshalling (from [lesomnus/cdr](https://github.com/lesomnus/cdr)) |
| [`cmd/ddsspy`](cmd/ddsspy) | prints a domain's participants, endpoints and nodes as they come and go |
| [`cmd/ddscat`](cmd/ddscat) | publishes to a topic, or prints what is published on one (`-scope network` for other hosts) |

## Limits

- Interoperability is tested with Cyclone DDS 0.10.x only. Its vendor-specific
  parameters (the version string) are read for it alone; everything else is
  the standard.
- UDP over IPv4, the default port mapping, no DDS Security, no shared memory.
  No XTypes type information is sent or required: Cyclone matches by type name.
- Endpoints match on topic, type, reliability and durability. Other QoS
  (deadline, lifespan, ownership, partitions, ...) is reported, not enforced.
- Topics are keyless: no keys, instances, dispose or unregister of samples.
- IPv4 only, over UDP. TCP would be another `transport.Transport`; none is
  written. `ScopeNetwork` stays on the link (multicast TTL 1) and reaches no
  routed network.
- Several interfaces are used together (`Config.Interfaces`, in order of
  preference). Each hears an announcement listing only its own locator,
  because a peer may send to every locator it is told of, reachable or not.
  A peer reachable on several interfaces is sent to on the first. Interfaces
  are fixed when the participant starts. A participant's own writers and readers do
  not meet: it does not discover itself.
- A writer does not wait. `Write` returns at once, and a full KEEP_ALL history
  is an error rather than a block. There is no flow control.
- Samples are limited to 1 MiB and 1024 ahead of the next in order.
- Its vendor ID is the specification's "unknown" (00.00): it has none assigned.

## Cost

On a host with 40 nodes and about 700 endpoints the observer's resident memory
is about 10 MiB, most of it the Go runtime; it keeps about 630 B per endpoint
and takes one in about 0.75 µs (`go test -bench . ./participant/`).

## Testing

```sh
go test ./...                                  # unit tests, on traffic captured from Cyclone
go test -run '^$' -fuzz '^FuzzParse$' ./rtps/  # also FuzzWriterProxy (participant), FuzzParseParticipantEntitiesInfo (ros2)
go test -tags e2e ./e2e/ -v                    # against Cyclone DDS in Docker
```

The E2E tests run ROS 2 processes and the observer in containers sharing a
network namespace (one machine), on Docker networks (one LAN), so the Docker
daemon need not be local. They compare what is observed with `ros2 topic info
-v` and `ros2 node list`, stop and kill processes, destroy nodes in a live one,
put thirty nodes in one process (fragmented samples, sent live and as
history to a reader that joins late), degrade the host's
loopback with `tc netem` (20 % loss, duplication, reordering, delay), and watch
from a second machine that nothing reaches it.
