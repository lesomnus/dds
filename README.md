# dds

A DDS participant in pure Go (no cgo), speaking DDSI-RTPS over UDP.

Its first aim is narrow: to **observe a ROS 2 system** without joining it as a
node — which participants are up and what process each is, what every one of
them publishes and subscribes to with which QoS, and which ROS nodes they host —
for monitoring. It is tested against [Cyclone DDS](https://cyclonedds.io)
0.10.x as ROS 2 Jazzy runs it (`rmw_cyclonedds_cpp`).

| | |
|---|---|
| SPDP: participants discovered, updated, lost — disposed, or lease expired | ✓ |
| SEDP: every endpoint's topic, type and QoS | ✓ |
| `ros_discovery_info`: ROS nodes, several to a process, and their endpoints | ✓ |
| Reliable reading: ordering, gaps, heartbeats, fragments (NACK_FRAG), loss | ✓ |
| Publishing user data | — not a goal |

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

## How it stays out of the way

- **It talks only to this host.** A `participant.Participant` announces
  itself to the participants its filter keeps (by default those whose packets
  come from an address of this host), by unicast to their addresses on this
  host. Nothing is multicast. Every datagram leaves through one function that
  refuses any destination that is not an address of this host. Another
  machine's participants are received — SPDP multicast reaches the socket — and
  ignored; they never learn of it. The E2E tests check this from the other
  machine with tcpdump.
- **It only reads.** It announces readers, never writers, and publishes no
  `ros_discovery_info`, so it is not a node: `ros2 node list` does not show it.
- **It says goodbye.** On the way out it disposes itself, so peers forget it at
  once rather than when its lease (10 s) runs out.

## Packages

| | |
|---|---|
| [`rtps`](rtps) | messages, submessages, parameter lists; SPDP and SEDP data; writing messages |
| [`transport`](transport) | the RTPS port mapping, multicast listening, interfaces |
| [`discovery`](discovery) | participants (SPDP) and endpoints (SEDP), with events |
| [`participant`](participant) | the participant: discovery on this host, reliable readers, subscriptions |
| [`ros2`](ros2) | `ros_discovery_info`, the node graph, ROS names of DDS topics and types |
| [`cdr`](cdr) | XCDR1 marshalling (from [lesomnus/cdr](https://github.com/lesomnus/cdr)) |
| [`cmd/ddsspy`](cmd/ddsspy) | prints a domain's participants, endpoints and nodes as they come and go |

## Limits

- Interoperability is tested with Cyclone DDS 0.10.x only. Its vendor-specific
  parameters (the version string) are read for it alone; everything else is
  the standard.
- UDP over IPv4, the default port mapping, no DDS Security, no shared memory.
  No XTypes type information is sent or required: Cyclone matches by type name.
- A subscription matches writers on topic, type, reliability and durability.
  Other QoS (deadline, ownership, partitions, ...) is reported, not enforced.
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
