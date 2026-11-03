# dds

A DDS participant in pure Go (no cgo), speaking DDSI-RTPS over UDP.

Its first aim is narrow: to **observe a ROS 2 system** without joining it as a
node — which participants are up and what process each is, what they publish
and subscribe to, and which ROS nodes they host — for monitoring. It is tested
against [Cyclone DDS](https://cyclonedds.io) 0.10.x, as ROS 2 Jazzy runs it
(`rmw_cyclonedds_cpp`).

## Status

| | |
|---|---|
| SPDP, receive only: participants discovered, updated, lost (disposed or lease expired) | done |
| SEDP: topics, types, QoS of every endpoint | planned |
| `ros_discovery_info`: ROS nodes and the participants that host them | planned |

See [#1](https://github.com/lesomnus/dds/issues/1) for the plan.

## Packages

| | |
|---|---|
| [`rtps`](rtps) | messages, submessages, parameter lists, SPDP participant data |
| [`transport`](transport) | the RTPS port mapping, multicast listening |
| [`discovery`](discovery) | participants from SPDP, with events |
| [`cdr`](cdr) | XCDR1 marshalling (from [lesomnus/cdr](https://github.com/lesomnus/cdr)) |
| [`cmd/ddsspy`](cmd/ddsspy) | prints the participants of a domain as they come and go |

```console
$ ddsspy -domain 138 -iface 192.168.10.0/24 -local
+ 0110aca2198b5406904d051f cyclonedds host=work pid=39 proc=planner_cpp_node lease=10s ...
- 0110aca2198b5406904d051f cyclonedds host=work pid=39 proc=planner_cpp_node ... (disposed)
```

Cyclone DDS announces `__Hostname`, `__Pid` and `__ProcessName` with each
participant. The PID is in the sender's PID namespace: in a container it is not
the host's.

## Principles

- **It decides whom it talks to.** Nothing is sent to a participant a filter
  rejects, and nothing is multicast by default, so observing one machine does
  not announce anything to the others.
- **It only reads.** No user data is published, and nothing that would make it
  a node in the ROS graph.

## Testing

```sh
go test ./...                              # unit tests, with packets captured from Cyclone
go test -run '^$' -fuzz FuzzParse ./rtps/  # the decoders against anything
go test -tags e2e ./e2e/ -v                # against Cyclone DDS in Docker
```

The E2E tests run ROS 2 processes and the observer in containers sharing a
network namespace (one machine), on Docker networks (one LAN), so the Docker
daemon need not be local.
