# `dds` receiver

An [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) receiver
that reports the DDS system on this host. It covers the participants and the
host processes they are, their endpoints with topic, type and QoS, and how much
each writer writes. With `ros`, it also covers the ROS 2 nodes they host.

It takes part in the domain read-only and talks only to participants on this
host, by unicast. Nothing is sent to other machines, and it is not a ROS node:
`ros2 node list` does not show it. The traffic is read off the wire without
taking part (see [`traffic`](../../traffic)).

```yaml
receivers:
  dds:
    domain: 138                  # ROS_DOMAIN_ID
    interface: 192.168.10.0/24   # the network DDS runs on, as Cyclone's <NetworkInterface address=...>
    ros: true                    # ROS nodes and names (the default)
    snapshot_interval: 1m        # the whole graph, as state, this often (the default)
    proc_root: /proc             # the host's /proc (the default; pid_mode and network_mode host)
    traffic:
      enabled: true              # (the default) needs CAP_NET_RAW; without it, no traffic metrics
      collection_interval: 10s   # (the default)
      # interfaces: [eth0, lo]   # default: the DDS interface and loopback
```

Build it into a collector with [ocb](https://opentelemetry.io/docs/collector/custom-collector/):

```yaml
receivers:
  - gomod: github.com/lesomnus/dds/otelcol/ddsreceiver v0.0.0-...
```

Put it in a logs pipeline, a metrics pipeline, or both. One receiver ID is one
DDS participant either way.

## What is ROS's

Without `ros` the receiver reports plain DDS. `ros: true` adds:

- **Nodes**, from `ros_discovery_info`: `ros.node.*` events and state records,
  the `ros.nodes` metric, and `ros.node.name` on a writer's traffic.
- **ROS names** of endpoints: `ros.name`, `ros.kind`, `ros.type` (e.g. `/chatter`,
  `topic`, `std_msgs/msg/String` from `rt/chatter`,
  `std_msgs::msg::dds_::String_`).
- **`ros.enclave`**, from the participant's user data.
- Services are left out of `dds.endpoints.unmatched`. A server with no client
  is no fault.

## Logs

One record per event. The `service.name` is `dds`, and the event is in `event.name`:

| `event.name` | |
|---|---|
| `dds.participant.discovered` / `.updated` / `.lost` | A participant. `dds.lost.reason` is `disposed` (it said goodbye) or `lease_expired` (it went silent, as in a crash; severity WARN). |
| `dds.endpoint.discovered` / `.updated` / `.lost` | A writer or reader, with `dds.topic`, `dds.type` and `dds.qos.*`. |
| `ros.node.appeared` / `.updated` / `.disappeared` | A node (`ros.node.name`). `disappeared` comes with `removed` (destroyed while the process lives on) or `participant_lost`. |
| `ros.node.state` | Every `snapshot_interval`, one per node. This is the state that process metrics join with. |

Every record names its participant:

- `dds.participant.guid_prefix`
- `dds.participant.process_name`
- `dds.participant.pid`, in the participant's own PID namespace, as Cyclone DDS
  announces it

It also names the **host** process holding the participant's sockets:
`process.pid`, `process.executable.name`, `process.start_time` (Unix seconds) and
`process.cgroup`. The receiver finds that process through `/proc`: first the UDP
socket bound to the participant's port (`/proc/net/udp`), then the process
holding it (`/proc/<pid>/fd`).

## Metrics

Every `snapshot_interval`, as gauges:

| | |
|---|---|
| `dds.participants` | DDS participants on this host |
| `dds.endpoints` `{dds.endpoint.kind}` | writers and readers |
| `dds.endpoints.unmatched` `{dds.endpoint.kind}` | endpoints with no counterpart on this host, by topic and type: readers no writer feeds, writers no reader reads |
| `dds.qos.incompatible_pairs` | reader/writer pairs on a topic where the reader asks for more reliability or durability than the writer offers |
| `ros.nodes` | ROS nodes (with `ros`) |

Every `traffic.collection_interval`, for each writer of this host's
participants. These are cumulative counters since the writer was first seen:

| | |
|---|---|
| `dds.writer.samples` | samples written, counted by sequence number |
| `dds.writer.bytes` | their serialized bytes; samples whose size was not seen are counted at the average |
| `dds.writer.retransmit_requests` `{dds.request.kind}` | what readers asked for again: `sample` (ACKNACK), `fragment` (NACK_FRAG) |
| `dds.capture.drops` | packets the kernel dropped before the capture read them |

A writer's points carry `dds.endpoint.guid`, `dds.topic`, `dds.type`,
`dds.participant.guid_prefix` and the host `process.*`. With `ros` they also
carry `ros.name` and `ros.node.name`. The topic's rate is the rate of
`dds.writer.samples`.

Only what goes over the network is seen. A writer with no matched reader sends
nothing, so it gets no points. Shared-memory delivery is invisible.
