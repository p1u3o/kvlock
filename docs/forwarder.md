# Replication forwarder and peer addresses

Both features are **off by default**. They let kvrocks run on Nomad
dynamic ports while replicas and local clients use fixed addresses.

## Why

With dynamic ports, the leader's address changes on every reschedule.
kvlock already knows the current leader, its session and its epoch from
the Consul lock. The forwarder turns that into one fixed local address:

```
replica kvrocks --REPLICAOF 127.0.1.1 18379--> kvlock forwarder --> current leader host:dynport
```

Each replica runs `REPLICAOF <forward-addr>` once. After that, failover
only changes where the forwarder sends traffic. No new REPLICAOF is needed.

## Forwarder behaviour

- **Target.** The target is the Consul-confirmed lock holder's advertised
  address. The private address (e.g. NetBird) is used when one is
  published. The lock value carries `addr`/`private`/`port`, so a node that
  advertises `NOMAD_HOST_ADDR_<label>` is reachable on its dynamic port.
- **Route key** = `addr|session|epoch`. Any change to the leader, the
  session or the epoch changes the key, even at the same address (for
  example when the same node is re-elected). A key change closes every
  forwarded connection at once (drain 0). kvrocks replicas reconnect and
  psync through the forwarder to the new leader. psync and full-sync
  decisions depend only on the replication id and offsets, never on IPs.
- **No forwarding to an old or fenced leader.** The route is withdrawn,
  and existing connections are closed, in four cases:
  - there is no leader;
  - the lock is released;
  - Consul has not been confirmed within `max(2 × check-interval, 1s)`;
  - this node lost its session.
- **Hold.** A connection that arrives while no route exists waits up to
  `--forward-hold` (default 5s), polling every 25ms. If a leader becomes
  known in that time, the connection is forwarded. Otherwise it is closed
  and counted as refused.
- **Leader-local.** On the leader, the forwarder refuses all connections
  (reason `this node is the leader`). Forwarding there would loop the
  leader back to itself. The leader never runs REPLICAOF to the forwarder.
  It is a master, and when it is demoted kvlock fences it to the fence
  host, then points it at the forwarder once the new leader is known.
- **Fencing.** Fencing is unchanged. With no leader known, a demoted node
  replicates from `--fence-host:--fence-port`. Once a leader is known, the
  fence and follow targets are the forwarder.

## The kvrocks self-replication check

`IsTryingToReplicateItself` (cmd_server.cc ~1136–1152, kvrocks d56980a)
refuses `REPLICAOF host port` only when both of these are true:

- the target IP matches one of the replica's own listening IPs (or, when
  bound to 0.0.0.0/::, its interface addresses from getifaddrs); and
- the port equals the replica's own port.

`127.0.1.1` is not an interface address (`lo` reports 127.0.0.1). The
default forward port 18379 never equals a dynamic kvrocks port. So the
check never fires. kvlock enforces two rules on top of that:

- `--forward-addr` must be a loopback IP literal;
- its port must differ from `--kvrocks-port`. Startup fails otherwise.

**Do not bind kvrocks on 127.0.1.1** (or on the forwarder's address in
general). If you do, the check refuses REPLICAOF, and the two listeners
can also clash.

### Replica identity through the forwarder

Through the forwarder, the master sees every replica connecting from the
leader host's own address. The replica's announced port is still its own.
So with `--forward` kvlock sets these on its local kvrocks itself
(`CONFIG SET`), before the first `REPLICAOF`:

```
replica-announce-ip   <--replica-announce-ip, else NOMAD_HOST_IP_<label>, else the advertise address>
replica-announce-port <--replica-announce-port, else NOMAD_HOST_PORT_<label>, else the advertise/kvrocks port>
```

`CONFIG SET` is not persisted, so kvlock re-applies it whenever it has
seen kvrocks down (a restart), and re-checks it with `CONFIG GET` every
30s while replicating. `--replica-announce=false` turns this off, e.g. to
keep values from the kvrocks config file instead (the jobspec template
below still sets them, as an override and for the window before kvlock
starts).

They keep the master's `INFO replication` slave list accurate. They also
keep kvlock's "is this node one of the leader's replicas" check working.
That check compares the slave list to this node's advertised address.
Without the announce settings it never matches, and kvlock falls back to
its other retry paths.

## Peer addresses (optional, N instances)

`--peer-expose` gives every instance a stable local address derived from
its Nomad alloc index: `<peer-base with last octet = index>:<peer-port>`.
With the defaults that is `127.0.2.<index>:18379`. A three-node cluster
gets:

| alloc index | peer address |
|---|---|
| 0 | 127.0.2.0:18379 |
| 1 | 127.0.2.1:18379 |
| 2 | 127.0.2.2:18379 |

- Each node publishes `{"node","index","addr","private","port"}` as its
  Consul member key value (`kvrocks/<cluster>/members/<node>`). The key is
  held by the node's session, so it disappears when the node dies. Without
  `--peer-expose` the value stays the bare node name, the original format.
  kvlock reads both formats.
- Every node runs one fail-closed listener per live alloc index. Its own
  index routes to its local kvrocks. The route key is node plus address.
  When an alloc is replaced, the address stays the same, but the new key
  drops old connections and new ones go to the replacement.
- Two live members that claim the same index are both refused. kvlock
  does not guess.
- Uses:
  - `kvrocks-leader status` shows the peer table.
  - Before promotion, the leader-offset sample (the lag gate) dials the
    leader through its peer address. It falls back to the real address
    when the index is unknown.
- The alloc index comes from `NOMAD_ALLOC_INDEX`, not from a hash.
  - It is unique within a job and stable across replacements, so there
    are no collisions.
  - The range 0–255 is enforced.
  - Linux routes all of 127.0.0.0/8 to `lo`, so no interface setup is
    needed. macOS only has 127.0.0.1, which matters for local testing only.
- **Rolling upgrade.** Enable `--peer-expose` only after every node runs
  this version. Older versions compare member values to node names, so
  they would read a JSON value as some other live node.

Clients can keep using the leader proxy (`--proxy-write-addr`) or the
forwarder address on every host to find the master.

## Configuration

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--forward` | `FORWARD_ENABLED` | `false` | Run the forwarder and point replicas at it |
| `--forward-addr` | `FORWARD_ADDR` | `127.0.1.1:18379` | Listen address. Must be a loopback IP literal, and its port must differ from the kvrocks port |
| `--forward-hold` | `FORWARD_HOLD` | `5s` | How long a new connection waits for a known leader. `0` refuses at once |
| `--advertise-nomad-label` | `ADVERTISE_NOMAD_LABEL` | unset | Take the advertised addr:port from `$NOMAD_HOST_ADDR_<label>`. An explicit `--advertise-addr`/`--advertise-port` wins |
| `--replica-announce` | `REPLICA_ANNOUNCE` | `true` | With `--forward`, `CONFIG SET replica-announce-ip/-port` on the local kvrocks |
| `--replica-announce-ip` | `REPLICA_ANNOUNCE_IP` | `NOMAD_HOST_IP_<label>`, else advertise addr | Announced IP (an IP literal) |
| `--replica-announce-port` | `REPLICA_ANNOUNCE_PORT` | `NOMAD_HOST_PORT_<label>`, else advertise port | Announced port |
| `--peer-expose` | `PEER_EXPOSE` | `false` | Publish the alloc index and expose peers at stable addresses |
| `--peer-base` | `PEER_BASE` | `127.0.2.0` | IPv4 loopback /24; the last octet is replaced by the index |
| `--peer-port` | `PEER_PORT` | `18379` | Port for peer addresses. Must differ from the kvrocks port |
| `--alloc-index` | `ALLOC_INDEX`, else `NOMAD_ALLOC_INDEX` | `-1` | This node's index (0–255). Required with `--peer-expose` |

## Status and metrics

`/status` gains `forwarder` (listen, target, reason, active, total, bytes
up/down, resets, dropped, refused) and `peers`. `kvrocks-leader status`
prints both under the node table.

Prometheus metrics. `listener` is `forward` or `peer-<index>`:

- `kvlock_proxy_active_connections{listener}`
- `kvlock_proxy_connections_total{listener}`
- `kvlock_proxy_bytes_total{listener,direction="up"|"down"}`
- `kvlock_proxy_route_resets_total{listener}`
- `kvlock_proxy_dropped_connections_total{listener}`
- `kvlock_proxy_refused_connections_total{listener}`

## Nomad jobspec (dynamic ports)

Host networking works because the forwarder port (18379) never equals the
dynamic kvrocks port. A bridge network is not required.

```hcl
group "kv" {
  count = 3   # any N >= 2

  network {
    port "kvrocks" {}            # dynamic, from Nomad's range
    port "http" { static = 8080 }
  }

  task "kvrocks" {
    driver = "docker"
    config {
      image        = var.kvrocks_image
      network_mode = "host"
      ports        = ["kvrocks"]
    }
    template {
      destination = "local/kvrocks.conf"
      data = <<EOT
bind 0.0.0.0
port {{ env "NOMAD_PORT_kvrocks" }}
# Accurate slave list through the forwarder. kvlock also sets these
# itself (--replica-announce, default on); kept here as an override:
replica-announce-ip {{ env "NOMAD_HOST_IP_kvrocks" }}
replica-announce-port {{ env "NOMAD_HOST_PORT_kvrocks" }}
EOT
    }
  }

  task "kvlock" {
    driver = "docker"
    config {
      image        = var.sidecar_image
      network_mode = "host"
      ports        = ["http"]
      args = [
        "--kvrocks-port", "${NOMAD_PORT_kvrocks}",
        "--advertise-nomad-label", "kvrocks",
        "--forward",
        "--forward-addr", "127.0.1.1:18379",
        "--peer-expose",          # optional
      ]
    }
    # NOMAD_ALLOC_INDEX is set by Nomad automatically.
  }
}
```

kvrocks must not have `127.0.1.1` in `bind`.
