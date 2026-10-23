# kvrocks-leader

Nomad sidecar that elects one read-write kvrocks master for a two-node
pair (kv01, kv02) using a Consul session lock. Replaces Redis Sentinel.

## How it works

Each sidecar runs next to its local kvrocks and loops every
`--check-interval` (default 3s):

1. **Health**: `PING` + `INFO replication` against local kvrocks (timeout
   `--kvrocks-timeout`, default 2s). After `--unhealthy-threshold`
   (default 3) consecutive failures the sidecar stops renewing its Consul
   session and releases the lock.
2. **Session**: Consul session with **no node health checks** (only the
   TTL), TTL `--session-ttl` (default 10s), `behavior=release`,
   `LockDelay` `--lock-delay` (default 1s). Renewed at TTL/3 while kvrocks
   is healthy. A flapping Consul agent/serf check therefore cannot destroy
   the session; only TTL expiry, explicit release, or invalidation can.
3. **Election**: atomic acquire of `kvrocks/<cluster>/leader` with JSON
   `{node, addr, private_addr, port, session, since, epoch}`. **Leadership
   is decided by the session that holds the key**, never by the node name
   in the value: a key whose value names us but which is held by no session
   (or another session) does not make us leader. `epoch` increments on
   every new leader.
4. **Leader**: `REPLICAOF NO ONE` (fallback `SLAVEOF NO ONE`), best-effort
   writable + `use-rsid-psync yes`, registers Consul service
   `--service-name` tagged `--master-tag` with a TTL check the sidecar
   refreshes.
5. **Follower**: blocking-query watch on the leader key, registers the
   same service tagged `--replica-tag`, issues `REPLICAOF host port` only
   when not already following that host:port (the leader's private address
   when it published one). Each tick it also samples the leader's
   `master_repl_offset` for the lag gate (below).
6. **Demote + fence**: on session invalidation, losing the lock, sustained
   unhealthiness, or Consul unreachable for longer than the session TTL
   since the last successful renew, the leader fails its TTL check,
   deregisters the master service, and **fences** its local instance:
   `REPLICAOF <new leader>` if one is known, otherwise
   `REPLICAOF --fence-host --fence-port` (default `127.0.0.1:1`, an address
   nothing listens on), then read-only. A replica rejects client writes, so
   an isolated old master stops accepting writes instead of merely stopping
   to advertise. Any node that is not leader but finds its instance in the
   master role fences it the same way.
7. **Consul blips**: individual Consul errors are tolerated. The leader
   steps down only when it has not renewed successfully for a full session
   TTL (by which time Consul has released the lock anyway). A 404 on renew
   (session gone) demotes immediately.
8. **Release**: lock release uses Consul's `?release=<session>`, which is
   atomic and only succeeds if our session still holds the key.
9. **Shutdown**: SIGTERM removes the master registration and releases the
   lock immediately (fast handover); followers just deregister.
10. **Flapping**: capped exponential backoff with jitter between
    elections. `--min-leader-time` (default 0 = off): a new leader keeps
    renewing and holds the lock through local health failures until it has
    led this long. It never delays involuntary loss (session gone, Consul
    outage past TTL, lock taken) or shutdown.

### Observe mode

`--mode=observe` runs the full loop — session, acquire attempts, lag gate —
but only writes its own key `kvrocks/<cluster>/observe/leader`, never
touches kvrocks (`REPLICAOF`, `CONFIG`) or the service catalog, and logs
every action it *would* take. Run it alongside Sentinel to validate before
cutover.

## kvrocks replication notes (kvrocks 2.11)

Checked against the kvrocks 2.11 source (`GetReplicationInfo`):

- Replica: `role:slave`, `master_host`, `master_port`,
  `master_link_status`, `slave_repl_offset`, and `master_repl_offset`
  which is the **replica's own** latest sequence. A replica cannot learn
  its lag from its own INFO.
- Master: `role:master`, `connected_slaves`,
  `slaveN:ip=..,port=..,offset=..,lag=..`, `master_repl_offset`.
- Offsets are RocksDB sequence numbers, **not bytes**. With kvrocks
  `--max-lag-bytes` is therefore a sequence-number delta (roughly a write
  count); with Redis it is bytes.
- Read-only flag is `slave-read-only` (default yes).
- `REPLICAOF host port` is async: it returns OK even when host is
  unreachable, which is what makes the fence target work.

**Lag gate**: a follower samples the leader's `master_repl_offset` over the
wire every tick. When the leader goes away it compares the last sample with
its own offset. Writes the old leader accepted after the last sample are not
counted, so lag can be under-estimated by up to one tick of writes. With no
sample (e.g. just started) lag is unknown and the `--hold-down` applies.

### Running against standalone Redis

The sidecar also works against a standalone Redis pair (not Cluster —
it knows nothing about hash slots):

```sh
kvrocks-leader --kv-kind=redis --kvrocks-port 6379 --cluster main ...
```

(`--kv-kind=redis`, `$KV_KIND=redis`.) The only backend-specific step is
`CONFIG SET use-rsid-psync yes`, which is skipped for `redis` since Redis
has no such setting. Everything else — `PING`, `INFO replication` lag
parsing (Redis reliably reports `slave_repl_offset`, so the lag gate is
exact rather than unknown), `REPLICAOF` (≥ Redis 5, `SLAVEOF` fallback
below that), `replica-read-only`/`slave-read-only` — is shared. Re-run
the failure-mode scenarios against Redis before relying on the timings
above, as PSYNC/full-sync dynamics differ from kvrocks.

## Split-brain window

If the old master is partitioned from Consul but still reachable by
clients, it keeps accepting writes until its sidecar has gone a full TTL
without a successful renew, then fences itself (replica of the fence target
or the new leader). Consul releases its lock at about the same time, and the
follower promotes after lock-delay plus the lag gate. Writes accepted by the
old master after the last replication to the new one are lost when it
resyncs from the new leader (same as with Sentinel). If the old master's
**sidecar** dies, nothing fences kvrocks until the sidecar returns; run it as
a Nomad sidecar so it is restarted with kvrocks.

## Failure modes

| Failure | What happens | Typical time |
|---|---|---|
| Master's kvrocks dies | Sidecar stops renewing after 3 failed checks (~9s); session expires (≤10s) + lock delay (1s); follower's lag gate passes; promotes | ~15–25s |
| Master's sidecar dies, kvrocks lives | Session TTL expires without renewal; lock released; follower promotes; old master keeps serving as master until its sidecar returns (then it sees the new leader and follows) — brief dual-master, same as Sentinel | ~TTL + delay ≈ 11s |
| Master partitioned from Consul | Leader keeps serving until a full TTL without a successful renew, then fences itself; follower promotes after session expiry | ~TTL + delay |
| Nodes partitioned from each other, both reach Consul | Lock stays with current holder; other side's gate refuses (old master recently seen) until hold-down | no failover (correct) |
| Consul restarted / outage shorter than TTL | Errors tolerated, leader keeps the lock, renew succeeds on return | no failover |
| Consul outage longer than TTL | Leader fences itself; sessions re-created on return; re-election with backoff | ~outage + seconds |
| SIGTERM on leader | Deregister + release synchronously | ~1–2s |
| Both start simultaneously | Empty key race; Consul acquire is atomic — one wins, epoch 1 | ~seconds |
| Replica far behind (`lag > --max-lag-bytes`) | Promotion refused, stays follower, warns; retries each tick | operator must resync |
| Lag unknown + old master recently seen | Refused until `--hold-down` (default 30s) elapses | ≤ hold-down |

## Running

```sh
kvrocks-leader \
  --consul-addr 127.0.0.1:8500 \
  --kvrocks-addr 127.0.0.1 --kvrocks-port 6666 \
  --node kv01 --advertise-addr 10.0.0.11 \
  --cluster main --mode active
```

Every flag has an env equivalent (`CONSUL_HTTP_ADDR`, `KVROCKS_ADDR`,
`KVROCKS_PORT`, `KVROCKS_PASSWORD` / `KVROCKS_PASSWORD_FILE`,
`CONSUL_HTTP_TOKEN` / `CONSUL_TOKEN_FILE`, `NODE_NAME`,
`ADVERTISE_ADDR`, `CLUSTER_NAME`, `SERVICE_NAME`, `MODE`, …).
Passwords/tokens are never logged.

`--advertise-addr-private` (e.g. the NetBird address) also registers a
second service, `--private-service-name` (default `<service>-private`), with
the same tags, puts `public`/`private` TaggedAddresses on both entries, and
makes replicas replicate over the private address.

Health: `GET :8080/healthz` returns 200 only when kvrocks answers, Consul
has answered within the session TTL, and kvrocks' role matches ours
(master while leader, replica while follower; skipped in observe mode),
otherwise 503 with a `problems` list. Also `GET :8080/status`,
`GET :8080/metrics` (`is_leader`, `elections_total`,
`leader_changes_total`, `lock_held_seconds`, `kvrocks_up`,
`replication_lag_bytes`, `fence_failures_total`).

Deploy examples: `deploy/consul-policy.hcl` (per-node ACL policy) and
`deploy/kvrocks.nomad.hcl` (one group per host; needs
`-var sidecar_image=...`).

Clients find the master via Consul, e.g. `kvrocks.service.consul` with
tag `master` (or your `--service-name`/`--master-tag`).

## Cutover from Sentinel

1. **Observe**: deploy sidecar with `--mode=observe` next to both nodes.
   Watch logs (`would acquire lock`, `would REPLICAOF`); confirm steady
   leader agreement with Sentinel's choice.
2. **Test env**: compose (`deploy/docker-compose.yml`, ports 18379/18380,
   Consul 18500 — never 6379/production) + `make integration` scenarios.
3. **Production active**: switch one node, then the other; keep Sentinel
   up until the sidecar has held a stable leader for 2× TTL, then
   decommission Sentinel.
4. **Rollback**: stop sidecars, restart Sentinel; kvrocks topology left
   behind is plain `SLAVEOF` which Sentinel understands.

## Repo layout

- `cmd/kvrocks-leader` — main (flags → clients → controller + HTTP)
- `internal/config` — flags/env, observe key prefix
- `internal/kvrocks` — Redis-protocol client + defensive INFO parser
- `internal/consulstore` — session/lock/registrar interfaces + live impl
- `internal/elect` — pure promotion gate, epoch, backoff (unit-tested)
- `internal/controller` — tick/watch/demote/shutdown state machine
- `internal/httpapi`, `internal/metrics`, `internal/logx`
- `deploy/` — `consul-policy.hcl`, `kvrocks.nomad.hcl`, compose, `integration.sh`
