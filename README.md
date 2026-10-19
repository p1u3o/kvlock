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
2. **Session**: Consul session, TTL `--session-ttl` (default 10s),
   `behavior=release`, `LockDelay` `--lock-delay` (default 1s; Consul's
   15s default is far too slow for failover). Renewed at TTL/3, only
   while kvrocks is healthy.
3. **Election**: atomic acquire of `kvrocks/<cluster>/leader` with JSON
   `{node, addr, port, session, since, epoch}`. `epoch` increments on
   every new leader.
4. **Leader**: `REPLICAOF NO ONE` (fallback `SLAVEOF NO ONE` for
   kvrocks < 2.11), best-effort writable + `use-rsid-psync yes`, registers
   Consul service `--service-name` (default `kvrocks`) tagged
   `--master-tag`, with a TTL check the sidecar refreshes (TTL =
   session TTL). Failover never waits on Nomad check intervals.
5. **Follower**: blocking-query watch on the leader key, registers the
   same service tagged `--replica-tag`, issues `REPLICAOF host port` only
   when not already following that host:port.
6. **Demote first**: on session invalidation, sustained unhealthiness, or
   Consul unreachable longer than ~TTL, the leader fails its TTL check +
   deregisters the master service and sets read-only *before* releasing the
   lock. An isolated old master therefore stops advertising within ~1 TTL.
7. **Shutdown**: SIGTERM removes the master registration and releases the
   lock immediately (fast handover); followers just deregister.
8. **Flapping**: capped exponential backoff with jitter between elections;
   `--min-leader-time` reserved for voluntary step-down hysteresis.

### Observe mode

`--mode=observe` runs the full loop — session, acquire attempts, lag gate —
but only writes its own key `kvrocks/<cluster>/observe/leader`, never
touches kvrocks (`REPLICAOF`, `CONFIG`) or the service catalog, and logs
every action it *would* take. Run it alongside Sentinel to validate before
cutover.

## kvrocks replication notes (verified Oct 2026)

From kvrocks docs / supported-commands table:

| Fact | Status |
|---|---|
| `SLAVEOF host port` / `SLAVEOF NO ONE` | supported since v1.0.0 |
| `REPLICAOF host port` / `REPLICAOF NO ONE` | supported since v2.11.0 (sidecar tries it first, falls back to `SLAVEOF`) |
| `ROLE` | supported since v1.1.30 |
| `INFO replication` → `role:master\|slave`, `master_repl_offset` | documented |
| `CONFIG GET/SET` | supported since v1.0.0; `use-rsid-psync` exists in `kvrocks.conf` (default `no`) — sidecar sets `yes` at startup for partial resync |
| Replica-side lag fields / read-only flag name (`replica-read-only` vs `slave-read-only`) | **UNVERIFIED** — parsed defensively; unknown lag returns -1 and the hold-down gate applies. `SetReadOnly` tries both names, failures are logged not fatal |

Target kvrocks 2.7.0+ (has `REPLICAOF`). Older than 2.11 works via the
`SLAVEOF` fallback but is untested — verify `INFO replication` output on
your version before production cutover.

## Split-brain window (honest)

Consul sessions + TTL checks are AP-ish during partitions. Worst case: the
old master is partitioned from Consul but still reachable by clients. It
demotes itself on the first failed renew/tick (session TTL expiry backs
this up server-side via `behavior=release`; its TTL check goes critical
within ~TTL), but in-flight writes during the ~10s window can diverge.
Mitigations: short TTL (10s), demote-first ordering, lag gate + hold-down
so the new master refuses a badly-behind replica, and `use-rsid-psync`
for incremental catch-up. For a 2-node pair without fencing this is the
expected trade-off; documented here so it is a conscious choice.

## Failure modes

| Failure | What happens | Typical time |
|---|---|---|
| Master's kvrocks dies | Sidecar stops renewing after 3 failed checks (~9s); session expires (≤10s) + lock delay (1s); follower's lag gate passes; promotes | ~15–25s |
| Master's sidecar dies, kvrocks lives | Session TTL expires without renewal; lock released; follower promotes; old master keeps serving as master until its sidecar returns (then it sees the new leader and follows) — brief dual-master, same as Sentinel | ~TTL + delay ≈ 11s |
| Master partitioned from Consul | Leader demotes itself (fail TTL + read-only) on first Consul error; follower promotes after session expiry | ~TTL ≈ 10s; isolated old master stops advertising within ~TTL |
| Nodes partitioned from each other, both reach Consul | Lock stays with current holder; other side's gate refuses (old master recently seen) until hold-down | no failover (correct) |
| Consul restarted / brief outage | Both sides error; leader demotes; sessions re-created on return; re-election with backoff | ~outage + 15s |
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

Health: `GET :8080/healthz`, `GET :8080/status`, `GET :8080/metrics`
(`is_leader`, `elections_total`, `leader_changes_total`,
`lock_held_seconds`, `kvrocks_up`, `replication_lag_bytes`).

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
