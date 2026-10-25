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
9. **Shutdown**: SIGTERM removes the master registration, fences the local
   instance (REPLICAOF the fence target, confirmed with INFO), drops client
   connections and only then releases the lock, so the next leader never
   overlaps with this one. If nothing answers on the local address
   (connection refused, or no reply within min(kvrocks timeout, 1s), three
   tries) the instance is treated as gone and the lock is released at once;
   this relies on kvrocks booting fenced (`replicaof` in its config). If it
   answers but fencing fails, the lock is left to expire with the session. Followers just deregister. Give the task a kill timeout of
   at least 10s.
   A non-leader whose instance comes back after a restart or a failed fence
   is fenced within ~250ms; booting kvrocks with `replicaof <fence-host>
   <fence-port>` in its config closes the remaining gap.
   Epochs come from a counter key next to the lock (`kvrocks/<cluster>/epoch`)
   that is only ever increased, and releasing the lock keeps its value.
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
  `--max-lag` (old name `--max-lag-bytes`) is therefore a sequence-number
  delta, roughly a write count, and defaults to 1000; with Redis it is
  bytes and defaults to 100MiB. One large value is one sequence number, so
  keep the bound small if values are big.
- Read-only flag is `slave-read-only` (default yes).
- `REPLICAOF host port` is async: it returns OK even when host is
  unreachable, which is what makes the fence target work.

**Lag gate**: a follower samples the leader's `master_repl_offset` over the
wire every tick. When the leader goes away it compares the last sample with
its own offset. Writes the old leader accepted after the last sample are not
counted, so lag can be under-estimated by up to one tick of writes. With no
sample (e.g. just started) lag is unknown and the `--hold-down` applies.

At promotion time the gate measures again: its own offset comes from the
current tick, and if the departed leader's kvrocks still answers as a master
it is re-sampled. If the last sample was older than `--lag-sample-max-age`
(default 3x `--check-interval`) when the leader was last seen, for example
because its kvrocks stopped answering before its session went away, a lag
that looks within bound counts as unknown and the hold-down applies. A lag
already over the bound stays a refusal.

**Time-based gate (v0.2)**: the offset gate counts writes, not time. A burst
of small writes refuses a replica that is only milliseconds behind, and on a
quiet cluster a replica that stopped replicating minutes ago still looks
current. So the leader also writes a heartbeat key, `kvlock:hb:<cluster>`
(value `<unix ms>:<epoch>`), every `--heartbeat-interval` (default 250ms,
0 = off). Followers read it from the leader's kvrocks and from their own
copy; the lag is the difference of the two values. Both were stamped by the
leader's clock, so clock skew between nodes does not matter, and a leader
that just died does not inflate the lag (unlike "now minus heartbeat").

At promotion, when a heartbeat of the departed leader was sampled, the time
gate decides: allowed when lag <= `--max-lag-time` (default 2s), otherwise it
refuses like the offset gate (hold-down, sample age and
`--lag-gate-max-wait` apply the same way). A replica that has no copy of a
heartbeat the leader had counts as far behind. When no leader heartbeat was
ever seen (first start, heartbeats disabled, a v0.1 leader) the offset gate
above is used. `/status` shows which gate would apply (`lag_gate`).

Observe mode never writes the heartbeat; it only reads an existing key.
Clients behind the proxy can see the key; it is tiny and rewritten in place.

A replica over `--max-lag` refuses until it catches up or the old leader
comes back. By default (`--lag-gate-max-wait 0`) it never promotes despite
lag. A non-zero `--lag-gate-max-wait` is an opt-in override: once the old
leader has been gone that long the replica promotes anyway and the writes the
old leader never replicated are lost. Such a promotion logs an error
("promoting despite lag"), adds a `lag_gate` event, increments
`kvlock_promoted_despite_lag_total`, sets the gauge
`kvlock_promoted_despite_lag` to 1 and `/status` `promoted_despite_lag` to
true for as long as that node stays leader. Alert on them.

`--lag-gate-max-bytes` (default 4194304, 4MiB; 0 = off) is a hard cap on top
of the gates above: a replica whose offset lag is over it never promotes, and
`--lag-gate-max-wait` cannot force it; it waits until it has caught up to
within the cap. At or under the cap the other gates still decide (the cap is
necessary, not sufficient). The lag is the backend offset difference
(`lag_bytes` in `/status`): bytes for Redis, but write sequence numbers for
kvrocks, so for kvrocks the value counts writes, not bytes.

`/status` reports `promotable` (false on the leader and on a replica the gate
would refuse now) and `promote_blocked` with the reason, e.g. lag over
`--lag-gate-max-bytes`, `--max-lag-time` or `--max-lag`. It does not model the
hold-down timer. The status page shows this next to the promotion gate.

### Running against standalone Redis

The sidecar also works against a standalone Redis pair (not Cluster —
it knows nothing about hash slots):

```sh
kvrocks-leader --kv-kind=redis --kvrocks-port 6379 --cluster main ...
```

(`--kv-kind=redis`, `$KV_KIND=redis`.) The only backend-specific step is
`CONFIG SET use-rsid-psync yes` (retried once the instance first answers,
unsupported on newer kvrocks and then ignored), which is skipped for `redis` since Redis
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
| SIGTERM on leader | Deregister, fence, kill clients, release | ~1–2s |
| Both start simultaneously | Empty key race; Consul acquire is atomic — one wins, epoch 1 | ~seconds |
| Replica far behind (`lag > --max-lag`) | Promotion refused, stays follower, warns; retries each tick; never promotes by default; with an opt-in `--lag-gate-max-wait` promotes after it (flagged `promoted_despite_lag`), but never while over `--lag-gate-max-bytes` | none by default; with max wait set, loses the unreplicated writes |
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
otherwise 503 with a `problems` list. Also `GET :8080/status` and
`GET :8080/metrics` (see [Metrics](#metrics)).

Deploy examples: `deploy/consul-policy.hcl` (per-node ACL policy) and
`deploy/kvrocks.nomad.hcl` (one group per host; needs
`-var sidecar_image=...`).

Clients find the master via Consul, e.g. `kvrocks.service.consul` with
tag `master` (or your `--service-name`/`--master-tag`).

## Metrics

`GET /metrics` is a standard Prometheus endpoint (client_golang, private
registry, plus Go/process collectors).

**v0.2 rename:** every sidecar metric now starts with `kvlock_` (for example
`is_leader` is `kvlock_is_leader`, `step_downs_total` is
`kvlock_step_downs_total`). Labels are unchanged; update dashboards and
alerts by adding the prefix. Go/process collector names are unchanged.

| metric | type | labels |
|---|---|---|
| `kvlock_is_leader`, `kvlock_kvrocks_up` | gauge | |
| `kvlock_replication_lag_bytes` | gauge (-1 = unknown) | offset lag (sequence numbers with kvrocks) |
| `kvlock_replication_lag_seconds` | gauge (-1 = unknown) | heartbeat lag, 0 on the leader |
| `kvlock_heartbeat_age_seconds` | gauge (-1 = none) | local clock minus newest local heartbeat (includes clock skew) |
| `kvlock_heartbeat_writes_total` | counter | `result` = ok\|error |
| `kvlock_lag_gate_refusals_total` | counter | |
| `kvlock_lock_held_seconds`, `kvlock_consul_last_renew_age_seconds` | gauge | |
| `kvlock_elections_total`, `kvlock_leader_acquired_total`, `kvlock_leader_changes_total` | counter | |
| `kvlock_step_downs_total` | counter | `reason` = backend_unhealthy, session_invalidated, consul_unreachable, lock_lost, lock_taken, handover, shutdown |
| `kvlock_fencing_events_total` | counter | `target` = leader\|fence, `result` = ok\|error |
| `kvlock_consul_errors_total` | counter | `op` = session\|renew\|lock |
| `kvlock_client_kill_runs_total` (`result`), `kvlock_clients_killed_total` | counter | |
| `kvlock_handovers_total` | counter | `result` = ok\|aborted\|no_successor |
| `kvlock_proxy_active_connections` | gauge | `listener` = write\|read |
| `kvlock_proxy_dropped_connections_total`, `kvlock_proxy_refused_connections_total` | counter | `listener` |
| `kvlock_backup_last_success_timestamp_seconds` | gauge | unix time of this node's last uploaded backup (0 = none from this node) |
| `kvlock_backup_last_size_bytes`, `kvlock_backup_duration_seconds` | gauge | last backup on this node (encrypted size; BGSAVE + upload time) |
| `kvlock_backup_failures_total` | counter | failed backup attempts on this node |

`fence_failures_total` from v0.1 is replaced by
`kvlock_fencing_events_total{result="error"}`.

## Status page

`GET /` (or `/ui`) on the HTTP port serves a small self-contained page
(embedded, no external assets, strict CSP) that polls `/status` every 2s:
cluster, mode, role, leader/epoch/lock age, addresses, replication link and
lag (time and offset), Consul renew age, health, fencing state, proxy and
sentinel connection counts, and the last 50 events (elections, follows,
step-downs, fences, handovers, lag-gate refusals). Everything is rendered as
text. The page is read-only; a handover button appears only after an admin
token is typed into the page, and it is sent as `Authorization: Bearer` to
`POST /handover`. `/status` itself has no secrets but exposes topology, so
keep the port on a private network.

## Client connections on demotion

After a demoted master has been fenced (REPLICAOF succeeded), the sidecar
runs `CLIENT KILL TYPE normal SKIPME yes` so clients drop their
connections and re-resolve the master instead of getting READONLY errors.
kvrocks 2.11 supports `TYPE normal|pubsub|master|replica` and `SKIPME`;
`TYPE normal` excludes replication links and pub/sub connections, and the
sidecar's own connection (pool size 1, named `kvlock-sidecar`) is skipped.
If the server rejects the syntax, it falls back to `CLIENT LIST` plus
`CLIENT KILL ID` for every connection that is not a replica/master link,
not subscribed, and not the sidecar. Disable with
`--kill-clients-on-demote=false` (`$KILL_CLIENTS_ON_DEMOTE`). Nothing is
killed in observe mode or if fencing failed.

## Planned handover

```sh
# on the current leader (or with --addr pointing at it)
kvrocks-leader handover [--to 10.0.0.12:6666] [--max-lag 0] [--timeout 30s]
```

or `POST /handover` with an optional JSON body
`{"to": "ip:port", "max_lag": 0, "timeout": "30s"}`. Without
`--admin-token`/`--admin-token-file` (`$KVLOCK_ADMIN_TOKEN[_FILE]`) the
endpoint only accepts loopback clients; with a token it requires
`Authorization: Bearer <token>` from anywhere. The CLI reads the same env
vars (`$KVLOCK_ADMIN_TOKEN`, or `--token-file`/`$KVLOCK_ADMIN_TOKEN_FILE`) and
`$KVLOCK_ADMIN_URL`. Since v0.2.2 the CLI refuses `--token`: a token on the
command line lands in shell history and `ps` output.

Steps on the leader:

1. **Checks**: active mode, holding the lock, Consul session renewed within
   TTL/2, local instance is master, and an online replica exists (the one
   named by `--to`, as `ip:port` in the master's `INFO replication`, or the
   least-lagged one).
2. **Wait** until that replica's lag is within `--max-lag` (default
   `--handover-max-lag`, which defaults to `--max-lag-bytes`), up to the
   timeout (`--handover-timeout`, 30s). Writes continue meanwhile. Timeout
   or any failed check aborts: nothing has changed and the node stays
   leader (HTTP 409, CLI exit 1).
3. **Commit**, inside the control loop: re-check, remove the master
   registration, fence to `--fence-host` (not to the replica, to avoid a
   replication cycle), write a hint into the lock value
   (`handover_to`, `handover_until` = now + timeout, `handover_offset` =
   our final offset), kill clients, release the lock. A failure before the
   release (fence or hint write) re-promotes the local instance and keeps
   leadership.
4. The other sidecars see the hint: non-targets do not contend (and fence
   themselves if writable) until it expires; the target acquires once it
   has replicated up to `handover_offset` (exactly, for the first half of
   the window; within `--handover-max-lag` afterwards). The old leader
   then follows the new one as a normal replica. HTTP 200 reports the new
   leader; 202 means the lock was released but no successor appeared
   within the timeout, after which normal election resumes.

Caveats: the hint expiry is wall-clock based, so keep node clocks in sync;
v0.1 sidecars ignore the hint (upgrade all nodes first); there is a short
write outage between fencing and the target's promotion (normally one
check interval).

### Handover on shutdown

On SIGTERM/SIGINT a leader first tries the same planned handover to the
least-lagged online replica, waiting at most `--shutdown-handover-timeout`
(`$SHUTDOWN_HANDOVER_TIMEOUT`, default 5s, 0 = off) for it to be within
`--handover-max-lag`. If there is no such replica in time, or the commit
fails, it falls back to fence-then-release. The local kvrocks must still be
running when the sidecar gets the signal (in Nomad, run kvrocks as the
prestart sidecar task and kvrocks-leader as the main task so it is stopped
first), and the sidecar's kill timeout must exceed the handover timeout.

## Watchdog (fence on stale sidecar)

If the sidecar process dies or hangs while its kvrocks is master, nothing
fences that master until Consul expires the session, and a replica may be
promoted meanwhile. `kvrocks-leader watchdog` closes that gap:

- The sidecar (with `--watchdog-file PATH`, `$KVLOCK_WATCHDOG_FILE`) writes a
  small JSON heartbeat file every `--watchdog-interval` (1s) from its own
  goroutine, atomically (temp file + rename). It also records when the
  control loop last finished a tick (`loop_unix_ms`, informational).
- The watchdog, a separate process next to kvrocks, reads the file every
  `--interval` (1s). When it is older than `--timeout` (5s, keep it **below
  `--session-ttl`**, default 10s) **and** local kvrocks is a writable master,
  it fences: `CONFIG SET replica-read-only yes`, `REPLICAOF --fence-host
  --fence-port` (default 127.0.0.1:1, unroutable), optionally `CLIENT KILL
  TYPE normal SKIPME yes` (`--kill-clients`), then confirms `role:slave`
  and that a probe write (`kvlock:watchdog:probe`, PX 2000) is refused.
  The read-only step matters: a promoted node runs with
  `slave-read-only=no`, and REPLICAOF alone leaves it writable as a
  disconnected replica (found by the loadtest, see below).
- It re-reads the heartbeat right before fencing, so a sidecar that just
  restarted is not raced. A missing file within `--startup-grace` (60s) is
  "starting". Stale + replica, or stale + kvrocks down, is left alone.
- It never promotes, never touches Consul and never undoes a fence; the
  restarted sidecar recovers the node through its normal follower path.
- **Observe mode**: if the watchdog runs with `--mode observe` or the
  heartbeat says `"mode":"observe"`, it only logs and counts
  `kvlock_watchdog_observe_would_fence_total`; it never fences, kills
  connections or changes config.

```sh
kvrocks-leader watchdog --heartbeat-file /alloc/kvlock/heartbeat \
  --kvrocks-port 6666 --timeout 5s --kill-clients --http-addr 127.0.0.1:9101
```

Metrics on `--http-addr` (`/metrics`, `/healthz`), own registry:
`kvlock_watchdog_heartbeat_age_seconds`, `kvlock_watchdog_heartbeat_stale`,
`kvlock_watchdog_checks_total{outcome}` (healthy, starting, stale_replica,
stale_backend_down, observe_would_fence, raced, fenced, fence_failed),
`kvlock_watchdog_fences_total{result}`, `kvlock_watchdog_observe_would_fence_total`,
`kvlock_watchdog_clients_killed_total`, `kvlock_watchdog_last_fence_timestamp_seconds`.
The sidecar adds `kvlock_watchdog_heartbeat_writes_total{result}`.
Alert on `fences_total{result="error"}` and on any `fences_total{result="ok"}`.
A Nomad task snippet is in [`docs/watchdog.nomad.hcl`](docs/watchdog.nomad.hcl).

## Cluster status CLI

```sh
kvrocks-leader status --addr 10.0.0.11:8080 --addr 10.0.0.12:8080
kvrocks-leader status --consul --service kvrocks --http-port 8080 --json
```

Reads `/status` from every node (`--addr` repeatable or comma-separated,
`$KVLOCK_STATUS_ADDRS`, or every catalog instance of `--service` via
`$CONSUL_HTTP_ADDR`), prints NODE / ROLE / EPOCH / HEALTH / LAG / LAST
BACKUP / SESSION, or the full report with `--json`. Exit codes: `0`
healthy; `1` when a node is unreachable or unhealthy, there is no leader,
more than one leader or writable master (split brain), nodes disagree on
the leader, a fence is pending, or a replica is over the lag limit (each
node's `--max-lag-time`, or `--max-lag`); `2` usage/discovery error.
Unknown lag is a warning only.

## Load test (`kvrocks-leader loadtest`)

Drives sequence-numbered mixed load and measures what a handover or
failover costs. **Test clusters only**: it refuses port 6379 outright and
refuses any `--addr` not listed in `--allow` (`host:port` or `:port`).

```sh
export KVLOCK_ADMIN_TOKEN=...   # or --token-file; never --token
kvrocks-leader loadtest \
  --addr 10.0.0.11:8379 --addr 10.0.0.12:8379 --allow :8379 \
  --duration 120s --rate 2000 --workers 8 --keys 10000 \
  --value-min 64 --value-max 1024 --pipeline 4 --read-ratio 0.7 \
  --status-addr 10.0.0.11:8080 --status-addr 10.0.0.12:8080 \
  --handover-at 30s
```

- Every key belongs to one worker, so writes per key are ordered; values
  carry a global sequence number. Keys are `kvlock:loadtest:<run-id>:N`
  and are deleted afterwards (`--keep-keys` to keep them).
- With several `--addr` the writable master is discovered via `INFO
  replication` and re-discovered after errors; with one address (proxy or
  DNS) it is used as-is.
- `--handover-at` calls `POST /handover` on the leader (found via
  `--status-addr`, or `--admin-url`). For a hard failover, kill the leader's
  sidecar or kvrocks yourself and `kill -USR1 <loadtest pid>` to drop a
  marker into the timeline. `--status-addr` adds a leader/epoch timeline.
- Report: writes/reads ok and failed with error kinds, the longest gap
  between acknowledged writes and total unavailability (gaps above a
  threshold), latency p50/p99/max per pipeline, **lost writes** (a key
  holds an older sequence than one acknowledged) and **stale reads** (a
  read returned an older sequence than one acknowledged before the read).
  `--json` for machine output. Exit `1` on any lost or stale data.

## Leader proxy

`--proxy-addr :6380` (`$PROXY_ADDR`) on every sidecar opens a plain TCP
passthrough to the current lock holder: the local instance when this node
is leader, otherwise the leader's private advertise address when it has
one, else its public one. The stream is not parsed, so TLS and AUTH pass
straight through to kvrocks (clients use the kvrocks password and TLS
settings; certificates must be valid for the name the client connects
to).

It fails closed:

- all proxied connections are closed immediately on any change of lock
  holder, of this node's role, or when this node fences its instance;
- new connections are refused (accepted and closed) while the leader is
  unknown, the local leader is not yet a writable master, or Consul has not
  answered within two check intervals (the leader's TTL grace window), and
  existing ones are dropped when that happens.

`--proxy-read-addr :6381` (`$PROXY_READ_ADDR`) routes to the local
instance while it is a replica of a known leader within `--max-lag-bytes`;
otherwise `--proxy-read-fallback leader` (default) sends reads to the
leader and `refuse` refuses them.

Clients should list the proxy on both nodes (`kv01:6380,kv02:6380`) or use
the Consul name, so losing one host only costs a reconnect. Each hop adds a
little latency; connections are long-lived, so pooling clients are fine.

## Replication forwarder (optional, dynamic ports)

Off by default. With `--forward`, kvlock listens on a fixed loopback
address (default `127.0.1.1:18379`) and forwards to the current leader's
advertised address, which can be a Nomad dynamic port taken from
`--advertise-nomad-label`. Replicas run `REPLICAOF` to that address once.

- When the leader, session or epoch changes, every forwarded connection
  is cut and new ones go to the new leader.
- A connection that arrives during the gap is held for up to
  `--forward-hold` (default 5s).
- The leader's own forwarder refuses connections.
- kvlock sets `replica-announce-ip/-port` on its kvrocks (from
  `NOMAD_HOST_IP_<label>`/`NOMAD_HOST_PORT_<label>` or
  `--replica-announce-ip/-port`) before `REPLICAOF`, and re-applies it
  after a kvrocks restart. `--replica-announce=false` disables it.
- Defaults: forwarder `127.0.1.1:18379`, peers `127.0.2.<index>:18379`,
  hold 5s, `--peer-expose` off (enable only once every node runs a
  version that understands the JSON member format).

`--peer-expose` gives each of N instances a stable address
`127.0.2.<NOMAD_ALLOC_INDEX>:18379`. It is used for status and for the
pre-promotion offset check.

Design, the kvrocks self-replication analysis, every config key and a
Nomad jobspec are in [docs/forwarder.md](docs/forwarder.md). The
kvlock-test rollout plan is in
[docs/forwarder-test-plan.md](docs/forwarder-test-plan.md).

## Sentinel-compatible endpoint

`--sentinel-addr :26379` (`$SENTINEL_ADDR`) serves a read-only subset of
the Sentinel protocol so existing Sentinel-aware clients can keep their
configuration during or after the cutover. The master name is
`--sentinel-name` (default `--cluster`). Answers come from the Consul lock
(master: the holder's public address, or private with
`--sentinel-private`) and passing `--replica-tag` registrations
(replicas, cached for 2s).

Supported: `PING`, `SENTINEL get-master-addr-by-name|masters|master|
replicas|slaves|sentinels|myid`, `ROLE`, `INFO`, `CLIENT SETNAME/SETINFO`,
`QUIT`, and `SUBSCRIBE +switch-master` (published within ~0.5s of the
holder's address changing, as `<name> <old-ip> <old-port> <new-ip>
<new-port>`). `get-master-addr-by-name` returns nil while the lock is
vacant. State-changing commands (`SENTINEL failover`, `set`, `monitor`,
`reset`, …) and anything else return errors. There is no AUTH and no TLS
on this listener; bind it to a private interface. Each sidecar answers
on its own; `num-other-sentinels` is 0 and `quorum` 1.

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

## Backups (S3)

Optional and **off by default**. When enabled, the cluster takes an
encrypted backup every `--backup-interval` (default 6h) and streams it to
any S3-compatible store (AWS, Scaleway, MinIO, …).

```sh
kvrocks-leader ... \
  --backup --backup-dir /kvrocks/backup \
  --backup-s3-endpoint s3.fr-par.scw.cloud --backup-s3-region fr-par \
  --backup-s3-bucket my-backups --backup-s3-prefix kvlock/ \
  --backup-key-file /secrets/backup.key \
  --backup-s3-access-key-file /secrets/s3-ak --backup-s3-secret-key-file /secrets/s3-sk
```

| Flag | Env | Default |
|---|---|---|
| `--backup` | `KVLOCK_BACKUP` | false |
| `--backup-interval` | `KVLOCK_BACKUP_INTERVAL` | 6h (min 1m) |
| `--backup-dir` | `KVLOCK_BACKUP_DIR` | required: kvrocks `backup-dir` as mounted in the sidecar |
| `--backup-bgsave-timeout` | `KVLOCK_BACKUP_BGSAVE_TIMEOUT` | 30m |
| `--backup-keep` | `KVLOCK_BACKUP_KEEP` | 0 (keep all) |
| `--backup-allow-leader` | `KVLOCK_BACKUP_ALLOW_LEADER` | false |
| `--backup-leader-grace` | `KVLOCK_BACKUP_LEADER_GRACE` | 1h |
| `--backup-key-file` | `KVLOCK_BACKUP_KEY_FILE`, or the key itself in `KVLOCK_BACKUP_KEY` | required |
| `--backup-s3-endpoint` | `KVLOCK_BACKUP_S3_ENDPOINT` | required, `host[:port]` |
| `--backup-s3-region` | `KVLOCK_BACKUP_S3_REGION` / `AWS_REGION` | |
| `--backup-s3-bucket` | `KVLOCK_BACKUP_S3_BUCKET` | required |
| `--backup-s3-prefix` | `KVLOCK_BACKUP_S3_PREFIX` | `kvlock/` |
| `--backup-s3-insecure` | `KVLOCK_BACKUP_S3_INSECURE` | false (HTTPS) |
| `--backup-s3-sse` | `KVLOCK_BACKUP_S3_SSE` | `AES256`; `none` to not send the header |
| `--backup-s3-access-key-file` | `KVLOCK_BACKUP_S3_ACCESS_KEY_FILE`, or `AWS_ACCESS_KEY_ID` | required |
| `--backup-s3-secret-key-file` | `KVLOCK_BACKUP_S3_SECRET_KEY_FILE`, or `AWS_SECRET_ACCESS_KEY` | required |

Generate a key with `openssl rand -hex 32` (raw 32 bytes, 64 hex chars or
base64 are accepted). **Losing the key makes every backup unreadable.**

**Who backs up.** Only a healthy follower whose instance is a linked,
caught-up replica (the same lag bound as promotion) and that holds a
valid Consul session. The leader backs up only with
`--backup-allow-leader`, and then only when it has no replicas or no
backup has succeeded for `interval + leader-grace`. Observe mode never
runs BGSAVE or touches S3.

**Single uploader.** `kvrocks/<cluster>/backup/last` holds a JSON record
(node, object key, time, size, duration). A node only tries when that
record is older than the interval, then acquires
`kvrocks/<cluster>/backup/lock` with its own controller session (so the
lock vanishes if the node dies), re-reads the record, and only then runs
the backup. A failed attempt backs off 15m on that node, leaving room for
the other replicas.

**What is uploaded.** `BGSAVE` makes kvrocks write a RocksDB checkpoint
into its `backup-dir`; kvlock polls `INFO persistence` until
`bgsave_in_progress` is 0 and checks `last_bgsave_status:ok`, then streams
`tar | gzip | encrypt` of that directory to
`<prefix>kvlock-<cluster>-<UTC yyyymmddThhmmssZ>-<node>.tar.gz.kvlb`
(multipart, nothing is staged on disk). The live data dir is never read.
The sidecar needs read access to kvrocks' `backup-dir`. Keep
`max-backup-keep-hours`/`max-backup-to-keep` such that kvrocks does not
purge the dir mid-upload (a full sync can also purge it; the next run
simply retries).

**Retention.** `--backup-keep N` deletes all but the newest N objects of
this cluster under the prefix after each success (bucket lifecycle rules
work too).

**Encryption format (KVLB1).** Client-side AES-256-GCM, streamed in chunks:

```
header : "KVLKBAK1" | chunk_size uint32 BE | nonce_prefix [7]byte   (19 bytes)
chunk* : sealed_len uint32 BE | AES-256-GCM(chunk)                   (plaintext <= chunk_size, default 64 KiB)
nonce  = nonce_prefix | counter uint32 BE | last_flag byte (1 on the final chunk)
aad    = the 19-byte header
```

The final chunk (possibly empty) carries `last_flag=1`, so truncation,
reordering, trailing data and tampering are all detected. In addition
the upload requests SSE `AES256`. **Scaleway** rejects that header on
some buckets; there use `--backup-s3-sse none` and enable bucket default
encryption (SSE-ONE) instead.

**Restore.**

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...   # or the *-key-file flags
kvrocks-leader restore --cluster main --list --backup-s3-endpoint ... --backup-s3-bucket ... --backup-key-file key
kvrocks-leader restore --cluster main --target /tmp/restore [--object <key>] --backup-s3-endpoint ... --backup-s3-bucket ... --backup-key-file key
```

`restore` picks the newest backup unless `--object` is given, verifies
and decrypts it, and unpacks into an empty or missing `--target`
(absolute paths, `..` and links are rejected). The result is a RocksDB
checkpoint: stop kvrocks, replace its `dir`'s `db` with it (or point a
fresh instance's `dir` at a copy), start kvrocks, then let the cluster
resync replicas from it. Manual decryption of a downloaded object:

```sh
kvrocks-leader backup-decrypt --backup-key-file key < obj.tar.gz.kvlb | tar -xz -C /tmp/restore
```

The status page shows a Backups card (last success, node, size, errors)
and each start/success/failure/deletion is listed under Recent events.

## Repo layout

- `cmd/kvrocks-leader` — main (flags → clients → controller + HTTP)
- `internal/config` — flags/env, observe key prefix
- `internal/kvrocks` — Redis-protocol client + defensive INFO parser
- `internal/consulstore` — session/lock/registrar interfaces + live impl
- `internal/elect` — pure promotion gate, epoch, backoff (unit-tested)
- `internal/controller` — tick/watch/demote/shutdown state machine
- `internal/controller/handover.go` — planned handover
- `internal/httpapi` — /healthz /status /metrics /handover
- `internal/proxy` — fail-closed TCP leader/read proxy
- `internal/sentinel` + `internal/resp` — Sentinel-compatible endpoint
- `internal/watchdog` — heartbeat file + stale-sidecar fence
- `internal/clusterstatus` — `status` CLI evaluation
- `internal/loadtest` — `loadtest` engine; `internal/testkv` starts a real kvrocks in tests
- `internal/metrics`, `internal/logx`
- `deploy/` — `consul-policy.hcl`, `kvrocks.nomad.hcl`, compose, `integration.sh`
