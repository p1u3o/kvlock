# Changelog

## v0.2.2 (unreleased)

### Added
- Offset election (`--offset-elect`, `$OFFSET_ELECT`, default on): with no
  leader, sidecars publish their replication offset under
  `kvrocks/<cluster>/offsets/` and the most up-to-date live node promotes even
  if it is a restarted former leader or its lag is unknown. It needs a fresh
  offset from every known live peer (and the old leader, if another node),
  never promotes behind a live node, and breaks ties by lowest node name.
  Fixes the deadlock where the restarted former leader deferred forever to a
  peer the lag gate refused. Metric `kvlock_offset_elections_total`.
- `--health-timeout` (default 5s) and `--unhealthy-slow-threshold` (default
  4): a health probe that times out counts as slow, not dead, and keeps the
  node's state until that many in a row. Refused or reset connections still
  count toward `--unhealthy-threshold`. Metric
  `kvlock_health_probe_slow_total`.
- `--lag-gate-max-bytes` (`$LAG_GATE_MAX_BYTES`, 0 = off): a replica whose
  offset lag is over it never promotes, even after `--lag-gate-max-wait`. It
  is in the backend's offset units, so the default is backend-aware: 1000
  writes for kvrocks, 4MiB for Redis.
- A promotion forced by `--lag-gate-max-wait` logs an error, adds a status
  event, increments `kvlock_promoted_despite_lag_total`, and sets the gauge
  `kvlock_promoted_despite_lag` and `/status` `promoted_despite_lag` while
  that node leads.
- `/status` `promotable`, `promote_blocked` and `max_lag_bytes`.

### Changed
- `--lag-gate-max-wait` now defaults to 0: never promote despite lag. A
  non-zero value is an opt-in override (it was 5m). It also bounds how long a
  restarted former leader defers to a healthy peer, so that wait is now
  unbounded by default while a healthy peer exists.

### Fixed
- A replica catching up under heavy load no longer drops its proxy and
  forwarder routes with "leadership state not confirmed with Consul". The
  leader key was only re-read at the end of tick's local kvrocks calls, which
  together could take longer than the session TTL on a busy replica. A
  separate loop now re-reads the key every min(check-interval/2, 1s) and
  refreshes the confirmation only when Consul shows the same holder session
  and epoch; a new holder, a new epoch or a lost lock cuts routes at once.
  New warnings `tick slow` and `tick reached the leader key late` show when
  tick falls behind.
- A single slow health probe under load (e.g. while a replica resyncs)
  marked kvrocks down, which withdrew the leader proxy route and cut every
  client.
- The leader proxy route key included this node's role and fence counter
  even on replicas, so fencing or re-pointing a replica's own instance cut
  its clients' connections to the unchanged remote leader. It now changes
  only with the leader's session or epoch (plus role and fence state on
  the leader itself).
- The forwarder held clients for `--forward-hold` even when the answer was
  final (e.g. on the leader). It now refuses at once and holds only while
  the leader is unknown or changing.
- A short gap in reading the leader key from Consul cut forwarder and proxy
  routes. The last route is now kept while the epoch is unchanged and loss is
  not known, up to the session TTL; followers verify the leader's kvrocks is
  still master for that epoch, and an epoch change or confirmed loss still
  cuts at once.
- `/status` showed a replica far behind as ready to promote.
- With `--forward`, kvlock sets `replica-announce-ip`/`-port` on its
  kvrocks (`--replica-announce`, `--replica-announce-ip`,
  `--replica-announce-port`), before REPLICAOF and again after a kvrocks
  restart.
- Optional replication forwarder (`--forward`, `--forward-addr`,
  `--forward-hold`):
  - It listens on a fixed loopback address and forwards to the current
    leader, so kvrocks can run on dynamic ports. Replicas run REPLICAOF
    to it once.
  - Connections are cut on any leader, session or epoch change. New
    connections are held while no leader is known. The leader refuses.
  - `/status` and `kvrocks-leader status` show its state.
  - New metrics: `kvlock_proxy_connections_total`,
    `kvlock_proxy_bytes_total` and `kvlock_proxy_route_resets_total`.
- `--advertise-nomad-label` takes the advertised address from
  `$NOMAD_HOST_ADDR_<label>`.
- Optional peer addresses (`--peer-expose`, `--peer-base`, `--peer-port`,
  `--alloc-index`):
  - Each instance is reachable at `127.0.2.<alloc index>`.
  - The index and address are published in the member key.
  - The pre-promotion leader-offset sample uses this address.
- `kvrocks-leader watchdog`: fences the local kvrocks (read-only, REPLICAOF
  to an unroutable target, optional client kill, confirmed by a write probe)
  when the sidecar heartbeat file goes stale while the node is a writable
  master. Passive in observe mode. Metrics under `kvlock_watchdog_*`.
- Sidecar `--watchdog-file` / `--watchdog-interval` (`$KVLOCK_WATCHDOG_FILE`,
  `$KVLOCK_WATCHDOG_INTERVAL`) to write that heartbeat, and
  `kvlock_watchdog_heartbeat_writes_total{result}`.
- `kvrocks-leader status`: cluster table or `--json` from several `/status`
  endpoints or Consul discovery; non-zero exit on no leader, split brain,
  lag over the limit or unreachable/unhealthy nodes.
- `kvrocks-leader loadtest`: sequence-numbered load generator that measures
  write gap, errors, latency, lost writes and stale reads around a handover
  or failover. Refuses port 6379 and anything not in `--allow`.
- `docs/watchdog.nomad.hcl` example task.

### Changed
- `handover` CLI no longer accepts `--token` (it leaks into shell history);
  use `$KVLOCK_ADMIN_TOKEN` or `--token-file`. The flag fails with exit 2
  without echoing its value.

### Fixed
- The sidecar now sets `slave-read-only yes` before every REPLICAOF (follow
  and fence) and re-asserts it once when it finds the node already
  following; only promotion turns it off. Previously a demoted ex-master
  kept `slave-read-only no` from its promotion and accepted writes while
  its replication link was down.
