# Changelog

## v0.2.2 (unreleased)

### Added
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
