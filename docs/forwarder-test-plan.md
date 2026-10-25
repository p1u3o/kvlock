# Forwarder test plan: kvlock-test (kv01, kv02, port 8379)

**Status: plan only. Not executed.** Nothing in this branch changes
kvlock-test, kvlock-observe or prod mx-kv.

## 0. Preconditions

- Build the `forwarder` branch image. Pin it by digest. Do not tag a
  release.
- Note the current kvlock-test job version (`nomad job history kvlock-test`)
  so you can roll back.
- Baseline: test3 handover/hard-kill gaps of 9.9s, 6.7s, 1.3s and 1.5s,
  with 0 lost and 0 stale writes.
- Both nodes healthy: `kvrocks-leader status --status-addr kv01:8080
  --status-addr kv02:8080` exits 0.

## 1. NetBird / firewall ports (decide before switching)

With the forwarder, replicas dial the leader's **dynamic** kvrocks port.
They use the advertised private (NetBird) address when one is set.
Clients that go through the leader proxy or the forwarder are unaffected.

| Option | Open between kv hosts | Pros | Cons |
|---|---|---|---|
| A. Nomad default dynamic range | TCP 20000–32000 | No job changes | 12k open ports between the hosts. Any dynamic-port service on them becomes reachable |
| B. Narrow range for kvrocks | e.g. TCP 18400–18409 | Small, auditable rule | Needs `client { min_dynamic_port/max_dynamic_port }`, which is node-wide, or static ports picked from that range per alloc |
| C. Keep a static kvrocks port, forwarder only for stable addressing | TCP 8379 (as today) | No firewall change | Gives up the dynamic-port goal |

**Recommendation: B.**

- Reserve a small range for kvrocks, e.g. 18400–18409, with
  `port "kvrocks" { static = <port in range> }` per group, or a
  dedicated node class whose dynamic range is narrowed.
- Open only that range in the NetBird policy, between the kv peer group.
- 18379 (forwarder) and the 127.0.2.x peer addresses are loopback only,
  so they need no firewall rule.
- Only fall back to A if you accept the larger surface.

## 2. Switch kvlock-test to dynamic ports + forwarding

1. kvrocks task:
   - change `port "kvrocks" { static = 8379 }` to a dynamic port (or a
     port in the narrow range);
   - make the config template set `port {{ env "NOMAD_PORT_kvrocks" }}`,
     `replica-announce-ip {{ env "NOMAD_HOST_IP_kvrocks" }}` and
     `replica-announce-port {{ env "NOMAD_HOST_PORT_kvrocks" }}`;
   - check that `bind` does **not** include 127.0.1.1.
2. kvlock task, add:
   - `--kvrocks-port ${NOMAD_PORT_kvrocks}`
   - `--advertise-nomad-label kvrocks`, and drop the static
     `--advertise-port`
   - `--forward`, `--forward-addr 127.0.1.1:18379`, `--forward-hold 5s`
   - optional: `--peer-expose`
3. Leader proxy (if used by the loadtest or clients): keep it on 8379 on
   each host. It is a fixed port, so clients still use `host:8379`.
4. `nomad job plan`, then `nomad job run`. Expect a rolling replacement:
   - the replica first, then the leader with handover on shutdown;
   - existing replicas get REPLICAOF to 127.0.1.1:18379 on the first
     follow.
5. Verify:
   - `kvrocks-leader status`:
     - the leader's forwarder shows `none (this node is the leader)`;
     - the replica's forwarder shows `-> <leader host>:<dynport>` with
       active 1 (the replication link);
     - with `--peer-expose`, the peer table lists index 0 and index 1.
   - On the replica: `INFO replication` shows master_host 127.0.1.1,
     master_port 18379, master_link_status up.
   - On the leader: `INFO replication` lists the slave as
     `<replica host IP>,<replica dynport>`, which confirms the announce
     settings work.

## 3. Deeptest (existing hard-kill + handover load)

Run the same loadtest settings as test3. Use the leader proxy or both
dynamic addresses, for example:

```sh
kvrocks-leader loadtest \
  --addr kv01:8379 --addr kv02:8379 --allow :8379 \   # via leader proxy
  --duration 300s --rate 2000 --workers 8 --keys 10000 \
  --status-addr kv01:8080 --status-addr kv02:8080 --handover-at 30s --json
```

Sequence, the same as test3:

1. Planned handover at 30s.
2. Hard kill of the leader's kvrocks (`docker kill`), with `kill -USR1`
   marker.
3. Hard kill of the leader's kvlock sidecar.
4. Handover back.

After each event, watch these:

- `kvlock_proxy_route_resets_total{listener="forward"}` goes up by 1 on
  the surviving replica.
- `kvlock_proxy_refused_connections_total{listener="forward"}` stays at 0.
  Any value above 0 means the hold expired; check how long the leader
  took to appear.
- The replica's `master_link_status` returns to up, using psync. Full
  syncs (`sync_full` on the master) should not increase on handover.

### Success criteria

- **0 lost writes and 0 stale reads** (loadtest exits 0).
- Gaps similar to test3: about 9.9s and 6.7s for the hard kills, and
  about 1.3s and 1.5s for the handovers. A regression of more than 1s per
  event is a fail and needs investigation.
- The forwarder never routes to a fenced node. In kvlock logs every
  `proxy route changed` target must be the epoch's leader. Cross-check it
  with the status timeline.
- No `REPLICAOF` refused errors (the self-check) in kvrocks logs.

## 4. Three-instance case (count = 3)

Only if a third test host is available (kv03). Otherwise, run the
equivalent with three allocs on two hosts using different dynamic ports.

1. Set `count = 3` with `--peer-expose`. Indexes 0, 1 and 2 map to
   127.0.2.0, 127.0.2.1 and 127.0.2.2 on port 18379.
2. Check that the status peer table on every node lists all three, and
   that each node's own index routes to its local kvrocks.
3. Kill the leader. Then check:
   - Both survivors reset their forwarder once.
   - Exactly one survivor promotes; the lag gate sample goes through the
     peer address (visible in debug logs).
   - The other survivor's existing link is cut and reconnects to the new
     leader **without a new REPLICAOF**.
4. Replace one replica alloc with `nomad alloc stop`:
   - Its index is reused, and its peer address re-routes to the new host
     and port.
   - The other peers are unaffected.
5. Same success criteria as section 3.

## 5. Rollback

1. `nomad job revert kvlock-test <previous version>`. This restores the
   static 8379 and removes the forwarder flags. kvlock re-points replicas
   at the leader's real address on the next follow, because the
   master_host comparison sees the change.
2. If a replica stays on 127.0.1.1:18379 (forwarder gone, link down):
   - run `REPLICAOF <leader host> 8379` on it by hand, or restart its
     sidecar;
   - then run `kvrocks-leader status` and wait until it exits 0.
3. Remove the NetBird rule added in step 1.
4. Peer-expose JSON member keys disappear with the old sessions. There is
   no Consul cleanup to do.
