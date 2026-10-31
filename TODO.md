# TODO / known issues

Found while testing v0.3.0-test4 on Nomad (kvrocks 2.9.0, bridge networking,
dynamic kvrocks ports behind `--forward`).

## `fence_pending` stays true after a kvrocks SIGKILL

After the kvrocks process is SIGKILLed, the sidecar logs "fence REPLICAOF
failed; instance may still accept writes" (the instance is down, so the fence
cannot be confirmed) and sets `fence_pending`. `/status` then keeps showing
`fence_pending: true` ("fence pending (retrying)") even after kvrocks is back
and the node is a healthy replica or the leader. It cleared only after the next
handover (the only path that runs a confirmed fence and resets it).

- Impact: cosmetic in the test. Routing, election and write safety were not
  affected. The status page shows it as an error, which can mislead.
- To do: clear `fencePending` when kvrocks comes back and the node is verified
  to be a read-only replica of the Consul-confirmed leader (or is the leader
  holding the lock), instead of waiting for another fence. Add a controller
  test for kill, restart, then status.

## Forwarder keeps a stale target after the leader's kvrocks is SIGKILLed

When the leader's kvrocks is SIGKILLed but its sidecar keeps running, the
forwarder on the other node keeps pointing at the dead leader's `host:port`
for about 9 s, until the lock changes hands. It clears at once when the lock is
released or handed over, so planned handovers are not affected.

- Impact: a replica trying to (re)sync in that window connects to a dead
  address and retries. It is a stale target, not a wrong-master risk, because
  the route is cut and re-pointed on the leader, session or epoch change.
- To do: drop or hold the forwarder route as soon as the leader's own health
  probe fails (the leader knows its kvrocks is down) rather than waiting for the
  session to be released.
