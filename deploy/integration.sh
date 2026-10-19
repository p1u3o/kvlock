#!/usr/bin/env bash
# Integration scenarios for kvrocks-leader. Requires docker compose.
# Never touches production: test ports 18379/18380 + Consul 18500.
set -euo pipefail

CONSUL=http://127.0.0.1:18500
KEY="kvrocks/test/leader"

need() { command -v "$1" >/dev/null || { echo "missing $1" >&2; exit 2; }; }
need docker; need curl; need redis-cli

export COMPOSE="docker compose -f deploy/docker-compose.yml"

leader_now() {
  curl -s "$CONSUL/v1/kv/$KEY?raw" || true
}

wait_leader() {
  local timeout="${1:-60}"
  for _ in $(seq 1 "$timeout"); do
    v="$(leader_now)"
    if [[ -n "$v" && "$v" != "null" ]]; then echo "$v"; return 0; fi
    sleep 1
  done
  echo "no leader within ${timeout}s" >&2
  return 1
}

scenario_boot() {
  echo "== boot: both start at once =="
  $COMPOSE up -d --build
  v="$(wait_leader 90)"
  echo "leader: $v"
  count="$(curl -s "$CONSUL/v1/health/service/kvrocks?tag=master" | grep -o '"ID"' | wc -l)"
  if [[ "$count" != "1" ]]; then echo "expected exactly 1 master service, got $count" >&2; return 1; fi
  echo "PASS boot"
}

scenario_sigterm() {
  echo "== SIGTERM the leader sidecar =="
  before="$(wait_leader 30)"
  node="$(echo "$before" | grep -o '"node":"[^"]*"' | head -1)"
  echo "leader before: $before"
  if echo "$node" | grep -q kv01; then svc=sidecar-a; else svc=sidecar-b; fi
  docker compose -f deploy/docker-compose.yml kill -s SIGTERM "$svc"
  sleep 12  # TTL 10s + margin
  after="$(wait_leader 60)"
  echo "leader after: $after"
  if [[ "$after" == "$before" ]]; then echo "leader did not move" >&2; return 1; fi
  echo "PASS sigterm"
}

scenario_kill_master_kvrocks() {
  echo "== kill master's kvrocks =="
  before="$(wait_leader 30)"
  echo "leader before: $before"
  if echo "$before" | grep -q kv01; then kvc=kvrocks-a; else kvc=kvrocks-b; fi
  docker compose -f deploy/docker-compose.yml stop "$kvc"
  sleep 15
  after="$(wait_leader 60)"
  echo "leader after: $after"
  if [[ "$after" == "$before" ]]; then echo "no failover" >&2; return 1; fi
  docker compose -f deploy/docker-compose.yml start "$kvc"
  sleep 10
  echo "PASS kill-kvrocks"
}

scenario_partition_master_consul() {
  echo "== partition master from Consul (iptables DROP 8500) =="
  before="$(wait_leader 30)"
  echo "leader before: $before (manual step if iptables unavailable)"
  echo "SKIP (requires pause/unpause support in this environment)"
}

cmd="${1:-all}"
case "$cmd" in
  boot) scenario_boot ;;
  sigterm) scenario_sigterm ;;
  kill-kvrocks) scenario_kill_master_kvrocks ;;
  partition) scenario_partition_master_consul ;;
  all) scenario_boot; scenario_sigterm; scenario_kill_master_kvrocks; scenario_partition_master_consul ;;
  down) $COMPOSE down -v ;;
  *) echo "usage: $0 [boot|sigterm|kill-kvrocks|partition|all|down]" >&2; exit 2 ;;
esac
