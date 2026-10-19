# Least-privilege Consul ACL for kvrocks-leader sidecars.
# Install: consul acl policy create -name kvrocks-leader -rules @deploy/consul-policy.hcl
# Token per node (kv01/kv02) with node_identity or legacy node write below.

# Sessions: create/renew/destroy sessions bound to this node.
session_prefix "" {
  policy = "write"
}
# Equivalent scoped form (preferred on Consul >= 1.11 with node identities):
# session "kv01" { policy = "write" }

# Leader lock (active) plus observe prefix used in --mode=observe.
key_prefix "kvrocks/" {
  policy = "write"
}

# Service catalog: register kvrocks with master/replica tags + TTL checks,
# read the service for discovery.
service_prefix "kvrocks" {
  policy = "write"
}

# Required to register checks tied to the agent.
agent_prefix "" {
  policy = "write"
}
node_prefix "" {
  policy = "read"
}
