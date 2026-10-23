# Consul ACL policy for ONE kvrocks-leader sidecar (this example: node kv01).
# Create one policy + token per node, replacing "kv01" with that node's
# Consul node name (kv02 for the other one):
#
#   sed 's/kv01/kv02/g' deploy/consul-policy.hcl > /tmp/kv02.hcl
#   consul acl policy create -name kvrocks-leader-kv01 -rules @deploy/consul-policy.hcl
#   consul acl token create -policy-name kvrocks-leader-kv01 -description "kvrocks-leader kv01"
#
# Assumes --cluster=main and --service-name=kvrocks. Adjust the key prefix
# and service names if you change those.

# Leader lock (kvrocks/main/leader) and the observe-mode key
# (kvrocks/main/observe/leader). Nothing else in the KV store.
key_prefix "kvrocks/main/" {
  policy = "write"
}

# Sessions may only be created on this node.
session "kv01" {
  policy = "write"
}

# Catalog entries the sidecar registers via its local agent (with their TTL
# checks). "kvrocks-private" is only needed with --advertise-addr-private.
service "kvrocks" {
  policy = "write"
}
service "kvrocks-private" {
  policy = "write"
}

# Registering a service on the local agent needs read on that node.
node "kv01" {
  policy = "read"
}
