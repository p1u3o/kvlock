# Example only (not used by any deployment): add the watchdog to an existing
# kvlock group next to the kvrocks and sidecar tasks. Values match the
# kvlock-test layout (kvrocks on 8379); adjust for other clusters.

# 1. Sidecar task: add the heartbeat file to its args.
#      "--watchdog-file", "${NOMAD_ALLOC_DIR}/kvlock/heartbeat",
#      "--watchdog-interval", "1s",

# 2. New task in the same group (shares the alloc dir with the sidecar).
task "kvlock-watchdog" {
  driver = "exec" # or "docker" with the same kvlock image

  lifecycle {
    hook    = "prestart"
    sidecar = true
  }

  config {
    command = "local/kvrocks-leader"
    args = [
      "watchdog",
      "--heartbeat-file", "${NOMAD_ALLOC_DIR}/kvlock/heartbeat",
      "--kvrocks-addr", "127.0.0.1",
      "--kvrocks-port", "8379",
      "--timeout", "5s",          # keep below the sidecar --session-ttl (10s)
      "--interval", "1s",
      "--startup-grace", "60s",
      "--kill-clients",
      "--mode", "active",         # "observe" to only log/count would-fence events
      "--http-addr", "0.0.0.0:${NOMAD_PORT_watchdog}",
      "--node", "${node.unique.name}",
    ]
  }

  template {
    destination = "secrets/kvrocks.env"
    env         = true
    data        = <<-EOT
      KVROCKS_PASSWORD={{ with nomadVar "nomad/jobs/kvlock-test" }}{{ .kvrocks_password }}{{ end }}
    EOT
  }

  resources {
    cpu    = 50
    memory = 32
  }
}

# 3. Group network: expose a port for Prometheus.
#   network { port "watchdog" { to = 9101 } }
# and a service for scraping:
#   service { name = "kvlock-watchdog" port = "watchdog" tags = ["metrics"] }
