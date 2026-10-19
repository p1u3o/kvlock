# Example Nomad group: kvrocks + kvrocks-leader sidecar.
# Host networking; Consul token via Vault (or workload identity); kvrocks
# password via Vault template. Failover does NOT depend on Nomad service
# checks — the sidecar keeps its own Consul TTL check fresh.
job "kvrocks" {
  datacenters = ["dc1"]
  type        = "service"

  group "kvrocks-pair" {
    network {
      mode = "host"
    }

    # ---- kvrocks ----
    task "kvrocks" {
      driver = "docker"

      config {
        image = "apache/kvrocks:2.7.0"
        args = [
          "/kvrocks/kvrocks.conf",
          "--bind", "0.0.0.0",
          "--port", "6666",
          "--daemonize", "no",
          # Partial resync on failover. Also enforced at runtime by the sidecar.
          "--use-rsid-psync", "yes",
        ]
        network_mode = "host"
      }

      env {
        # Kvrocks port; sidecar defaults match.
        KVROCKS_PORT = "6666"
      }

      template {
        # Vault-provided password shared by kvrocks + sidecar.
        data        = <<EOH
requirepass {{ with secret "kv/data/kvrocks" }}{{ .Data.data.password }}{{ end }}
EOH
        destination = "secrets/kvrocks.conf.include"
        change_mode = "restart"
      }

      template {
        data        = <<EOH
CONSUL_HTTP_TOKEN={{ with secret "kv/data/consul/kvrocks-leader" }}{{ .Data.data.token }}{{ end }}
EOH
        destination = "secrets/consul.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }

    # ---- sidecar ----
    task "kvrocks-leader" {
      driver = "docker"

      lifecycle {
        hook    = "prestart"
        sidecar = true
      }

      config {
        # Built by Dockerfile in this repo.
        image        = "registry.example.com/kvrocks-leader:latest"
        network_mode = "host"
        args = [
          "--consul-addr", "${NOMAD_IP_kvrocks}:8500",
          "--kvrocks-addr", "127.0.0.1",
          "--kvrocks-port", "6666",
          "--node", "${node.unique.name}",
          "--advertise-addr", "${NOMAD_IP_kvrocks}",
          "--advertise-port", "6666",
          "--cluster", "main",
          "--service-name", "kvrocks",
          "--http-addr", "127.0.0.1:8080",
        ]
      }

      template {
        data        = <<EOH
KVROCKS_PASSWORD={{ with secret "kv/data/kvrocks" }}{{ .Data.data.password }}{{ end }}
CONSUL_HTTP_TOKEN={{ with secret "kv/data/consul/kvrocks-leader" }}{{ .Data.data.token }}{{ end }}
EOH
        destination = "secrets/sidecar.env"
        env         = true
        change_mode = "restart"
      }

      resources {
        cpu    = 200
        memory = 128
      }
    }

    # Clients discover the master through Consul DNS / catalog, e.g.
    # master.kvrocks.service.consul filtered by tag "master",
    # NOT through a Nomad service check.
    service {
      name = "kvrocks-sidecar"
      port = "8080"
      tags = ["http", "metrics"]
      check {
        name     = "sidecar-http"
        type     = "http"
        path     = "/healthz"
        interval = "30s"
        timeout  = "5s"
      }
    }
  }
}
