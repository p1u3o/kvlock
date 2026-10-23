# Example Nomad job: kvrocks 2.11 + kvrocks-leader sidecar on two hosts.
#
# One group per host (kv01, kv02) so each kvrocks/sidecar pair is pinned to
# its own machine and they can never land together. Host networking with
# static ports: kvrocks 6666, sidecar HTTP 8080 (bound to the host IP so
# Nomad's check and Prometheus can reach it).
#
# Secrets come from Vault templates into secrets/. The kvrocks task gets
# only the kvrocks password; only the sidecar gets the Consul token.
# Failover does not depend on Nomad checks: the sidecar keeps its own Consul
# TTL check fresh.

variable "sidecar_image" {
  type        = string
  description = "kvrocks-leader image built from this repo's Dockerfile, pinned by tag or digest"
  # No default on purpose: set it with -var sidecar_image=...
}

variable "kvrocks_image" {
  type    = string
  default = "apache/kvrocks:2.11.0"
}

locals {
  nodes = ["kv01", "kv02"]
}

job "kvrocks" {
  datacenters = ["dc1"]
  type        = "service"

  dynamic "group" {
    for_each = local.nodes
    labels   = [group.value]

    content {
      count = 1

      constraint {
        attribute = "${node.unique.name}"
        value     = group.value
      }

      network {
        mode = "host"
        port "kvrocks" {
          static = 6666
        }
        port "http" {
          static = 8080
        }
      }

      task "kvrocks" {
        driver = "docker"

        config {
          image        = var.kvrocks_image
          network_mode = "host"
          ports        = ["kvrocks"]
          # Replace the image's default config path with ours; keep its
          # data dir so the volume below holds the data.
          entrypoint = ["kvrocks"]
          args = [
            "-c", "/secrets/kvrocks.conf",
            "--dir", "/var/lib/kvrocks",
          ]
          volumes = ["/srv/kvrocks:/var/lib/kvrocks"]
        }

        template {
          data        = <<EOH
bind {{ env "NOMAD_IP_kvrocks" }} 127.0.0.1
port {{ env "NOMAD_PORT_kvrocks" }}
daemonize no
# Partial resync after failover.
use-rsid-psync yes
slave-read-only yes
requirepass {{ with secret "kv/data/kvrocks" }}{{ .Data.data.password }}{{ end }}
masterauth {{ with secret "kv/data/kvrocks" }}{{ .Data.data.password }}{{ end }}
EOH
          destination = "secrets/kvrocks.conf"
          perms       = "0600"
          change_mode = "restart"
        }

        resources {
          cpu    = 1000
          memory = 1024
        }
      }

      task "kvrocks-leader" {
        driver = "docker"

        lifecycle {
          hook    = "prestart"
          sidecar = true
        }

        config {
          image        = var.sidecar_image
          network_mode = "host"
          ports        = ["http"]
          args = [
            "--consul-addr", "127.0.0.1:8500",
            "--kvrocks-addr", "127.0.0.1",
            "--kvrocks-port", "${NOMAD_PORT_kvrocks}",
            "--node", "${node.unique.name}",
            "--advertise-addr", "${NOMAD_IP_kvrocks}",
            # Optional NetBird address. Set client meta "netbird_ip" on both
            # hosts, then uncomment (an unset meta is NOT interpolated away).
            # "--advertise-addr-private", "${meta.netbird_ip}",
            "--advertise-port", "${NOMAD_PORT_kvrocks}",
            "--cluster", "main",
            "--service-name", "kvrocks",
            "--http-addr", "${NOMAD_ADDR_http}",
          ]
        }

        template {
          data        = <<EOH
KVROCKS_PASSWORD={{ with secret "kv/data/kvrocks" }}{{ .Data.data.password }}{{ end }}
CONSUL_HTTP_TOKEN={{ with secret (printf "kv/data/consul/kvrocks-leader-%s" (env "node.unique.name")) }}{{ .Data.data.token }}{{ end }}
EOH
          destination = "secrets/sidecar.env"
          env         = true
          perms       = "0600"
          change_mode = "restart"
        }

        resources {
          cpu    = 200
          memory = 128
        }
      }

      # Sidecar health for Nomad/ops visibility only. Clients find the
      # master via Consul (tag "master" on service "kvrocks"), not this.
      service {
        name     = "kvrocks-sidecar"
        port     = "http"
        tags     = ["http", "metrics", group.value]
        provider = "consul"
        check {
          name     = "sidecar-healthz"
          type     = "http"
          path     = "/healthz"
          interval = "10s"
          timeout  = "3s"
        }
      }
    }
  }
}
