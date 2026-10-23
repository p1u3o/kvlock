package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all sidecar settings. Every field is settable via flag
// and overridable via the listed environment variable.
type Config struct {
	ConsulAddr          string
	ConsulToken         string
	ConsulTokenFile     string
	ConsulTLSCA         string
	ConsulTLSSkipVerify bool

	KvrocksAddr         string
	KvrocksPort         int
	KvrocksPassword     string
	KvrocksPasswordFile string
	KvrocksTimeout      time.Duration
	KVKind              string // "kvrocks" or "redis"

	NodeName      string
	AdvertiseAddr string
	// AdvertiseAddrPrivate is an optional second address (e.g. NetBird).
	AdvertiseAddrPrivate string
	// PrivateServiceName is the catalog name used for the private address.
	PrivateServiceName string
	AdvertisePort      int
	Cluster            string

	SessionTTL         time.Duration
	LockDelay          time.Duration
	UnhealthyThreshold int

	MaxLagBytes int64
	HoldDown    time.Duration

	ServiceName string
	MasterTag   string
	ReplicaTag  string

	Mode string // "active" or "observe"

	// FenceHost/FencePort is the REPLICAOF target used to fence this
	// instance when it must stop accepting writes but the new leader is
	// unknown. It should be an address where nothing listens.
	FenceHost string
	FencePort int

	MinLeaderTime      time.Duration
	ReelectBaseBackoff time.Duration
	ReelectMaxBackoff  time.Duration

	// KillClientsOnDemote closes normal client connections after fencing
	// so clients reconnect and rediscover the master.
	KillClientsOnDemote bool

	HTTPAddr string

	// AdminToken guards admin endpoints (POST /handover). When empty they
	// only accept connections from loopback addresses.
	AdminToken     string
	AdminTokenFile string
	// ProxyAddr enables the TCP leader proxy (e.g. ":6380").
	ProxyAddr string
	// ProxyReadAddr enables the read proxy (local healthy replica).
	ProxyReadAddr string
	// ProxyReadFallback is "leader" or "refuse" when the local replica
	// is not usable.
	ProxyReadFallback string
	// SentinelAddr enables the Sentinel-compatible listener (e.g. ":26379").
	SentinelAddr string
	// SentinelName is the master name clients ask for (default: cluster).
	SentinelName string
	// SentinelPrivate announces the leader's private address when it has one.
	SentinelPrivate bool
	// HandoverTimeout bounds how long a planned handover waits for a
	// replica to catch up; it is also how long the handover hint reserves
	// the lock for the chosen replica.
	HandoverTimeout time.Duration
	// HandoverMaxLag is the lag bound for a planned handover; < 0 means
	// use MaxLagBytes.
	HandoverMaxLag int64

	CheckInterval time.Duration
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getenvDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// Parse reads flags (starting after program name) plus env vars.
// Env vars win over flag defaults but flags win over env when explicitly set.
// Implementation: flag defaults are seeded from env, then flags parsed.
func Parse(args []string) (*Config, error) {
	c := &Config{}
	fs := flag.NewFlagSet("kvrocks-leader", flag.ContinueOnError)

	fs.StringVar(&c.ConsulAddr, "consul-addr", getenv("CONSUL_HTTP_ADDR", "127.0.0.1:8500"), "Consul address ($CONSUL_HTTP_ADDR)")
	fs.StringVar(&c.ConsulToken, "consul-token", getenv("CONSUL_HTTP_TOKEN", ""), "Consul ACL token ($CONSUL_HTTP_TOKEN)")
	fs.StringVar(&c.ConsulTokenFile, "consul-token-file", getenv("CONSUL_TOKEN_FILE", ""), "File containing Consul ACL token ($CONSUL_TOKEN_FILE)")
	fs.StringVar(&c.ConsulTLSCA, "consul-tls-ca", getenv("CONSUL_CACERT", ""), "Consul TLS CA bundle ($CONSUL_CACERT)")
	fs.BoolVar(&c.ConsulTLSSkipVerify, "consul-tls-skip-verify", getenvBool("CONSUL_TLS_SKIP_VERIFY", false), "Skip Consul TLS verification ($CONSUL_TLS_SKIP_VERIFY)")

	fs.StringVar(&c.KvrocksAddr, "kvrocks-addr", getenv("KVROCKS_ADDR", "127.0.0.1"), "Local kvrocks host ($KVROCKS_ADDR)")
	fs.IntVar(&c.KvrocksPort, "kvrocks-port", getenvInt("KVROCKS_PORT", 6666), "Local kvrocks port ($KVROCKS_PORT)")
	fs.StringVar(&c.KvrocksPassword, "kvrocks-password", getenv("KVROCKS_PASSWORD", ""), "kvrocks password ($KVROCKS_PASSWORD)")
	fs.StringVar(&c.KvrocksPasswordFile, "kvrocks-password-file", getenv("KVROCKS_PASSWORD_FILE", ""), "File containing kvrocks password ($KVROCKS_PASSWORD_FILE)")
	fs.DurationVar(&c.KvrocksTimeout, "kvrocks-timeout", getenvDur("KVROCKS_TIMEOUT", 2*time.Second), "Per-command kvrocks timeout ($KVROCKS_TIMEOUT)")
	fs.StringVar(&c.KVKind, "kv-kind", getenv("KV_KIND", "kvrocks"), "Backend: kvrocks|redis ($KV_KIND)")

	fs.StringVar(&c.NodeName, "node", getenv("NODE_NAME", getenv("HOSTNAME", "kv01")), "Local node name ($NODE_NAME)")
	fs.StringVar(&c.AdvertiseAddr, "advertise-addr", getenv("ADVERTISE_ADDR", ""), "Address to advertise as master ($ADVERTISE_ADDR, default: kvrocks addr)")
	fs.StringVar(&c.AdvertiseAddrPrivate, "advertise-addr-private", getenv("ADVERTISE_ADDR_PRIVATE", ""), "Optional private-network address (e.g. NetBird). Registered as a second service and used by replicas for replication ($ADVERTISE_ADDR_PRIVATE)")
	fs.StringVar(&c.PrivateServiceName, "private-service-name", getenv("PRIVATE_SERVICE_NAME", ""), "Consul service name for the private address ($PRIVATE_SERVICE_NAME, default: <service-name>-private)")
	fs.IntVar(&c.AdvertisePort, "advertise-port", getenvInt("ADVERTISE_PORT", 0), "Port to advertise ($ADVERTISE_PORT, default: kvrocks port)")
	fs.StringVar(&c.Cluster, "cluster", getenv("CLUSTER_NAME", "main"), "Cluster name; leader key is kvrocks/<cluster>/leader ($CLUSTER_NAME)")

	fs.DurationVar(&c.SessionTTL, "session-ttl", getenvDur("SESSION_TTL", 10*time.Second), "Consul session TTL ($SESSION_TTL)")
	fs.DurationVar(&c.LockDelay, "lock-delay", getenvDur("LOCK_DELAY", 1*time.Second), "Consul lock delay ($LOCK_DELAY)")
	fs.IntVar(&c.UnhealthyThreshold, "unhealthy-threshold", getenvInt("UNHEALTHY_THRESHOLD", 3), "Consecutive failed kvrocks checks before releasing lock ($UNHEALTHY_THRESHOLD)")

	fs.Int64Var(&c.MaxLagBytes, "max-lag-bytes", getenvInt64("MAX_LAG_BYTES", 104857600), "Max replication lag bytes tolerated before refusing promotion ($MAX_LAG_BYTES)")
	fs.DurationVar(&c.HoldDown, "hold-down", getenvDur("HOLD_DOWN", 30*time.Second), "Promote with unknown lag only if old master unreachable this long ($HOLD_DOWN)")

	fs.StringVar(&c.ProxyAddr, "proxy-addr", getenv("PROXY_ADDR", ""), "Listen address for the TCP leader proxy, e.g. :6380 ($PROXY_ADDR, default: disabled)")
	fs.StringVar(&c.ProxyReadAddr, "proxy-read-addr", getenv("PROXY_READ_ADDR", ""), "Listen address for the read proxy to the local replica, e.g. :6381 ($PROXY_READ_ADDR, default: disabled)")
	fs.StringVar(&c.ProxyReadFallback, "proxy-read-fallback", getenv("PROXY_READ_FALLBACK", "leader"), "Read proxy when the local replica is unusable: leader or refuse ($PROXY_READ_FALLBACK)")
	fs.StringVar(&c.SentinelAddr, "sentinel-addr", getenv("SENTINEL_ADDR", ""), "Listen address for the read-only Sentinel-compatible endpoint, e.g. :26379 ($SENTINEL_ADDR, default: disabled)")
	fs.StringVar(&c.SentinelName, "sentinel-name", getenv("SENTINEL_NAME", ""), "Master name served on the Sentinel endpoint ($SENTINEL_NAME, default: --cluster)")
	fs.BoolVar(&c.SentinelPrivate, "sentinel-private", getenvBool("SENTINEL_PRIVATE", false), "Announce private addresses on the Sentinel endpoint ($SENTINEL_PRIVATE)")
	fs.StringVar(&c.ServiceName, "service-name", getenv("SERVICE_NAME", "kvrocks"), "Consul service name ($SERVICE_NAME)")
	fs.StringVar(&c.MasterTag, "master-tag", getenv("MASTER_TAG", "master"), "Tag for the master registration ($MASTER_TAG)")
	fs.StringVar(&c.ReplicaTag, "replica-tag", getenv("REPLICA_TAG", "replica"), "Tag for the replica registration ($REPLICA_TAG)")

	fs.StringVar(&c.Mode, "mode", getenv("MODE", "active"), "Mode: active|observe ($MODE)")
	fs.StringVar(&c.FenceHost, "fence-host", getenv("FENCE_HOST", "127.0.0.1"), "REPLICAOF target used to fence a demoted master when the leader is unknown ($FENCE_HOST)")
	fs.IntVar(&c.FencePort, "fence-port", getenvInt("FENCE_PORT", 1), "Port for --fence-host; must have nothing listening ($FENCE_PORT)")
	fs.DurationVar(&c.MinLeaderTime, "min-leader-time", getenvDur("MIN_LEADER_TIME", 0), "After winning, hold the lock at least this long before voluntarily handing over on local health failures; 0 disables ($MIN_LEADER_TIME)")
	fs.DurationVar(&c.ReelectBaseBackoff, "reelect-base-backoff", getenvDur("REELECT_BASE_BACKOFF", 1*time.Second), "Base backoff between elections ($REELECT_BASE_BACKOFF)")
	fs.DurationVar(&c.ReelectMaxBackoff, "reelect-max-backoff", getenvDur("REELECT_MAX_BACKOFF", 15*time.Second), "Max backoff between elections ($REELECT_MAX_BACKOFF)")

	fs.BoolVar(&c.KillClientsOnDemote, "kill-clients-on-demote", getenvBool("KILL_CLIENTS_ON_DEMOTE", true), "After fencing a demoted master, CLIENT KILL TYPE normal so clients reconnect ($KILL_CLIENTS_ON_DEMOTE)")

	fs.StringVar(&c.HTTPAddr, "http-addr", getenv("HTTP_ADDR", ":8080"), "Health/metrics listen address ($HTTP_ADDR)")
	fs.StringVar(&c.AdminToken, "admin-token", getenv("KVLOCK_ADMIN_TOKEN", ""), "Bearer token for admin endpoints; empty = loopback clients only ($KVLOCK_ADMIN_TOKEN)")
	fs.StringVar(&c.AdminTokenFile, "admin-token-file", getenv("KVLOCK_ADMIN_TOKEN_FILE", ""), "File containing the admin token ($KVLOCK_ADMIN_TOKEN_FILE)")
	fs.DurationVar(&c.HandoverTimeout, "handover-timeout", getenvDur("HANDOVER_TIMEOUT", 30*time.Second), "Planned handover: max wait for a replica to catch up ($HANDOVER_TIMEOUT)")
	fs.Int64Var(&c.HandoverMaxLag, "handover-max-lag", getenvInt64("HANDOVER_MAX_LAG", -1), "Planned handover lag bound; -1 = --max-lag-bytes ($HANDOVER_MAX_LAG)")
	fs.DurationVar(&c.CheckInterval, "check-interval", getenvDur("CHECK_INTERVAL", 3*time.Second), "kvrocks health check interval ($CHECK_INTERVAL)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Load secrets from files (file wins if both set, to favour Vault templates).
	if c.ConsulTokenFile != "" {
		b, err := os.ReadFile(c.ConsulTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read consul token file: %w", err)
		}
		c.ConsulToken = string(bytesTrim(b))
	}
	if c.AdminTokenFile != "" {
		b, err := os.ReadFile(c.AdminTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read admin token file: %w", err)
		}
		c.AdminToken = string(bytesTrim(b))
	}
	if c.KvrocksPasswordFile != "" {
		b, err := os.ReadFile(c.KvrocksPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read kvrocks password file: %w", err)
		}
		c.KvrocksPassword = string(bytesTrim(b))
	}

	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.KvrocksAddr
	}
	if c.ProxyReadFallback != "leader" && c.ProxyReadFallback != "refuse" {
		return nil, fmt.Errorf("invalid --proxy-read-fallback %q (leader|refuse)", c.ProxyReadFallback)
	}
	if c.SentinelName == "" {
		c.SentinelName = c.Cluster
	}
	if c.AdvertiseAddrPrivate != "" && c.PrivateServiceName == "" {
		c.PrivateServiceName = c.ServiceName + "-private"
	}
	if c.AdvertisePort == 0 {
		c.AdvertisePort = c.KvrocksPort
	}
	if c.Mode != "active" && c.Mode != "observe" {
		return nil, fmt.Errorf("invalid --mode %q: want active|observe", c.Mode)
	}
	if c.KVKind != "kvrocks" && c.KVKind != "redis" {
		return nil, fmt.Errorf("invalid --kv-kind %q: want kvrocks|redis", c.KVKind)
	}
	if c.FencePort <= 0 || c.FencePort > 65535 {
		return nil, fmt.Errorf("invalid --fence-port %d", c.FencePort)
	}
	if c.FencePort == c.KvrocksPort && (c.FenceHost == c.KvrocksAddr || c.FenceHost == "127.0.0.1" || c.FenceHost == "localhost") {
		return nil, fmt.Errorf("--fence-host/--fence-port must not point at the local instance")
	}
	if c.HandoverMaxLag < 0 {
		c.HandoverMaxLag = c.MaxLagBytes
	}
	if c.HandoverTimeout <= 0 {
		return nil, fmt.Errorf("invalid --handover-timeout %v", c.HandoverTimeout)
	}
	if c.SessionTTL < 5*time.Second {
		return nil, fmt.Errorf("session TTL %v too short: minimum 5s", c.SessionTTL)
	}
	return c, nil
}

// LeaderKey returns the Consul KV lock key. Observe mode uses a separate
// prefix so it never fights Sentinel or the active sidecars.
func (c *Config) LeaderKey() string {
	if c.Mode == "observe" {
		return "kvrocks/" + c.Cluster + "/observe/leader"
	}
	return "kvrocks/" + c.Cluster + "/leader"
}

func bytesTrim(b []byte) []byte {
	// Trim trailing newline/CR/spaces without importing bytes for one call.
	n := len(b)
	for n > 0 && (b[n-1] == '\n' || b[n-1] == '\r' || b[n-1] == ' ' || b[n-1] == '\t') {
		n--
	}
	i := 0
	for i < n && (b[i] == ' ' || b[i] == '\t') {
		i++
	}
	return b[i:n]
}
