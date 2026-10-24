package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/p1u3o/kvlock/internal/backup"
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
	// LagSampleMaxAge: a leader offset sample older than this when the
	// leader disappeared is treated as unknown lag. 0 disables.
	LagSampleMaxAge time.Duration
	// LagGateMaxWait: promote a lagging replica anyway once the old leader
	// has been gone this long. 0 = refuse until it returns.
	LagGateMaxWait time.Duration
	// MaxLagTime is the heartbeat lag (time) a replica may be behind and
	// still promote. The time gate is used whenever the departed leader's
	// heartbeat was sampled; otherwise the offset gate (MaxLagBytes) applies.
	MaxLagTime time.Duration
	// HeartbeatInterval is how often the leader writes its heartbeat key
	// and replicas read it. 0 disables heartbeats (offset gate only).
	HeartbeatInterval time.Duration

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

	// WatchdogFile, when set, is the heartbeat file the sidecar refreshes
	// every WatchdogInterval for `kvrocks-leader watchdog`.
	WatchdogFile     string
	WatchdogInterval time.Duration

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
	// ShutdownHandover bounds the planned handover a leader attempts on
	// SIGTERM before falling back to fence-then-release; 0 disables it.
	ShutdownHandover time.Duration

	CheckInterval time.Duration

	Backup Backup
}

// Backup configures the optional S3 backups. Secrets are resolved into
// Key/AccessKey/SecretKey by Parse and never printed.
type Backup struct {
	Enabled       bool
	Interval      time.Duration
	AllowLeader   bool
	LeaderGrace   time.Duration
	Dir           string
	BGSaveTimeout time.Duration
	Keep          int

	KeyFile string
	Key     []byte // 32 bytes

	Endpoint      string
	Region        string
	Bucket        string
	Prefix        string
	Insecure      bool
	SSE           string // "AES256" or "none"
	AccessKey     string
	AccessKeyFile string
	SecretKey     string
	SecretKeyFile string
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

	fs.Int64Var(&c.MaxLagBytes, "max-lag", getenvInt64("MAX_LAG", getenvInt64("MAX_LAG_BYTES", -1)), "Max replication lag tolerated before refusing promotion, in the backend's offset units: write sequence numbers for kvrocks (default 1000), bytes for Redis (default 100MiB) ($MAX_LAG)")
	fs.Int64Var(&c.MaxLagBytes, "max-lag-bytes", c.MaxLagBytes, "Old name for --max-lag ($MAX_LAG_BYTES)")
	fs.DurationVar(&c.LagSampleMaxAge, "lag-sample-max-age", getenvDur("LAG_SAMPLE_MAX_AGE", 0), "Treat the leader offset sample as unknown lag if it was older than this when the leader disappeared; 0 = 3x --check-interval ($LAG_SAMPLE_MAX_AGE)")
	fs.DurationVar(&c.LagGateMaxWait, "lag-gate-max-wait", getenvDur("LAG_GATE_MAX_WAIT", 5*time.Minute), "Promote a replica that is over --max-lag anyway once the old leader has been gone this long, accepting the loss of its unreplicated writes; 0 = wait forever ($LAG_GATE_MAX_WAIT)")
	fs.DurationVar(&c.MaxLagTime, "max-lag-time", getenvDur("MAX_LAG_TIME", 2*time.Second), "Max heartbeat lag (time behind the leader) tolerated before refusing promotion ($MAX_LAG_TIME)")
	fs.DurationVar(&c.HeartbeatInterval, "heartbeat-interval", getenvDur("HEARTBEAT_INTERVAL", 250*time.Millisecond), "Leader writes the heartbeat key kvlock:hb:<cluster> this often; replicas read it at the same rate; 0 disables ($HEARTBEAT_INTERVAL)")
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

	fs.StringVar(&c.WatchdogFile, "watchdog-file", getenv("KVLOCK_WATCHDOG_FILE", ""), "Heartbeat file for the watchdog, e.g. ${NOMAD_ALLOC_DIR}/kvlock/heartbeat; empty disables ($KVLOCK_WATCHDOG_FILE)")
	fs.DurationVar(&c.WatchdogInterval, "watchdog-interval", getenvDur("KVLOCK_WATCHDOG_INTERVAL", time.Second), "How often the heartbeat file is refreshed ($KVLOCK_WATCHDOG_INTERVAL)")
	fs.StringVar(&c.HTTPAddr, "http-addr", getenv("HTTP_ADDR", ":8080"), "Health/metrics listen address ($HTTP_ADDR)")
	fs.StringVar(&c.AdminToken, "admin-token", getenv("KVLOCK_ADMIN_TOKEN", ""), "Bearer token for admin endpoints; empty = loopback clients only ($KVLOCK_ADMIN_TOKEN)")
	fs.StringVar(&c.AdminTokenFile, "admin-token-file", getenv("KVLOCK_ADMIN_TOKEN_FILE", ""), "File containing the admin token ($KVLOCK_ADMIN_TOKEN_FILE)")
	fs.DurationVar(&c.HandoverTimeout, "handover-timeout", getenvDur("HANDOVER_TIMEOUT", 30*time.Second), "Planned handover: max wait for a replica to catch up ($HANDOVER_TIMEOUT)")
	fs.Int64Var(&c.HandoverMaxLag, "handover-max-lag", getenvInt64("HANDOVER_MAX_LAG", -1), "Planned handover lag bound; -1 = --max-lag-bytes ($HANDOVER_MAX_LAG)")
	fs.DurationVar(&c.ShutdownHandover, "shutdown-handover-timeout", getenvDur("SHUTDOWN_HANDOVER_TIMEOUT", 5*time.Second), "On shutdown as leader, hand over to a caught-up replica within this time before fencing and releasing; 0 = off ($SHUTDOWN_HANDOVER_TIMEOUT)")
	fs.DurationVar(&c.CheckInterval, "check-interval", getenvDur("CHECK_INTERVAL", 3*time.Second), "kvrocks health check interval ($CHECK_INTERVAL)")

	b := &c.Backup
	fs.BoolVar(&b.Enabled, "backup", getenvBool("KVLOCK_BACKUP", false), "Enable encrypted S3 backups from a healthy replica ($KVLOCK_BACKUP)")
	fs.DurationVar(&b.Interval, "backup-interval", getenvDur("KVLOCK_BACKUP_INTERVAL", 6*time.Hour), "Time between cluster backups ($KVLOCK_BACKUP_INTERVAL)")
	fs.BoolVar(&b.AllowLeader, "backup-allow-leader", getenvBool("KVLOCK_BACKUP_ALLOW_LEADER", false), "Let the leader back up when it has no replica or replicas missed --backup-interval + --backup-leader-grace ($KVLOCK_BACKUP_ALLOW_LEADER)")
	fs.DurationVar(&b.LeaderGrace, "backup-leader-grace", getenvDur("KVLOCK_BACKUP_LEADER_GRACE", time.Hour), "Extra time the leader waits for a replica backup ($KVLOCK_BACKUP_LEADER_GRACE)")
	fs.StringVar(&b.Dir, "backup-dir", getenv("KVLOCK_BACKUP_DIR", ""), "kvrocks backup-dir (where BGSAVE writes its checkpoint) as mounted in this container ($KVLOCK_BACKUP_DIR)")
	fs.DurationVar(&b.BGSaveTimeout, "backup-bgsave-timeout", getenvDur("KVLOCK_BACKUP_BGSAVE_TIMEOUT", 30*time.Minute), "Max wait for BGSAVE to finish ($KVLOCK_BACKUP_BGSAVE_TIMEOUT)")
	fs.IntVar(&b.Keep, "backup-keep", getenvInt("KVLOCK_BACKUP_KEEP", 0), "Keep only the newest N backups of this cluster; 0 = keep all ($KVLOCK_BACKUP_KEEP)")
	b.RegisterStoreFlags(fs)

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

	if c.WatchdogFile != "" && c.WatchdogInterval <= 0 {
		return nil, fmt.Errorf("--watchdog-interval must be > 0")
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
	if c.MaxLagBytes < 0 {
		// kvrocks offsets count writes, not bytes; a byte-sized default
		// would let a replica that is missing almost everything promote.
		c.MaxLagBytes = 1000
		if c.KVKind == "redis" {
			c.MaxLagBytes = 104857600
		}
	}
	if c.LagSampleMaxAge <= 0 {
		c.LagSampleMaxAge = 3 * c.CheckInterval
	}
	if c.LagGateMaxWait < 0 {
		return nil, fmt.Errorf("--lag-gate-max-wait must be >= 0")
	}
	if c.MaxLagTime <= 0 {
		return nil, fmt.Errorf("--max-lag-time must be > 0")
	}
	if c.HeartbeatInterval < 0 {
		return nil, fmt.Errorf("--heartbeat-interval must be >= 0")
	}
	if c.HeartbeatInterval > 0 && c.HeartbeatInterval < 50*time.Millisecond {
		return nil, fmt.Errorf("--heartbeat-interval %v too short: minimum 50ms", c.HeartbeatInterval)
	}
	if c.HandoverMaxLag < 0 {
		c.HandoverMaxLag = c.MaxLagBytes
	}
	if c.ShutdownHandover < 0 {
		return nil, fmt.Errorf("invalid --shutdown-handover-timeout %v", c.ShutdownHandover)
	}
	if c.HandoverTimeout <= 0 {
		return nil, fmt.Errorf("invalid --handover-timeout %v", c.HandoverTimeout)
	}
	if c.SessionTTL < 5*time.Second {
		return nil, fmt.Errorf("session TTL %v too short: minimum 5s", c.SessionTTL)
	}
	if c.Backup.Enabled {
		if err := c.Backup.resolve(); err != nil {
			return nil, err
		}
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

// EpochKey is where the monotonic epoch counter lives, next to the
// leader key.
func (c *Config) EpochKey() string {
	k := c.LeaderKey() // always ends in "leader"
	return k[:len(k)-len("leader")] + "epoch"
}

// HeartbeatKey is the key the leader writes into its kvrocks so replicas
// can measure lag in time. It replicates like any other write.
func (c *Config) HeartbeatKey() string { return "kvlock:hb:" + c.Cluster }

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

// resolve validates the backup settings and loads secrets. It is also
// used by the restore subcommand (which does not need Dir).
func (b *Backup) resolve() error {
	if err := b.ResolveStore(); err != nil {
		return err
	}
	if b.Dir == "" {
		return fmt.Errorf("--backup-dir is required with --backup")
	}
	if b.Interval < time.Minute {
		return fmt.Errorf("--backup-interval %v too short: minimum 1m", b.Interval)
	}
	if b.Keep < 0 || b.LeaderGrace < 0 || b.BGSaveTimeout <= 0 {
		return fmt.Errorf("invalid --backup-keep/--backup-leader-grace/--backup-bgsave-timeout")
	}
	return nil
}

// ResolveStore validates S3 settings and loads the encryption key and S3
// credentials from files or the environment.
func (b *Backup) ResolveStore() error {
	if b.Endpoint == "" || b.Bucket == "" {
		return fmt.Errorf("--backup-s3-endpoint and --backup-s3-bucket are required")
	}
	if strings.Contains(b.Endpoint, "://") {
		return fmt.Errorf("--backup-s3-endpoint must be host[:port] without a scheme (use --backup-s3-insecure for http)")
	}
	switch strings.ToUpper(b.SSE) {
	case "AES256":
		b.SSE = "AES256"
	case "NONE", "":
		b.SSE = "none"
	default:
		return fmt.Errorf("invalid --backup-s3-sse %q: want AES256|none", b.SSE)
	}
	if b.Prefix != "" && !strings.HasSuffix(b.Prefix, "/") {
		b.Prefix += "/"
	}
	var err error
	if b.Key, err = secret(b.KeyFile, "KVLOCK_BACKUP_KEY", "backup key", false); err != nil {
		return err
	}
	if b.Key == nil {
		return fmt.Errorf("a backup encryption key is required (--backup-key-file or $KVLOCK_BACKUP_KEY)")
	}
	if b.Key, err = backup.ParseKey(b.Key); err != nil {
		return err
	}
	ak, err := secret(b.AccessKeyFile, "AWS_ACCESS_KEY_ID", "S3 access key", true)
	if err != nil {
		return err
	}
	sk, err := secret(b.SecretKeyFile, "AWS_SECRET_ACCESS_KEY", "S3 secret key", true)
	if err != nil {
		return err
	}
	if len(ak) == 0 || len(sk) == 0 {
		return fmt.Errorf("S3 credentials are required (--backup-s3-*-key-file or $AWS_ACCESS_KEY_ID/$AWS_SECRET_ACCESS_KEY)")
	}
	b.AccessKey, b.SecretKey = string(ak), string(sk)
	return nil
}

// secret reads file (trimmed unless raw-capable) or falls back to env.
func secret(file, env, what string, trim bool) ([]byte, error) {
	if file != "" {
		v, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read %s file: %w", what, err)
		}
		if trim || len(v) != 32 {
			v = bytesTrim(v)
		}
		return v, nil
	}
	if v := os.Getenv(env); v != "" {
		return []byte(strings.TrimSpace(v)), nil
	}
	return nil, nil
}

// RegisterStoreFlags registers the S3 and encryption-key flags shared by
// the sidecar and the restore subcommand.
func (b *Backup) RegisterStoreFlags(fs *flag.FlagSet) {
	fs.StringVar(&b.KeyFile, "backup-key-file", getenv("KVLOCK_BACKUP_KEY_FILE", ""), "File with the 32-byte encryption key (raw, hex or base64); or set $KVLOCK_BACKUP_KEY ($KVLOCK_BACKUP_KEY_FILE)")
	fs.StringVar(&b.Endpoint, "backup-s3-endpoint", getenv("KVLOCK_BACKUP_S3_ENDPOINT", ""), "S3 endpoint host[:port], e.g. s3.fr-par.scw.cloud ($KVLOCK_BACKUP_S3_ENDPOINT)")
	fs.StringVar(&b.Region, "backup-s3-region", getenv("KVLOCK_BACKUP_S3_REGION", getenv("AWS_REGION", "")), "S3 region ($KVLOCK_BACKUP_S3_REGION, $AWS_REGION)")
	fs.StringVar(&b.Bucket, "backup-s3-bucket", getenv("KVLOCK_BACKUP_S3_BUCKET", ""), "S3 bucket ($KVLOCK_BACKUP_S3_BUCKET)")
	fs.StringVar(&b.Prefix, "backup-s3-prefix", getenv("KVLOCK_BACKUP_S3_PREFIX", "kvlock/"), "Object key prefix ($KVLOCK_BACKUP_S3_PREFIX)")
	fs.BoolVar(&b.Insecure, "backup-s3-insecure", getenvBool("KVLOCK_BACKUP_S3_INSECURE", false), "Use plain HTTP to the endpoint ($KVLOCK_BACKUP_S3_INSECURE)")
	fs.StringVar(&b.SSE, "backup-s3-sse", getenv("KVLOCK_BACKUP_S3_SSE", "AES256"), "Server-side encryption to request: AES256|none ($KVLOCK_BACKUP_S3_SSE)")
	fs.StringVar(&b.AccessKeyFile, "backup-s3-access-key-file", getenv("KVLOCK_BACKUP_S3_ACCESS_KEY_FILE", ""), "File with the S3 access key; or set $AWS_ACCESS_KEY_ID ($KVLOCK_BACKUP_S3_ACCESS_KEY_FILE)")
	fs.StringVar(&b.SecretKeyFile, "backup-s3-secret-key-file", getenv("KVLOCK_BACKUP_S3_SECRET_KEY_FILE", ""), "File with the S3 secret key; or set $AWS_SECRET_ACCESS_KEY ($KVLOCK_BACKUP_S3_SECRET_KEY_FILE)")
}
