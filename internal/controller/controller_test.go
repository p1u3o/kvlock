package controller_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
)

// --- fakes ---

type fakeSession struct {
	mu        sync.Mutex
	id        string
	renewErr  error
	valid     bool
	destroyed bool
}

func (f *fakeSession) Create(ctx context.Context, ttl, ld time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.id = "sess-1"
	f.valid = true
	return f.id, nil
}
func (f *fakeSession) Renew(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.renewErr != nil {
		return f.renewErr
	}
	if !f.valid {
		return errInvalid
	}
	return nil
}
func (f *fakeSession) Destroy(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyed = true
	f.valid = false
	return nil
}
func (f *fakeSession) Valid(ctx context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.valid && id == f.id, nil
}

type errString string

func (e errString) Error() string { return string(e) }

const errInvalid = errString("invalid session")

type fakeLock struct {
	mu        sync.Mutex
	val       []byte
	sess      string
	idx       uint64
	acquires  int
	getErr    error
	epoch     uint64 // separate counter key, as in live.BumpEpoch
	onRelease func()
}

func (f *fakeLock) BumpEpoch(ctx context.Context, key string, min uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch++
	if f.epoch < min {
		f.epoch = min
	}
	return f.epoch, nil
}

func (f *fakeLock) Acquire(ctx context.Context, key, sess string, v []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquires++
	if f.sess != "" && f.sess != sess {
		return false, nil
	}
	f.val = v
	f.sess = sess
	f.idx++
	return true, nil
}
func (f *fakeLock) Release(ctx context.Context, key, sess string) error {
	if f.onRelease != nil {
		f.onRelease()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Like Consul's release op: drop the session, keep the value.
	if f.sess == sess {
		f.sess = ""
		f.idx++
	}
	return nil
}
func (f *fakeLock) Get(ctx context.Context, key string) ([]byte, string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, "", 0, f.getErr
	}
	return f.val, f.sess, f.idx, nil
}
func (f *fakeLock) Watch(ctx context.Context, key string, wi uint64, wt time.Duration) ([]byte, string, uint64, error) {
	<-ctx.Done()
	return nil, "", wi, ctx.Err()
}

type fakeRegistrar struct {
	mu              sync.Mutex
	master, replica int
	dereg           int
	failed, passed  int
}

func (f *fakeRegistrar) RegisterMaster(ctx context.Context, s, n, a string, p int, t []string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.master++
	return nil
}
func (f *fakeRegistrar) RegisterReplica(ctx context.Context, s, n, a string, p int, t []string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replica++
	return nil
}
func (f *fakeRegistrar) PassTTL(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passed++
	return nil
}
func (f *fakeRegistrar) FailTTL(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed++
	return nil
}
func (f *fakeRegistrar) Deregister(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dereg++
	return nil
}
func (f *fakeRegistrar) CheckID(id string) string { return "service:" + id }

type fakeKV struct {
	mu         sync.Mutex
	up         bool
	role       string
	masterHost string
	masterPort int
	lag        int64
	promotions int
	follows    int
	readonly   bool
	local      int64
	peerOffset int64
	peerErr    error

	kills             int
	killedWhileMaster bool
	replicas          []kvrocks.ReplicaState
	failReplicaOf     bool

	downErr   error // returned while !up (default "down")
	hang      bool  // InfoReplication blocks until the context ends
	rsidCalls int
	rsidOK    int
	setRO     int
}

func (f *fakeKV) downError() error {
	if f.downErr != nil {
		return f.downErr
	}
	return errString("down")
}

func (f *fakeKV) Ping(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.up {
		return f.downError()
	}
	return nil
}
func (f *fakeKV) InfoReplication(ctx context.Context) (*kvrocks.ReplicationInfo, error) {
	f.mu.Lock()
	if f.hang {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer f.mu.Unlock()
	if !f.up {
		return nil, f.downError()
	}
	ri := &kvrocks.ReplicationInfo{Role: f.role, MasterHost: f.masterHost, MasterPort: f.masterPort, LagBytes: f.lag, LocalOffset: f.local, MasterOffset: -1, MasterLinkUp: true}
	if f.role == "master" {
		ri.MasterOffset = f.local
		ri.Replicas = append([]kvrocks.ReplicaState(nil), f.replicas...)
	}
	return ri, nil
}
func (f *fakeKV) PeerInfoReplication(ctx context.Context, h string, p int) (*kvrocks.ReplicationInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peerErr != nil {
		return nil, f.peerErr
	}
	return &kvrocks.ReplicationInfo{Role: "master", MasterOffset: f.peerOffset, LocalOffset: f.peerOffset}, nil
}
func (f *fakeKV) ReplicaOf(ctx context.Context, h string, p int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReplicaOf {
		return errString("replicaof failed")
	}
	f.follows++
	if p == 0 {
		f.role = "master"
	} else {
		f.role = "slave"
		f.masterHost = h
		f.masterPort = p
	}
	return nil
}
func (f *fakeKV) Promote(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promotions++
	f.role = "master"
	f.readonly = false
	return nil
}
func (f *fakeKV) EnsureRsIDPsync(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rsidCalls++
	if !f.up {
		return f.downError()
	}
	f.rsidOK++
	return nil
}
func (f *fakeKV) SetReadOnly(ctx context.Context, ro bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readonly = ro
	f.setRO++
	return nil
}
func (f *fakeKV) Close() error { return nil }
func (f *fakeKV) KillClients(ctx context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kills++
	// Killing must happen after the instance stopped accepting writes.
	if f.role != "slave" {
		f.killedWhileMaster = true
	}
	return 2, nil
}

func testConfig(node string) *config.Config {
	return &config.Config{
		ConsulAddr: "127.0.0.1:8500", KvrocksAddr: "127.0.0.1", KvrocksPort: 6666,
		KvrocksTimeout: time.Second, NodeName: node, AdvertiseAddr: "10.0.0.1", AdvertisePort: 6666,
		Cluster: "main", SessionTTL: 10 * time.Second, LockDelay: time.Second,
		UnhealthyThreshold: 2, MaxLagBytes: 1000, HoldDown: 50 * time.Millisecond,
		ServiceName: "kvrocks", MasterTag: "master", ReplicaTag: "replica",
		Mode: "active", FenceHost: "127.0.0.1", FencePort: 1,
		ReelectBaseBackoff: time.Millisecond, ReelectMaxBackoff: 5 * time.Millisecond,
		HTTPAddr: ":0", CheckInterval: 20 * time.Millisecond,
		KillClientsOnDemote: true,
		HandoverTimeout:     2 * time.Second, HandoverMaxLag: 1000,
	}
}

func runCtl(t *testing.T, cfg *config.Config, deps controller.Deps, d time.Duration) (*controller.Controller, *fakeLock, *fakeRegistrar, *fakeKV) {
	t.Helper()
	c := controller.New(cfg, deps, logx.New(cfg.NodeName, cfg.Mode), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(d)
	cancel()
	<-done
	return c, deps.Lock.(*fakeLock), deps.Registrar.(*fakeRegistrar), deps.KV.(*fakeKV)
}

func TestSingleNodeBecomesLeader(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", lag: 0}
	deps := controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv}
	c, _, reg, _ := runCtl(t, cfg, deps, 200*time.Millisecond)
	snap := c.Snapshot()
	if snap.Role != elect.RoleLeader {
		t.Fatalf("expected leader, got %v", snap.Role)
	}
	if reg.master == 0 {
		t.Fatal("expected master registration")
	}
}

func TestFollowerPointsAtLeader(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}
	lock := &fakeLock{}
	lv := elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now())
	lock.val = lv
	lock.sess = "sess-x"
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	c, _, _, fkv := runCtl(t, cfg, deps, 200*time.Millisecond)
	if c.Snapshot().Role != elect.RoleFollower {
		t.Fatalf("expected follower, got %v", c.Snapshot().Role)
	}
	fkv.mu.Lock()
	follows := fkv.follows
	host := fkv.masterHost
	fkv.mu.Unlock()
	if follows == 0 || host != "10.0.0.9" {
		t.Fatalf("expected follow of 10.0.0.9, follows=%d host=%q", follows, host)
	}
}

func TestUnhealthyReleasesAndDemotes(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", lag: 0}
	sess := &fakeSession{}
	lock := &fakeLock{}
	reg := &fakeRegistrar{}
	deps := controller.Deps{Session: sess, Lock: lock, Registrar: reg, KV: kv}
	c := controller.New(cfg, deps, logx.New("kv01", "active"), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		cancel()
		<-done
		t.Fatal("precondition: should be leader first")
	}
	kv.mu.Lock()
	kv.up = false
	kv.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("expected demote after sustained unhealthiness")
	}
	reg.mu.Lock()
	failed := reg.failed
	dereg := reg.dereg
	reg.mu.Unlock()
	if failed == 0 || dereg == 0 {
		t.Fatalf("expected fail+deregister on demote, got fail=%d dereg=%d", failed, dereg)
	}
}

func TestPromotionRefusedWhenFarBehind(t *testing.T) {
	// Pure gate: known huge lag + recently-seen old master => refuse.
	dec := elect.ShouldPromote(99999999, 1000, time.Now(), time.Hour, time.Now(), true)
	if dec.Allow {
		t.Fatal("expected refusal for far-behind replica")
	}
	// Fresh cluster (node is master, lag unknown, no old master) elects.
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "master", lag: -1}
	deps := controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv}
	c, _, _, _ := runCtl(t, cfg, deps, 150*time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("fresh cluster should elect a leader")
	}
}

// startCtl runs a controller until the returned stop func is called.
func startCtl(t *testing.T, cfg *config.Config, deps controller.Deps) (*controller.Controller, func()) {
	t.Helper()
	c := controller.New(cfg, deps, logx.New(cfg.NodeName, cfg.Mode), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	return c, func() { cancel(); <-done }
}

func (f *fakeKV) state() (role, host string, port int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.role, f.masterHost, f.masterPort
}

func TestNodeNameAloneDoesNotGrantLeadership(t *testing.T) {
	// The key names kv01 but is held by an old session (e.g. from before a
	// restart). kv01's new sidecar must not treat itself as leader, must not
	// REPLICAOF itself, and must stop being a writable master.
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), sess: "sess-old"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(150 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("must not be leader without holding the lock with our session")
	}
	role, host, port := kv.state()
	if role != "slave" || host != "127.0.0.1" || port != 1 {
		t.Fatalf("expected fence to 127.0.0.1:1, got role=%s %s:%d", role, host, port)
	}
}

func TestVacantKeyWithOldValueIsElectable(t *testing.T) {
	// behavior=release leaves the value behind with no session.
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "master", lag: 0}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now())}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(200 * time.Millisecond)
	stop()
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("expected to take over vacant key, got %v", c.Snapshot().Role)
	}
	if lv := elect.ParseLeader(lock.val); lv == nil || lv.Epoch != 5 || lv.Node != "kv02" {
		t.Fatalf("expected epoch 5 for kv02, got %+v", lv)
	}
}

func TestDemoteFencesByReplicatingNewLeader(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", lag: 0}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(100 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("precondition: leader")
	}
	lock.mu.Lock()
	lock.sess = "sess-kv02"
	lock.val = elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-kv02", 9, time.Now())
	lock.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleFollower {
		t.Fatalf("expected follower, got %v", c.Snapshot().Role)
	}
	role, host, port := kv.state()
	if role != "slave" || host != "10.0.0.2" || port != 6666 {
		t.Fatalf("old master must become replica of new leader, got role=%s %s:%d", role, host, port)
	}
	m := c.Metrics()
	if got := testutil.ToFloat64(m.StepDowns.WithLabelValues(metrics.ReasonLockTaken)); got != 1 {
		t.Fatalf("step_downs_total{reason=lock_taken} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Fencing.WithLabelValues("leader", "ok")); got < 1 {
		t.Fatalf("fencing_events_total{target=leader,result=ok} = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(m.IsLeader); got != 0 {
		t.Fatalf("is_leader = %v after demotion", got)
	}
	if got := testutil.ToFloat64(m.LeaderAcquired); got != 1 {
		t.Fatalf("leader_acquired_total = %v, want 1", got)
	}
}

func TestConsulErrorsToleratedUntilTTL(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.SessionTTL = 400 * time.Millisecond
	kv := &fakeKV{up: true, role: "master", lag: 0}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("precondition: leader")
	}
	lock.mu.Lock()
	lock.getErr = errString("connection refused")
	lock.mu.Unlock()
	time.Sleep(120 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("a brief Consul error must not cause step-down")
	}
	time.Sleep(400 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("expected step-down once ~TTL passed without a successful renew")
	}
	role, host, port := kv.state()
	if role != "slave" || host != "127.0.0.1" || port != 1 {
		t.Fatalf("expected fence while leader unknown, got role=%s %s:%d", role, host, port)
	}
}

func TestLagGateUsesLeaderOffsetSampledBeforeFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		peer, mine int64
		wantLeader bool
	}{
		{"close behind promotes", 1000, 990, true},
		{"far behind refuses", 1_000_000, 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig("kv02")
			cfg.HoldDown = time.Hour // known lag must decide, not the hold-down
			kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: tc.mine, peerOffset: tc.peer}
			lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 2, time.Now()), sess: "sess-kv01"}
			c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
			defer stop()
			time.Sleep(80 * time.Millisecond)
			if got := c.Snapshot().LagBytes; got != tc.peer-tc.mine {
				t.Fatalf("lag while following = %d, want %d", got, tc.peer-tc.mine)
			}
			// Leader dies: session released, peer unreachable.
			kv.mu.Lock()
			kv.peerErr = errString("connection refused")
			kv.mu.Unlock()
			lock.mu.Lock()
			lock.sess = ""
			lock.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			if isLeader := c.Snapshot().Role == elect.RoleLeader; isLeader != tc.wantLeader {
				t.Fatalf("leader=%v want %v (lag %d)", isLeader, tc.wantLeader, c.Snapshot().LagBytes)
			}
		})
	}
}

func TestMinLeaderTimeDefersVoluntaryHandover(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.MinLeaderTime = time.Hour
	kv := &fakeKV{up: true, role: "master"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	kv.mu.Lock()
	kv.up = false
	kv.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("must keep the lock inside min-leader-time")
	}
	if c.Snapshot().Healthy {
		t.Fatal("healthz must report unhealthy while the backend is down")
	}
}

func TestSnapshotHealthy(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	if s := c.Snapshot(); !s.Healthy || !s.ConsulOK || s.BackendRole != "master" {
		t.Fatalf("expected healthy leader snapshot, got %+v", s)
	}
}

func TestFollowerUsesLeaderPrivateAddr(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "203.0.113.9", "100.64.0.9", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	_, _, _, fkv := runCtl(t, cfg, deps, 200*time.Millisecond)
	fkv.mu.Lock()
	host := fkv.masterHost
	fkv.mu.Unlock()
	if host != "100.64.0.9" {
		t.Fatalf("expected replication over private addr, got %q", host)
	}
}

func TestDemotionKillsClientsAfterFencing(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cfg := testConfig("kv01")
		cfg.KillClientsOnDemote = enabled
		kv := &fakeKV{up: true, role: "master"}
		lock := &fakeLock{}
		c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
		time.Sleep(80 * time.Millisecond)
		if c.Snapshot().Role != elect.RoleLeader {
			stop()
			t.Fatal("precondition: leader")
		}
		lock.mu.Lock()
		lock.sess = "sess-kv02"
		lock.val = elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-kv02", 9, time.Now())
		lock.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		stop()
		kv.mu.Lock()
		kills, early := kv.kills, kv.killedWhileMaster
		kv.mu.Unlock()
		if enabled && (kills != 1 || early) {
			t.Fatalf("enabled: kills=%d killedWhileMaster=%v, want exactly one kill after fencing", kills, early)
		}
		if !enabled && kills != 0 {
			t.Fatalf("disabled: kills=%d, want 0", kills)
		}
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
