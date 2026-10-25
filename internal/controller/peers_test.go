package controller_test

import (
	"context"
	"testing"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
)

type memberLock struct {
	*fakeLock
	members []string
}

func (p *memberLock) Members(ctx context.Context, prefix string) ([]string, error) {
	return p.members, nil
}

func TestParseMemberBothFormats(t *testing.T) {
	if m := controller.ParseMember("kv01"); m.Node != "kv01" || m.Index != -1 {
		t.Fatalf("bare: %+v", m)
	}
	m := controller.ParseMember(`{"node":"kv02","index":1,"addr":"10.0.0.2","port":23456}`)
	if m.Node != "kv02" || m.Index != 1 || m.Port != 23456 {
		t.Fatalf("json: %+v", m)
	}
}

func threeNodes(self string, idx int) (*controller.Controller, *memberLock) {
	cfg := testConfig(self)
	cfg.PeerExpose, cfg.PeerBase, cfg.PeerPort, cfg.AllocIndex = true, "127.0.2.0", 18379, idx
	pl := &memberLock{fakeLock: &fakeLock{}, members: []string{
		`{"node":"kv01","index":0,"addr":"10.0.0.1","port":21001}`,
		`{"node":"kv02","index":1,"addr":"10.0.0.2","private":"100.64.0.2","port":22002}`,
		`{"node":"kv03","index":2,"addr":"10.0.0.3","port":23003}`,
	}}
	c := controller.New(cfg, controller.Deps{Session: &fakeSession{}, Lock: pl, Registrar: &fakeRegistrar{}, KV: &fakeKV{up: true}}, logx.New(self, "active"), metrics.New())
	return c, pl
}

func TestPeerRoutesThreeInstances(t *testing.T) {
	c, pl := threeNodes("kv01", 0)
	if err := c.RefreshPeers(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := c.PeerIndexes(); len(got) != 3 {
		t.Fatalf("indexes %v", got)
	}
	if a, _, _ := c.PeerRoute(0); a != "127.0.0.1:6666" {
		t.Fatalf("own index should route to local kvrocks, got %q", a)
	}
	if a, _, _ := c.PeerRoute(1); a != "100.64.0.2:22002" {
		t.Fatalf("index 1: %q", a)
	}
	_, k2, _ := c.PeerRoute(2)
	if a, _, _ := c.PeerRoute(2); a != "10.0.0.3:23003" {
		t.Fatalf("index 2: %q", a)
	}
	// Alloc 2 replaced on a new host/port: same index, new key.
	pl.members[2] = `{"node":"kv03","index":2,"addr":"10.0.0.33","port":29999}`
	c.RefreshPeers(context.Background())
	if a, k, _ := c.PeerRoute(2); a != "10.0.0.33:29999" || k == k2 {
		t.Fatalf("replaced: %q %q", a, k)
	}
	// Alloc 2 gone: refuse.
	pl.members = pl.members[:2]
	c.RefreshPeers(context.Background())
	if a, _, _ := c.PeerRoute(2); a != "" {
		t.Fatalf("gone member still routed: %q", a)
	}
}

func TestPeerDuplicateIndexRefused(t *testing.T) {
	c, pl := threeNodes("kv01", 0)
	pl.members = append(pl.members, `{"node":"kv04","index":2,"addr":"10.0.0.4","port":24004}`)
	c.RefreshPeers(context.Background())
	if a, _, _ := c.PeerRoute(2); a != "" {
		t.Fatalf("duplicate index must not route: %q", a)
	}
}

func TestProbeUsesPeerAddress(t *testing.T) {
	c, _ := threeNodes("kv01", 0)
	c.RefreshPeers(context.Background())
	if h, p := controller.ProbeAddrForTest(c, "kv03", "10.0.0.3", 23003); h != "127.0.2.2" || p != 18379 {
		t.Fatalf("probe %s:%d", h, p)
	}
	// Unknown node falls back to its real address.
	if h, p := controller.ProbeAddrForTest(c, "kv09", "10.0.0.9", 1); h != "10.0.0.9" || p != 1 {
		t.Fatalf("fallback %s:%d", h, p)
	}
}

// livePeer must not mistake our own JSON member value for a peer.
func TestLivePeerParsesJSONMembers(t *testing.T) {
	c, pl := threeNodes("kv01", 0)
	pl.members = []string{`{"node":"kv01","index":0,"addr":"10.0.0.1","port":21001}`}
	if p, _ := controller.LivePeerForTest(c, context.Background()); p != "" {
		t.Fatalf("own member taken as peer: %q", p)
	}
	pl.members = append(pl.members, `{"node":"kv02","index":1,"addr":"10.0.0.2","port":22002}`, "kv03")
	if p, _ := controller.LivePeerForTest(c, context.Background()); p != "kv02" && p != "kv03" {
		t.Fatalf("peer: %q", p)
	}
}
