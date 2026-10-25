package clusterstatus

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/httpapi"
)

func TestTableShowsForwarderAndPeers(t *testing.T) {
	a := snap("kv01", "leader", "master", 3, 0)
	a.Forwarder = &httpapi.Forwarder{Listen: "127.0.1.1:18379", Reason: "this node is the leader"}
	b := snap("kv02", "follower", "slave", 3, 0)
	b.Forwarder = &httpapi.Forwarder{Listen: "127.0.1.1:18379", Target: "10.0.0.1:23001", Active: 1, Total: 4, Resets: 2, BytesUp: 10, BytesDown: 900}
	b.Peers = []httpapi.Peer{{Index: 0, Listen: "127.0.2.0:18379", Target: "10.0.0.1:23001"}, {Index: 2, Listen: "127.0.2.2:18379", Reason: "no live member"}}
	var out bytes.Buffer
	WriteTable(&out, eval(a, b), time.Now())
	s := out.String()
	for _, want := range []string{"forwarders:", "none (this node is the leader)", "-> 10.0.0.1:23001", "resets 2", "bytes up/down 10/900", "[2] 127.0.2.2:18379 -> none"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q:\n%s", want, s)
		}
	}
}
