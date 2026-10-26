package elect

import (
	"testing"
	"time"
)

func TestOffsetElect(t *testing.T) {
	now := time.Now()
	e := func(n string, off int64, age time.Duration) OffsetEntry {
		return OffsetEntry{Node: n, Offset: off, AtMs: now.Add(-age).UnixMilli()}
	}
	base := OffsetElectInput{Self: "kv02", SelfOffset: 31145777, Peers: []string{"kv01"}, Fresh: 3 * time.Second, Now: now}
	cases := []struct {
		name  string
		mod   func(*OffsetElectInput)
		allow bool
	}{
		{"ahead of peer", func(in *OffsetElectInput) { in.Offsets = []OffsetEntry{e("kv01", 30819728, 0)} }, true},
		{"behind peer", func(in *OffsetElectInput) { in.Offsets = []OffsetEntry{e("kv01", 31145778, 0)} }, false},
		{"tie lower name wins", func(in *OffsetElectInput) { in.Offsets = []OffsetEntry{e("kv01", 31145777, 0)} }, false},
		{"tie we are lower", func(in *OffsetElectInput) {
			in.Self, in.Peers = "kv01", []string{"kv02"}
			in.Offsets = []OffsetEntry{e("kv02", 31145777, 0)}
		}, true},
		{"peer entry stale", func(in *OffsetElectInput) { in.Offsets = []OffsetEntry{e("kv01", 1, 10*time.Second)} }, false},
		{"peer entry missing", func(in *OffsetElectInput) {}, false},
		{"own offset unknown", func(in *OffsetElectInput) { in.SelfOffset = -1; in.Offsets = []OffsetEntry{e("kv01", 1, 0)} }, false},
		{"no peers", func(in *OffsetElectInput) { in.Peers = nil }, false},
		{"departed old leader not visible", func(in *OffsetElectInput) {
			in.OldLeader = "kv03"
			in.Offsets = []OffsetEntry{e("kv01", 1, 0)}
		}, false},
		{"old leader visible and behind", func(in *OffsetElectInput) {
			in.OldLeader = "kv01"
			in.Peers = nil
			in.Offsets = []OffsetEntry{e("kv01", 1, 0)}
		}, true},
		{"one of three ahead", func(in *OffsetElectInput) {
			in.Peers = []string{"kv01", "kv03"}
			in.Offsets = []OffsetEntry{e("kv01", 1, 0), e("kv03", 31145800, 0)}
		}, false},
		{"own entry ignored", func(in *OffsetElectInput) {
			in.Offsets = []OffsetEntry{e("kv02", 99999999999, 0), e("kv01", 1, 0)}
		}, true},
	}
	for _, tc := range cases {
		in := base
		tc.mod(&in)
		if d := OffsetElect(in); d.Allow != tc.allow {
			t.Errorf("%s: allow=%v want %v (%s)", tc.name, d.Allow, tc.allow, d.Reason)
		}
	}
}

// Two leaderless nodes evaluating each other's entries never both win.
func TestOffsetElectAtMostOneWinner(t *testing.T) {
	now := time.Now()
	for _, offs := range [][2]int64{{5, 5}, {5, 6}, {6, 5}, {0, 0}} {
		ents := []OffsetEntry{{Node: "a", Offset: offs[0], AtMs: now.UnixMilli()}, {Node: "b", Offset: offs[1], AtMs: now.UnixMilli()}}
		a := OffsetElect(OffsetElectInput{Self: "a", SelfOffset: offs[0], Peers: []string{"b"}, Offsets: ents, Fresh: time.Second, Now: now})
		b := OffsetElect(OffsetElectInput{Self: "b", SelfOffset: offs[1], Peers: []string{"a"}, Offsets: ents, Fresh: time.Second, Now: now})
		if a.Allow == b.Allow {
			t.Errorf("offsets %v: a=%v b=%v, want exactly one winner", offs, a.Allow, b.Allow)
		}
	}
}

func TestParseOffset(t *testing.T) {
	now := time.Now()
	e, ok := ParseOffset(string(MarshalOffset("kv01", 42, now)))
	if !ok || e.Node != "kv01" || e.Offset != 42 || e.AtMs != now.UnixMilli() {
		t.Fatalf("round trip: %+v %v", e, ok)
	}
	if _, ok := ParseOffset("kv01"); ok {
		t.Fatal("bare value must not parse")
	}
}
