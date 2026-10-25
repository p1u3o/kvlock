package main

import (
	"testing"

	"github.com/p1u3o/kvlock/internal/controller"
)

type fixedView struct{ v controller.ProxyView }

func (f fixedView) ProxyView() controller.ProxyView { return f.v }
func (f fixedView) Subscribe() <-chan struct{}      { return nil }

func TestReadRoute(t *testing.T) {
	both := fixedView{controller.ProxyView{Leader: "10.0.0.1:6666", Key: "k", Replica: "127.0.0.1:6666"}}
	if a := readRoute(both, true)().Addr; a != "127.0.0.1:6666" {
		t.Fatalf("healthy replica should be preferred, got %s", a)
	}
	noRep := fixedView{controller.ProxyView{Leader: "10.0.0.1:6666", Key: "k"}}
	if d := readRoute(noRep, true)(); d.Addr != "10.0.0.1:6666" || d.Key != "k" {
		t.Fatalf("fallback to leader: %s %s", d.Addr, d.Key)
	}
	if a := readRoute(noRep, false)().Addr; a != "" {
		t.Fatalf("refuse mode must not route, got %s", a)
	}
	if a := writeRoute(both)().Addr; a != "10.0.0.1:6666" {
		t.Fatalf("write route: %s", a)
	}
}
