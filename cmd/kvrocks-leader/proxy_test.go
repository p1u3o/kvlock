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
	if a, _, _ := readRoute(both, true)(); a != "127.0.0.1:6666" {
		t.Fatalf("healthy replica should be preferred, got %s", a)
	}
	noRep := fixedView{controller.ProxyView{Leader: "10.0.0.1:6666", Key: "k"}}
	if a, k, _ := readRoute(noRep, true)(); a != "10.0.0.1:6666" || k != "k" {
		t.Fatalf("fallback to leader: %s %s", a, k)
	}
	if a, _, _ := readRoute(noRep, false)(); a != "" {
		t.Fatalf("refuse mode must not route, got %s", a)
	}
	if a, _, _ := writeRoute(both)(); a != "10.0.0.1:6666" {
		t.Fatalf("write route: %s", a)
	}
}
