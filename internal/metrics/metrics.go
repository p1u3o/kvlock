package metrics

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Registry is a tiny Prometheus exposition without external deps.
type Registry struct {
	mu       sync.Mutex
	gauges   map[string]*int64Box
	counters map[string]*int64Box
}

type int64Box struct{ v atomic.Int64 }

func New() *Registry {
	return &Registry{gauges: map[string]*int64Box{}, counters: map[string]*int64Box{}}
}

func (r *Registry) gauge(name string) *int64Box {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.gauges[name]
	if !ok {
		b = &int64Box{}
		r.gauges[name] = b
	}
	return b
}

func (r *Registry) counter(name string) *int64Box {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.counters[name]
	if !ok {
		b = &int64Box{}
		r.counters[name] = b
	}
	return b
}

func (r *Registry) SetGauge(name string, v int64)        { r.gauge(name).v.Store(v) }
func (r *Registry) SetGaugeFloat(name string, v float64) { r.gauge(name).v.Store(int64(v * 1000)) }
func (r *Registry) Inc(name string)                      { r.counter(name).v.Add(1) }
func (r *Registry) Add(name string, d int64)             { r.counter(name).v.Add(d) }

// Float gauges are rendered /1000; document in help text.
var floatGauges = map[string]bool{"lock_held_seconds": true}

// Render writes Prometheus text exposition.
func (r *Registry) Render() string {
	r.mu.Lock()
	names := func(m map[string]*int64Box) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	gNames, cNames := names(r.gauges), names(r.counters)
	type pair struct {
		k string
		v int64
	}
	gs := make([]pair, 0, len(gNames))
	for _, k := range gNames {
		gs = append(gs, pair{k, r.gauges[k].v.Load()})
	}
	cs := make([]pair, 0, len(cNames))
	for _, k := range cNames {
		cs = append(cs, pair{k, r.counters[k].v.Load()})
	}
	r.mu.Unlock()

	out := ""
	for _, p := range gs {
		if floatGauges[p.k] {
			out += fmt.Sprintf("# TYPE %s gauge\n%s %0.3f\n", p.k, p.k, float64(p.v)/1000.0)
		} else {
			out += fmt.Sprintf("# TYPE %s gauge\n%s %d\n", p.k, p.k, p.v)
		}
	}
	for _, p := range cs {
		out += fmt.Sprintf("# TYPE %s counter\n%s %d\n", p.k, p.k, p.v)
	}
	return out
}
