package job

import (
	"fmt"
	"sync"

	"github.com/andrejgribov/drift/pkg/core"
)

// registry holds host-registered code-defined components resolved by "ref:<name>".
// It is package-global so a host program can register before calling Load.
var registry = struct {
	mu      sync.RWMutex
	ops     map[string]core.Operator
	sources map[string]core.Source
	sinks   map[string]core.Sink
}{
	ops:     map[string]core.Operator{},
	sources: map[string]core.Source{},
	sinks:   map[string]core.Sink{},
}

// RegisterOp registers a code-defined operator under name, resolvable in a job
// via "op: ref:<name>".
func RegisterOp(name string, op core.Operator) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.ops[name] = op
}

// RegisterSource registers a code-defined source resolvable via "type: ref:<name>".
func RegisterSource(name string, s core.Source) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sources[name] = s
}

// RegisterSink registers a code-defined sink resolvable via "type: ref:<name>".
func RegisterSink(name string, s core.Sink) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sinks[name] = s
}

func lookupOp(name string) (core.Operator, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	op, ok := registry.ops[name]
	if !ok {
		return nil, fmt.Errorf("job: no operator registered as ref:%s", name)
	}
	return op, nil
}

func lookupSource(name string) (core.Source, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	s, ok := registry.sources[name]
	if !ok {
		return nil, fmt.Errorf("job: no source registered as ref:%s", name)
	}
	return s, nil
}

func lookupSink(name string) (core.Sink, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	s, ok := registry.sinks[name]
	if !ok {
		return nil, fmt.Errorf("job: no sink registered as ref:%s", name)
	}
	return s, nil
}

// RegisteredRefs returns the names registered in each category (for `drift list`).
func RegisteredRefs() (ops, sources, sinks []string) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	for k := range registry.ops {
		ops = append(ops, k)
	}
	for k := range registry.sources {
		sources = append(sources, k)
	}
	for k := range registry.sinks {
		sinks = append(sinks, k)
	}
	return ops, sources, sinks
}
