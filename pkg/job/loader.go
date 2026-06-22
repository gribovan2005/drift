package job

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/pipeline"
)

// Load parses a YAML job definition and builds runnable components. It validates
// the spec (unique labels, resolvable next targets, known ops, well-typed
// params, resolvable refs) before constructing anything.
func Load(data []byte) (*Built, error) {
	spec, err := Parse(data)
	if err != nil {
		return nil, err
	}

	if err := validateStructure(spec); err != nil {
		return nil, err
	}

	src, err := buildSource(spec.Source)
	if err != nil {
		return nil, fmt.Errorf("job %q: source: %w", spec.Name, err)
	}

	stages := make([]pipeline.Stage, 0, len(spec.Stages))
	for _, s := range spec.Stages {
		op, err := buildStageOp(s)
		if err != nil {
			return nil, fmt.Errorf("job %q: stage %q: %w", spec.Name, s.Label, err)
		}
		stages = append(stages, pipeline.Stage{
			Label: s.Label,
			Op:    op,
			Next:  s.Next,
		})
	}

	snk, err := buildSink(spec.Sink)
	if err != nil {
		return nil, fmt.Errorf("job %q: sink: %w", spec.Name, err)
	}

	return &Built{Spec: spec, Source: src, Stages: stages, Sink: snk}, nil
}

// validateStructure checks label uniqueness and next-target existence. Operator
// and param validation happen during construction in buildOperator.
func validateStructure(spec Spec) error {
	if len(spec.Stages) == 0 {
		return fmt.Errorf("job %q: no stages defined", spec.Name)
	}

	labels := make(map[string]bool, len(spec.Stages))
	for _, s := range spec.Stages {
		if s.Label == "" {
			return fmt.Errorf("job %q: stage with empty label", spec.Name)
		}
		if labels[s.Label] {
			return fmt.Errorf("job %q: duplicate stage label %q", spec.Name, s.Label)
		}
		labels[s.Label] = true
	}

	for _, s := range spec.Stages {
		for _, n := range s.Next {
			if !labels[n] {
				return fmt.Errorf("job %q: stage %q has next %q which does not exist", spec.Name, s.Label, n)
			}
		}
	}

	return nil
}
