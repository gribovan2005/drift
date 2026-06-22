// Package job loads declarative YAML pipeline definitions and builds runnable
// pipelines. It implements the hybrid model from the CLI & Jobs spec: common
// operators are configured by data (built-ins), while arbitrary Go logic is
// referenced by name via "ref:<name>" from an in-process registry.
package job

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"

	"gopkg.in/yaml.v3"
)

// Spec is the parsed YAML job definition. The json tags define the HTTP wire
// format used by the web builder; note that encoding/json does not honour yaml's
// ",inline", so params are a nested object on the wire and inline in the file.
type Spec struct {
	Name   string        `yaml:"name" json:"name"`
	Source ComponentSpec `yaml:"source" json:"source"`
	Stages []StageSpec   `yaml:"stages" json:"stages"`
	Sink   ComponentSpec `yaml:"sink" json:"sink"`
}

// ComponentSpec describes a source or sink: a type plus free-form params.
type ComponentSpec struct {
	Type   string         `yaml:"type" json:"type"`
	Params map[string]any `yaml:",inline" json:"params,omitempty"`
}

// StageSpec describes one operator stage. Params beyond the reserved keys
// (label, op, next) are operator-specific and captured inline.
type StageSpec struct {
	Label string   `yaml:"label" json:"label"`
	Op    string   `yaml:"op" json:"op"`
	Next  []string `yaml:"next,omitempty" json:"next,omitempty"`
	// Parallelism runs this stage across N shards (intra-stage data parallelism).
	// 0/1 = single goroutine. Only stateless and keyed operators (dedup, session)
	// can be parallelized; global windows cannot. See pkg/pipeline.Parallel.
	Parallelism int            `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
	Params      map[string]any `yaml:",inline" json:"params,omitempty"`
}

// Built is the result of loading a job: the parsed spec plus the constructed
// runnable components.
type Built struct {
	Spec   Spec
	Source core.Source
	Stages []pipeline.Stage
	Sink   core.Sink
}

// Pipeline assembles a runnable pipeline from the built components.
func (b *Built) Pipeline(opts ...pipeline.Option) *pipeline.Pipeline {
	return pipeline.New(b.Source, b.Stages, b.Sink, opts...)
}

// Parse unmarshals YAML into a Spec without building anything (used by validate).
func Parse(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Spec{}, fmt.Errorf("job: parse yaml: %w", err)
	}
	if s.Name == "" {
		return Spec{}, fmt.Errorf("job: missing required field 'name'")
	}
	return s, nil
}
