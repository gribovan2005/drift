package job

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Marshal serialises a Spec to YAML. It is the inverse of Parse: the ",inline"
// param maps are flattened back into each component/stage, so the output round-
// trips through Parse/Load. Used by the control plane to persist builder output.
func Marshal(spec Spec) ([]byte, error) {
	data, err := yaml.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("job: marshal yaml: %w", err)
	}
	return data, nil
}
