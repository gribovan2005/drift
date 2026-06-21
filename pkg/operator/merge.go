package operator

import "github.com/andrejgribov/drift/pkg/core"

// Merge combines records from the pipeline primary stream (via Process) with
// records arriving on an extra channel. On each Process call the extra channel
// is drained non-blocking, so records from extra appear only when the primary
// stream is active.
//
// Limitation: if the primary stream produces no records, extra records are not
// drained until the next primary batch arrives.
//
// The extra channel must be managed by the caller; Merge does not close it.
type Merge struct {
	extra  <-chan []core.Record
	schema core.Schema
}

// NewMerge creates a Merge operator. extra must be non-nil.
func NewMerge(extra <-chan []core.Record) *Merge {
	return &Merge{extra: extra}
}

func (m *Merge) Process(in []core.Record) ([]core.Record, error) {
	out := make([]core.Record, len(in))
	copy(out, in)

	// Non-blocking drain of the extra channel.
	for {
		select {
		case batch, ok := <-m.extra:
			if !ok {
				return out, nil
			}
			out = append(out, batch...)
		default:
			return out, nil
		}
	}
}

func (m *Merge) OnSchemaChange(s core.Schema) { m.schema = s }
