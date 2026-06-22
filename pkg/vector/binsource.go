package vector

import (
	"context"

	"github.com/gribovan2005/drift/pkg/core"
)

// BinSource decodes binary columnar frames (see EncodeBatch) into chunk-records,
// decoding in the read path — modelling a real wire source (e.g. a Kafka topic of
// binary-columnar messages) whose decode cost counts toward end-to-end throughput.
// Frames that fail to decode are skipped. For a parallel binary source, wrap N
// BinSources in source.NewParallel.
func BinSource(frames [][]byte) core.Source {
	return &binSource{frames: frames}
}

type binSource struct{ frames [][]byte }

func (s *binSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 8)
	go func() {
		defer close(ch)
		for _, f := range s.frames {
			b, err := DecodeBatch(f)
			if err != nil {
				continue // skip malformed frame
			}
			select {
			case ch <- core.Record{Chunk: b}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
