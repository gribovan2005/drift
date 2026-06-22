package vector

import "github.com/gribovan2005/drift/pkg/core"

// Vector aggregates reduce a stream of chunks to a single scalar result. They
// accumulate during Process (emitting nothing) and emit one row Record on Flush —
// the same pattern as the row windows. The result leaves the columnar world (a
// scalar), so it is a normal row Record and can go to any sink.
//
// These are single-stage: do NOT wrap an aggregate in vector.Parallel (each shard
// would produce its own partial). Stateless Map/Filter are the parallelisable ops.

// SumInt64 sums the named int64 column across all chunks; emits {out: sum} on flush.
func SumInt64(field, out string) core.Operator { return &sumInt64{field: field, out: out} }

type sumInt64 struct {
	field, out string
	sum        int64
}

func (a *sumInt64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		for _, v := range r.Chunk.Int64(a.field) {
			a.sum += v
		}
	}
	return nil, nil
}
func (a *sumInt64) OnSchemaChange(core.Schema) {}
func (a *sumInt64) Flush() ([]core.Record, error) {
	return []core.Record{{Payload: map[string]any{a.out: a.sum}}}, nil
}

// SumFloat64 sums the named float64 column across all chunks.
func SumFloat64(field, out string) core.Operator { return &sumFloat64{field: field, out: out} }

type sumFloat64 struct {
	field, out string
	sum        float64
}

func (a *sumFloat64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		for _, v := range r.Chunk.Float64(a.field) {
			a.sum += v
		}
	}
	return nil, nil
}
func (a *sumFloat64) OnSchemaChange(core.Schema) {}
func (a *sumFloat64) Flush() ([]core.Record, error) {
	return []core.Record{{Payload: map[string]any{a.out: a.sum}}}, nil
}

// MaxInt64 emits the maximum of the named int64 column across all chunks. If no
// rows were seen, it emits nothing.
func MaxInt64(field, out string) core.Operator { return &maxInt64{field: field, out: out} }

type maxInt64 struct {
	field, out string
	max        int64
	seen       bool
}

func (a *maxInt64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		for _, v := range r.Chunk.Int64(a.field) {
			if !a.seen || v > a.max {
				a.max, a.seen = v, true
			}
		}
	}
	return nil, nil
}
func (a *maxInt64) OnSchemaChange(core.Schema) {}
func (a *maxInt64) Flush() ([]core.Record, error) {
	if !a.seen {
		return nil, nil
	}
	return []core.Record{{Payload: map[string]any{a.out: a.max}}}, nil
}

// CountRows counts total rows across all chunks; emits {out: count} on flush.
func CountRows(out string) core.Operator { return &countRows{out: out} }

type countRows struct {
	out string
	n   int64
}

func (a *countRows) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk != nil {
			a.n += int64(r.Chunk.Len)
		}
	}
	return nil, nil
}
func (a *countRows) OnSchemaChange(core.Schema) {}
func (a *countRows) Flush() ([]core.Record, error) {
	return []core.Record{{Payload: map[string]any{a.out: a.n}}}, nil
}
