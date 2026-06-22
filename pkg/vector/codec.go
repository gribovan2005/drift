package vector

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/gribovan2005/drift/pkg/core"
)

// Binary columnar wire format for a Batch (Int64/Float64 columns):
//
//	uint32  numCols
//	per col: uint8 kind, uint16 nameLen, name bytes
//	uint32  len (rows)
//	per col: len * 8 bytes, little-endian (int64 bits / float64 bits)
//
// This is the fast alternative to JSON for the vectorized fast-lane: decode is a
// few tight loops over raw bytes — no parsing, no per-value allocation, no boxing.
// Encode favours simplicity (it is an offline/produce-side step); Decode is the
// hot path and is hand-rolled for speed. See drift/Specs/Vectorized Fast-Lane.md.

// EncodeBatch serialises b to the binary columnar format. Only Int64/Float64
// columns are supported.
func EncodeBatch(b *core.Batch) ([]byte, error) {
	for _, c := range b.Cols {
		if c.Kind != core.KindInt64 && c.Kind != core.KindFloat64 {
			return nil, fmt.Errorf("vector: EncodeBatch supports Int64/Float64 only, got kind %d", c.Kind)
		}
	}
	// size: header + len + data
	size := 4
	for i := range b.Cols {
		size += 1 + 2 + len(b.Schema.Fields[i].Name)
	}
	size += 4 + len(b.Cols)*b.Len*8

	out := make([]byte, size)
	o := 0
	binary.LittleEndian.PutUint32(out[o:], uint32(len(b.Cols)))
	o += 4
	for i, c := range b.Cols {
		out[o] = byte(c.Kind)
		o++
		name := b.Schema.Fields[i].Name
		binary.LittleEndian.PutUint16(out[o:], uint16(len(name)))
		o += 2
		o += copy(out[o:], name)
	}
	binary.LittleEndian.PutUint32(out[o:], uint32(b.Len))
	o += 4
	for _, c := range b.Cols {
		switch c.Kind {
		case core.KindInt64:
			for j := 0; j < b.Len; j++ {
				binary.LittleEndian.PutUint64(out[o:], uint64(c.I64[j]))
				o += 8
			}
		case core.KindFloat64:
			for j := 0; j < b.Len; j++ {
				binary.LittleEndian.PutUint64(out[o:], math.Float64bits(c.F64[j]))
				o += 8
			}
		}
	}
	return out, nil
}

// DecodeBatch parses the binary columnar format back into a *core.Batch. Hot path.
func DecodeBatch(data []byte) (*core.Batch, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("vector: short frame")
	}
	o := 0
	numCols := int(binary.LittleEndian.Uint32(data[o:]))
	o += 4
	fields := make([]core.Field, numCols)
	kinds := make([]core.ColumnKind, numCols)
	for i := 0; i < numCols; i++ {
		if o+3 > len(data) {
			return nil, fmt.Errorf("vector: truncated header")
		}
		kind := core.ColumnKind(data[o])
		o++
		nameLen := int(binary.LittleEndian.Uint16(data[o:]))
		o += 2
		if o+nameLen > len(data) {
			return nil, fmt.Errorf("vector: truncated name")
		}
		name := string(data[o : o+nameLen])
		o += nameLen
		kinds[i] = kind
		typ := core.FieldTypeFloat
		if kind == core.KindInt64 {
			typ = core.FieldTypeInt
		}
		fields[i] = core.Field{Name: name, Type: typ}
	}
	if o+4 > len(data) {
		return nil, fmt.Errorf("vector: truncated len")
	}
	n := int(binary.LittleEndian.Uint32(data[o:]))
	o += 4

	cols := make([]core.Column, numCols)
	for i := 0; i < numCols; i++ {
		if o+n*8 > len(data) {
			return nil, fmt.Errorf("vector: truncated column %d", i)
		}
		switch kinds[i] {
		case core.KindInt64:
			v := make([]int64, n)
			for j := 0; j < n; j++ {
				v[j] = int64(binary.LittleEndian.Uint64(data[o:]))
				o += 8
			}
			cols[i] = core.Column{Kind: core.KindInt64, I64: v}
		case core.KindFloat64:
			v := make([]float64, n)
			for j := 0; j < n; j++ {
				v[j] = math.Float64frombits(binary.LittleEndian.Uint64(data[o:]))
				o += 8
			}
			cols[i] = core.Column{Kind: core.KindFloat64, F64: v}
		default:
			return nil, fmt.Errorf("vector: unsupported kind %d", kinds[i])
		}
	}
	return &core.Batch{Schema: core.Schema{Fields: fields}, Len: n, Cols: cols}, nil
}
