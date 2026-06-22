package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/gribovan2005/drift/pkg/core"
)

// Binary columnar wire format for a Batch (Int64/Float64/String/Bool columns):
//
//	uint32  numCols
//	per col: uint8 kind, uint16 nameLen, name bytes
//	uint32  len (rows)
//	per col, len values:
//	  Int64/Float64 → 8 bytes little-endian each
//	  Bool          → 1 byte each (0/1)
//	  String        → uint32 len + raw bytes each
//
// This is the fast alternative to JSON for the vectorized fast-lane: decode is a
// few tight loops over raw bytes — no parsing, no per-value allocation, no boxing.
// Encode favours simplicity (it is an offline/produce-side step); Decode is the
// hot path and is hand-rolled for speed. See drift/Specs/Vectorized Fast-Lane.md.

// EncodeBatch serialises b to the binary columnar format (Int64/Float64/String/Bool).
func EncodeBatch(b *core.Batch) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [8]byte

	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(b.Cols)))
	buf.Write(scratch[:4])
	for i, c := range b.Cols {
		if c.Null != nil {
			// The wire format carries no validity mask yet; refuse rather than silently
			// dropping NULLs (e.g. a left-outer join result). Convert via ToRows for now.
			return nil, fmt.Errorf("vector: EncodeBatch: column %q has NULLs; binary codec has no null mask yet", b.Schema.Fields[i].Name)
		}
		buf.WriteByte(byte(c.Kind))
		name := b.Schema.Fields[i].Name
		binary.LittleEndian.PutUint16(scratch[:2], uint16(len(name)))
		buf.Write(scratch[:2])
		buf.WriteString(name)
	}
	binary.LittleEndian.PutUint32(scratch[:4], uint32(b.Len))
	buf.Write(scratch[:4])

	for _, c := range b.Cols {
		switch c.Kind {
		case core.KindInt64:
			for j := 0; j < b.Len; j++ {
				binary.LittleEndian.PutUint64(scratch[:8], uint64(c.I64[j]))
				buf.Write(scratch[:8])
			}
		case core.KindFloat64:
			for j := 0; j < b.Len; j++ {
				binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(c.F64[j]))
				buf.Write(scratch[:8])
			}
		case core.KindBool:
			for j := 0; j < b.Len; j++ {
				if c.B[j] {
					buf.WriteByte(1)
				} else {
					buf.WriteByte(0)
				}
			}
		case core.KindString:
			for j := 0; j < b.Len; j++ {
				binary.LittleEndian.PutUint32(scratch[:4], uint32(len(c.Str[j])))
				buf.Write(scratch[:4])
				buf.WriteString(c.Str[j])
			}
		default:
			return nil, fmt.Errorf("vector: EncodeBatch unsupported kind %d", c.Kind)
		}
	}
	return buf.Bytes(), nil
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
		var typ core.FieldType
		switch kind {
		case core.KindInt64:
			typ = core.FieldTypeInt
		case core.KindFloat64:
			typ = core.FieldTypeFloat
		case core.KindString:
			typ = core.FieldTypeString
		case core.KindBool:
			typ = core.FieldTypeBool
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
		switch kinds[i] {
		case core.KindInt64:
			if o+n*8 > len(data) {
				return nil, fmt.Errorf("vector: truncated int64 column %d", i)
			}
			v := make([]int64, n)
			for j := 0; j < n; j++ {
				v[j] = int64(binary.LittleEndian.Uint64(data[o:]))
				o += 8
			}
			cols[i] = core.Column{Kind: core.KindInt64, I64: v}
		case core.KindFloat64:
			if o+n*8 > len(data) {
				return nil, fmt.Errorf("vector: truncated float64 column %d", i)
			}
			v := make([]float64, n)
			for j := 0; j < n; j++ {
				v[j] = math.Float64frombits(binary.LittleEndian.Uint64(data[o:]))
				o += 8
			}
			cols[i] = core.Column{Kind: core.KindFloat64, F64: v}
		case core.KindBool:
			if o+n > len(data) {
				return nil, fmt.Errorf("vector: truncated bool column %d", i)
			}
			v := make([]bool, n)
			for j := 0; j < n; j++ {
				v[j] = data[o] != 0
				o++
			}
			cols[i] = core.Column{Kind: core.KindBool, B: v}
		case core.KindString:
			v := make([]string, n)
			for j := 0; j < n; j++ {
				if o+4 > len(data) {
					return nil, fmt.Errorf("vector: truncated string len in column %d", i)
				}
				l := int(binary.LittleEndian.Uint32(data[o:]))
				o += 4
				if o+l > len(data) {
					return nil, fmt.Errorf("vector: truncated string in column %d", i)
				}
				v[j] = string(data[o : o+l])
				o += l
			}
			cols[i] = core.Column{Kind: core.KindString, Str: v}
		default:
			return nil, fmt.Errorf("vector: unsupported kind %d", kinds[i])
		}
	}
	return &core.Batch{Schema: core.Schema{Fields: fields}, Len: n, Cols: cols}, nil
}
