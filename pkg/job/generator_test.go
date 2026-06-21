package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderField(t *testing.T) {
	// seq + substitution
	assert.Equal(t, 5, renderField("seq", 5))
	assert.Equal(t, "u5_x", renderField("u${seq}_x", 5))
	assert.Equal(t, "hello", renderField("hello", 1))
	assert.Equal(t, true, renderField(true, 1)) // non-string passthrough

	// choice cycles by sequence
	assert.Equal(t, "a", renderField("choice:a|b|c", 0))
	assert.Equal(t, "b", renderField("choice:a|b|c", 1))
	assert.Equal(t, "c", renderField("choice:a|b|c", 2))
	assert.Equal(t, "a", renderField("choice:a|b|c", 3))

	// rand:int within bounds
	for i := range 50 {
		v := renderField("rand:int:5:7", i).(int)
		assert.GreaterOrEqual(t, v, 5)
		assert.LessOrEqual(t, v, 7)
	}

	// rand:float within bounds
	for i := range 50 {
		v := renderField("rand:float:0:1", i).(float64)
		assert.GreaterOrEqual(t, v, 0.0)
		assert.Less(t, v, 1.0)
	}

	// malformed templates fall back to verbatim
	assert.Equal(t, "rand:int:bad", renderField("rand:int:bad", 1))
}
