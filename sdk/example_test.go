package sdk_test

import (
	"context"
	"fmt"

	"github.com/gribovan2005/drift/sdk"
)

// Example builds a small pipeline with the fluent SDK: keep even values, add one,
// and collect the results in memory.
func Example() {
	in := []sdk.Record{
		{Payload: map[string]any{"v": 0}},
		{Payload: map[string]any{"v": 1}},
		{Payload: map[string]any{"v": 2}},
		{Payload: map[string]any{"v": 3}},
	}

	out := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(in)).
		Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
		Map(func(r sdk.Record) (sdk.Record, error) {
			r.Payload["v"] = r.Payload["v"].(int) + 1
			return r, nil
		}).
		To(out).
		Run(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(len(out.Records()), "records")
	// Output: 2 records
}
