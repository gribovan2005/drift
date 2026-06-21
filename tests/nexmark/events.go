// Package nexmark implements a subset of the Nexmark streaming benchmark
// (the de-facto standard for comparing stream processors like Flink) on top of
// Drift. Nexmark models an auction system with three event types — Person,
// Auction, Bid — and a set of queries q0..q22. Drift can express the
// stateless/windowed queries (q0, q1, q2, …); queries needing joins or keyed
// group-by aggregation are tracked in BENCHMARKS.md until those operators exist.
//
// See https://github.com/nexmark/nexmark for the reference (Flink) suite.
package nexmark

import (
	"github.com/andrejgribov/drift/pkg/core"
)

// channels mimics the Nexmark bid "channel" dimension.
var channels = []string{"Google", "Facebook", "Baidu", "Apple", "Amazon"}

// states includes OR/ID/CA (the ones Nexmark q3 filters on) plus others.
var states = []string{"OR", "ID", "CA", "WA", "NY", "TX", "FL", "OH"}

// Key-space sizes — bounded so joins (auction.seller→person, bid.auction→auction)
// find matches as ids cycle through the mixed stream.
const (
	numPersons  = 1000
	numAuctions = 1000
)

// Nexmark generator proportions (per 50 events: 1 person, 3 auctions, 46 bids).
const (
	personProp  = 1
	auctionProp = 3
	mixSpan     = 50
)

// GenerateEvents produces the Nexmark mixed stream — Person, Auction, and Bid
// events interleaved in one source (the shape real joins consume). The event
// type is the record's SchemaID ("person"|"auction"|"bid"). Deterministic.
func GenerateEvents(n int) []core.Record {
	recs := make([]core.Record, n)
	var p, a, b int // per-type counters
	for i := range recs {
		switch slot := i % mixSpan; {
		case slot < personProp:
			recs[i] = personRecord(p)
			p++
		case slot < personProp+auctionProp:
			recs[i] = auctionRecord(a)
			a++
		default:
			recs[i] = bidRecord(b)
			b++
		}
	}
	return recs
}

func personRecord(i int) core.Record {
	id := i % numPersons
	return core.Record{SchemaID: "person", SchemaVersion: 1, Payload: map[string]any{
		"id":       id,
		"name":     "person-" + channels[i%len(channels)],
		"city":     channels[i%len(channels)],
		"state":    states[id%len(states)],
		"dateTime": int64(1_700_000_000_000 + i),
	}}
}

func auctionRecord(i int) core.Record {
	return core.Record{SchemaID: "auction", SchemaVersion: 1, Payload: map[string]any{
		"id":       i % numAuctions,
		"seller":   i % numPersons,
		"category": i % 10,
		"itemName": "item-" + channels[i%len(channels)],
		"dateTime": int64(1_700_000_000_000 + i),
	}}
}

func bidRecord(i int) core.Record {
	return core.Record{SchemaID: "bid", SchemaVersion: 1, Payload: map[string]any{
		"auction":  i % numAuctions,
		"bidder":   i % numPersons,
		"price":    (i*97)%100000 + 1,
		"channel":  channels[i%len(channels)],
		"url":      "https://nexmark/" + channels[i%len(channels)],
		"dateTime": int64(1_700_000_000_000 + i),
	}}
}

// GenerateBids produces n Bid events as core.Records. The distribution is
// deterministic (no RNG) so benchmark runs are reproducible: auction and bidder
// ids spread over fixed key spaces, price cycles a realistic range, and dateTime
// increments. This is the "bid stream" that q0/q1/q2 operate on.
func GenerateBids(n int) []core.Record {
	const auctions = 1000 // active auction key space
	const bidders = 10000 // bidder key space
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.Record{
			SchemaID:      "bid",
			SchemaVersion: 1,
			Payload: map[string]any{
				"auction":  i % auctions,
				"bidder":   i % bidders,
				"price":    (i*97)%100000 + 1, // cents, 1..100000
				"channel":  channels[i%len(channels)],
				"url":      "https://nexmark/" + channels[i%len(channels)],
				"dateTime": int64(1_700_000_000_000 + i), // ms, monotonic
				"extra":    "x",
			},
		}
	}
	return recs
}
