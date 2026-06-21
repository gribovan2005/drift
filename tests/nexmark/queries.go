package nexmark

import (
	"strconv"
	"strings"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
)

// The currency factor Nexmark q1 uses to convert bid prices USD -> EUR.
const usdToEur = 0.908

// Q0 — pass-through. Establishes the raw end-to-end throughput ceiling (every
// query pays at least this). Nexmark q0 is the identity query.
func Q0() []pipeline.Stage {
	return []pipeline.Stage{{
		Label: "identity",
		Op:    operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }),
	}}
}

// Q1 — currency conversion. Map each bid's price from USD cents to EUR cents.
func Q1() []pipeline.Stage {
	return []pipeline.Stage{{
		Label: "to-eur",
		Op: operator.NewMap(func(r core.Record) (core.Record, error) {
			r.Payload["price"] = int(float64(r.Payload["price"].(int)) * usdToEur)
			return r, nil
		}),
	}}
}

// Q2 — filtering. Keep only bids whose auction id matches a sampling rule
// (Nexmark uses auction % 123 == 0). Drops ~99% of the stream.
func Q2() []pipeline.Stage {
	return []pipeline.Stage{{
		Label: "sample",
		Op: operator.NewFilter(func(r core.Record) bool {
			return r.Payload["auction"].(int)%123 == 0
		}),
	}}
}

// Q7 — highest bid per window. Tumbling (count-based proxy for Nexmark's 10s
// event-time window) emitting the max-price bid of each window.
func Q7() []pipeline.Stage {
	win, _ := operator.NewTumblingWindow(10000, func(w []core.Record) (core.Record, error) {
		best := w[0]
		maxP := -1
		for _, r := range w {
			if p := r.Payload["price"].(int); p > maxP {
				maxP = p
				best = r
			}
		}
		return best, nil
	})
	return []pipeline.Stage{{Label: "max-per-window", Op: win}}
}

// Q14 — expression + filter. Add a computed field, keep only sizeable bids.
func Q14() []pipeline.Stage {
	return []pipeline.Stage{
		{Label: "compute", Op: operator.NewMap(func(r core.Record) (core.Record, error) {
			r.Payload["price_dollars"] = float64(r.Payload["price"].(int)) / 100.0
			return r, nil
		})},
		{Label: "big-bids", Op: operator.NewFilter(func(r core.Record) bool {
			return r.Payload["price"].(int) > 1000
		})},
	}
}

// Q21 — add channel id derived from the bid url (last path segment).
func Q21() []pipeline.Stage {
	return []pipeline.Stage{{Label: "channel-id", Op: operator.NewMap(func(r core.Record) (core.Record, error) {
		url := r.Payload["url"].(string)
		if i := strings.LastIndexByte(url, '/'); i >= 0 {
			r.Payload["channel_id"] = url[i+1:]
		}
		return r, nil
	})}}
}

// Q22 — split the url into directory components.
func Q22() []pipeline.Stage {
	return []pipeline.Stage{{Label: "split-url", Op: operator.NewMap(func(r core.Record) (core.Record, error) {
		parts := strings.Split(r.Payload["url"].(string), "/")
		if len(parts) > 2 {
			r.Payload["dir"] = parts[2]
		}
		return r, nil
	})}}
}

// keyField returns a KeyFunc reading a payload field as a string key.
func keyField(field string) operator.KeyFunc {
	return func(r core.Record) string {
		switch v := r.Payload[field].(type) {
		case string:
			return v
		case int:
			return strconv.Itoa(v)
		default:
			return ""
		}
	}
}

// groupCount builds a keyed group-by count stage over fixed-count windows.
func groupCount(label, field string, size int) pipeline.Stage {
	w, _ := operator.NewKeyedCountWindow(keyField(field), size, operator.CountAgg(field))
	return pipeline.Stage{Label: label, Op: w}
}

// Q12 — bids per bidder in a (count-window proxy for processing-time) window.
func Q12() []pipeline.Stage { return []pipeline.Stage{groupCount("per-bidder", "bidder", 100)} }

// Q15 — bidding statistics: count per day bucket.
func Q15() []pipeline.Stage {
	day := operator.NewMap(func(r core.Record) (core.Record, error) {
		r.Payload["day"] = int(r.Payload["dateTime"].(int64) / 86_400_000)
		return r, nil
	})
	w, _ := operator.NewKeyedCountWindow(keyField("day"), 1000, operator.CountAgg("day"))
	return []pipeline.Stage{{Label: "day", Op: day, Next: []string{"by-day"}}, {Label: "by-day", Op: w}}
}

// Q16 — channel statistics: count per channel.
func Q16() []pipeline.Stage { return []pipeline.Stage{groupCount("per-channel", "channel", 1000)} }

// Q17 — auction statistics: count per auction.
func Q17() []pipeline.Stage { return []pipeline.Stage{groupCount("per-auction", "auction", 1000)} }

// Q5 — hot items: count bids per auction in a window, then pick the hottest
// auction(s) by count (group-by count → global top-N).
func Q5() []pipeline.Stage {
	gc, _ := operator.NewKeyedCountWindow(keyField("auction"), 5000, operator.CountAgg("auction"))
	top, _ := operator.NewTopN(nil, func(r core.Record) float64 { return float64(r.Payload["count"].(int)) }, 1, 50)
	return []pipeline.Stage{
		{Label: "count-bids", Op: gc, Next: []string{"hottest"}},
		{Label: "hottest", Op: top},
	}
}

// Q11 — user sessions: number of bids per bidder per session (event-time gap).
func Q11() []pipeline.Stage {
	ts := operator.NewTimestampAssigner(func(r core.Record) time.Time {
		return time.UnixMilli(r.Payload["dateTime"].(int64))
	})
	sw, _ := operator.NewSessionWindow(30*time.Second, keyField("bidder"), func(w []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"bidder": w[0].Payload["bidder"], "count": len(w)}}, nil
	})
	return []pipeline.Stage{
		{Label: "assign-time", Op: ts, Next: []string{"sessions"}},
		{Label: "sessions", Op: sw},
	}
}

// Q18 — latest bid per (auction, bidder): keep the most recent record per key
// (windowed keep-last via a group-by aggregate returning the last row).
func Q18() []pipeline.Stage {
	keyFn := func(r core.Record) string {
		return strconv.Itoa(r.Payload["auction"].(int)) + ":" + strconv.Itoa(r.Payload["bidder"].(int))
	}
	last := operator.KeyedAggregateFunc(func(_ string, w []core.Record) (core.Record, error) {
		return w[len(w)-1], nil
	})
	kw, _ := operator.NewKeyedCountWindow(keyFn, 50, last)
	return []pipeline.Stage{{Label: "latest", Op: kw}}
}

// Q19 — auction top-N: the top-10 bids by price per auction.
func Q19() []pipeline.Stage {
	top, _ := operator.NewTopN(keyField("auction"), func(r core.Record) float64 { return float64(r.Payload["price"].(int)) }, 10, 100)
	return []pipeline.Stage{{Label: "top-bids", Op: top}}
}

// ── join queries (run over the mixed Person/Auction/Bid stream) ─────────────

// Q3 — local item suggestion: auctions joined to their seller (person), kept
// only for sellers in OR/ID/CA.
func Q3() []pipeline.Stage {
	j, _ := operator.NewJoin("auction", keyField("seller"), "person", keyField("id"), 4,
		func(auc, per core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{
				"name": per.Payload["name"], "city": per.Payload["city"],
				"state": per.Payload["state"], "auction": auc.Payload["id"],
			}}, nil
		})
	filt := operator.NewFilter(func(r core.Record) bool {
		s, _ := r.Payload["state"].(string)
		return s == "OR" || s == "ID" || s == "CA"
	})
	return []pipeline.Stage{{Label: "join", Op: j, Next: []string{"in-state"}}, {Label: "in-state", Op: filt}}
}

// Q8 — monitor new users: persons joined to auctions they created.
func Q8() []pipeline.Stage {
	j, _ := operator.NewJoin("person", keyField("id"), "auction", keyField("seller"), 4,
		func(per, auc core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{
				"id": per.Payload["id"], "name": per.Payload["name"], "auction": auc.Payload["id"],
			}}, nil
		})
	return []pipeline.Stage{{Label: "new-users", Op: j}}
}

// Q20 — expand bid with auction: enrich each bid with its auction's fields.
func Q20() []pipeline.Stage {
	return []pipeline.Stage{{Label: "enrich", Op: bidAuctionJoin(func(bid, auc core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{
			"auction": bid.Payload["auction"], "price": bid.Payload["price"],
			"bidder": bid.Payload["bidder"], "category": auc.Payload["category"],
			"seller": auc.Payload["seller"],
		}}, nil
	})}}
}

// Q13 — bounded side input: enrich bids from a static side table (a map join).
func Q13() []pipeline.Stage {
	side := make(map[int]string, numAuctions)
	for i := 0; i < numAuctions; i++ {
		side[i] = "cat-" + strconv.Itoa(i%10)
	}
	m := operator.NewMap(func(r core.Record) (core.Record, error) {
		if a, ok := r.Payload["auction"].(int); ok {
			r.Payload["side"] = side[a]
		}
		return r, nil
	})
	return []pipeline.Stage{{Label: "side-lookup", Op: m}}
}

// Q9 — winning bids: highest bid price per auction (join then keyed max).
func Q9() []pipeline.Stage {
	w, _ := operator.NewKeyedCountWindow(keyField("auction"), 1000, operator.MaxAgg("auction", "price"))
	return []pipeline.Stage{
		{Label: "join", Op: bidAuctionJoin(func(bid, auc core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"auction": bid.Payload["auction"], "price": bid.Payload["price"]}}, nil
		}), Next: []string{"winning"}},
		{Label: "winning", Op: w},
	}
}

// Q4 — average price per category (join bid+auction, then avg by category).
func Q4() []pipeline.Stage {
	w, _ := operator.NewKeyedCountWindow(keyField("category"), 1000, operator.AvgAgg("category", "price"))
	return []pipeline.Stage{
		{Label: "join", Op: bidAuctionJoin(func(bid, auc core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"category": auc.Payload["category"], "price": bid.Payload["price"]}}, nil
		}), Next: []string{"avg-by-cat"}},
		{Label: "avg-by-cat", Op: w},
	}
}

// Q6 — average selling price per seller (join bid+auction, then avg by seller).
func Q6() []pipeline.Stage {
	w, _ := operator.NewKeyedCountWindow(keyField("seller"), 500, operator.AvgAgg("seller", "price"))
	return []pipeline.Stage{
		{Label: "join", Op: bidAuctionJoin(func(bid, auc core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"seller": auc.Payload["seller"], "price": bid.Payload["price"]}}, nil
		}), Next: []string{"avg-by-seller"}},
		{Label: "avg-by-seller", Op: w},
	}
}

// bidAuctionJoin builds a Join of the bid stream onto auctions (by auction id).
func bidAuctionJoin(fn operator.JoinFunc) *operator.Join {
	j, _ := operator.NewJoin("bid", keyField("auction"), "auction", keyField("id"), 2, fn)
	return j
}

// Q10 — log to storage: pass events through to a file sink (NDJSON).
func Q10() []pipeline.Stage {
	return []pipeline.Stage{{
		Label: "passthrough",
		Op:    operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }),
	}}
}

// Query is a named benchmark query. Mixed selects the Nexmark mixed stream
// (Person/Auction/Bid) instead of the bid-only stream; FileSink writes the
// output to an NDJSON file instead of the in-memory sink.
type Query struct {
	ID       string
	Desc     string
	Stages   func() []pipeline.Stage
	Mixed    bool
	FileSink bool
}

// Implemented returns the Nexmark queries Drift can currently express.
func Implemented() []Query {
	return []Query{
		{ID: "q0", Desc: "pass-through (throughput ceiling)", Stages: Q0},
		{ID: "q1", Desc: "currency conversion (map)", Stages: Q1},
		{ID: "q2", Desc: "filtering (auction %% 123 == 0)", Stages: Q2},
		{ID: "q3", Desc: "local item suggestion (join+filter)", Stages: Q3, Mixed: true},
		{ID: "q4", Desc: "avg price per category (join+group)", Stages: Q4, Mixed: true},
		{ID: "q5", Desc: "hot items (group-by + top-N)", Stages: Q5},
		{ID: "q6", Desc: "avg selling price per seller (join+group)", Stages: Q6, Mixed: true},
		{ID: "q7", Desc: "highest bid per window (window max)", Stages: Q7},
		{ID: "q8", Desc: "monitor new users (join)", Stages: Q8, Mixed: true},
		{ID: "q9", Desc: "winning bids (join+max)", Stages: Q9, Mixed: true},
		{ID: "q10", Desc: "log to storage (file sink)", Stages: Q10, FileSink: true},
		{ID: "q11", Desc: "user sessions per bidder (session)", Stages: Q11},
		{ID: "q12", Desc: "bids per bidder (group-by count)", Stages: Q12},
		{ID: "q13", Desc: "bounded side input (static join)", Stages: Q13, Mixed: true},
		{ID: "q14", Desc: "expression + filter", Stages: Q14},
		{ID: "q15", Desc: "bidding stats per day (group-by)", Stages: Q15},
		{ID: "q16", Desc: "channel stats (group-by count)", Stages: Q16},
		{ID: "q17", Desc: "auction stats (group-by count)", Stages: Q17},
		{ID: "q18", Desc: "latest bid per auction+bidder", Stages: Q18},
		{ID: "q19", Desc: "auction top-10 bids (top-N)", Stages: Q19},
		{ID: "q20", Desc: "expand bid with auction (join)", Stages: Q20, Mixed: true},
		{ID: "q21", Desc: "add channel id (map)", Stages: Q21},
		{ID: "q22", Desc: "split url (map)", Stages: Q22},
	}
}
