package sdk

import (
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gribovan2005/drift/pkg/pipeline"
)

// Profile bundles the resource knobs for a deployment shape. Use a preset
// (Sidecar / Dedicated) with WithProfile, optionally tweaking fields first.
//
// LOCAL knobs (BatchSize, ChannelBuffer, MaxLinger) are pure pipeline config and
// are always safe — including when Drift is embedded in a host service. The
// PROCESS-GLOBAL knobs (GOMAXPROCS, GCPercent, MemLimit) change the whole Go
// process and are applied ONLY when OwnsProcess() was called, so an embedded
// library never silently re-tunes its host. See drift/Specs/Resource Profiles.md.
type Profile struct {
	BatchSize     int           // stage batch size (→ WithBatchSize)
	ChannelBuffer int           // default channel buffer (→ WithChannelBuffer)
	MaxLinger     time.Duration // time-based partial-batch flush (→ WithMaxLinger)

	GOMAXPROCS int   // process-global; <=0 leaves it unchanged
	GCPercent  int   // process-global GOGC; <=0 leaves it unchanged
	MemLimit   int64 // process-global GOMEMLIMIT in bytes; <=0 leaves it unset

	ownsProcess bool // gates the process-global knobs; set via OwnsProcess()
}

// Sidecar is the polite-neighbour profile: runs next to a latency-sensitive
// service. Small footprint, low latency (small batch + linger flush), frugal GC.
var Sidecar = Profile{
	BatchSize:     16,
	ChannelBuffer: 32,
	MaxLinger:     5 * time.Millisecond,
	GOMAXPROCS:    maxInt(1, runtime.NumCPU()/4),
	GCPercent:     50,
}

// Dedicated is the throughput profile for a standalone consumer that owns the
// node: big batches and buffers, all cores, GC tuned for fewer cycles.
var Dedicated = Profile{
	BatchSize:     512,
	ChannelBuffer: 1024,
	MaxLinger:     0,
	GOMAXPROCS:    runtime.NumCPU(),
	GCPercent:     200,
}

// OwnsProcess returns a copy of the profile with the process-global knobs enabled.
// Call it ONLY when Drift owns the process (a standalone/dedicated consumer),
// never when embedded inside another service.
func (p Profile) OwnsProcess() Profile {
	p.ownsProcess = true
	return p
}

// runtimeSettings resolves the process-global values this profile would apply.
// Pure — no side effects — so it can be unit-tested without mutating global state.
func (p Profile) runtimeSettings() (maxprocs, gcpercent int, memlimit int64) {
	return p.GOMAXPROCS, p.GCPercent, p.MemLimit
}

// PipelineOptions returns the profile's LOCAL (non-process-global) pipeline
// options — batch size, channel buffer, max-linger. The single source of truth for
// applying a profile to a pipeline; used by WithProfile and by the YAML/runner path
// (pkg/job) so a job's `profile:` field tunes the same knobs.
func (p Profile) PipelineOptions() []pipeline.Option {
	return []pipeline.Option{
		pipeline.WithBatchSize(p.BatchSize),
		pipeline.WithChannelBuffer(p.ChannelBuffer),
		pipeline.WithMaxLinger(p.MaxLinger),
	}
}

// ProfileByName returns the named preset ("sidecar"/"dedicated", case-insensitive)
// and whether it was found. Used by the YAML loader to resolve a `profile:` field.
func ProfileByName(name string) (Profile, bool) {
	switch strings.ToLower(name) {
	case "sidecar":
		return Sidecar, true
	case "dedicated":
		return Dedicated, true
	default:
		return Profile{}, false
	}
}

// WithProfile applies a resource profile. LOCAL knobs (batch/buffer/linger) are
// always applied. PROCESS-GLOBAL knobs are applied only if the profile came from
// OwnsProcess().
func WithProfile(p Profile) Option {
	return func(s *Stream) {
		s.popts = append(s.popts, p.PipelineOptions()...)
		if !p.ownsProcess {
			return
		}
		maxprocs, gcpercent, memlimit := p.runtimeSettings()
		if maxprocs > 0 {
			runtime.GOMAXPROCS(maxprocs)
		}
		if gcpercent > 0 {
			debug.SetGCPercent(gcpercent)
		}
		if memlimit > 0 {
			debug.SetMemoryLimit(memlimit)
		}
	}
}

// WithBatchSize sets the stage batch size (records per Operator call). Larger
// favours throughput, smaller favours latency.
func WithBatchSize(n int) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithBatchSize(n)) }
}

// WithChannelBuffer sets the default inter-stage channel buffer (a per-stage
// override still wins). Larger absorbs bursts; smaller is a tighter footprint.
func WithChannelBuffer(n int) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithChannelBuffer(n)) }
}

// WithMaxLinger flushes a partial batch at least every d, so a small batch size
// delivers low latency under sparse input. d<=0 disables it (the default).
func WithMaxLinger(d time.Duration) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithMaxLinger(d)) }
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
