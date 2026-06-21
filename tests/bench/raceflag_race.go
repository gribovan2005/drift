//go:build race

package bench

// raceEnabled is true when the test binary is built with -race. Throughput-floor
// regression tests skip in this mode: race instrumentation distorts timing by
// 2-10x, so the measured rate reflects the detector, not a real regression.
const raceEnabled = true
