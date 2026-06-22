//go:build race

package nexmark

// raceEnabled is true when the test binary is built with -race. The Nexmark
// throughput runs are measurements (events/sec), not correctness checks: race
// instrumentation distorts timing by 2-10x and the 2M/50M-event workloads blow
// past CI timeouts, so they skip under -race.
const raceEnabled = true
