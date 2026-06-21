//go:build !race

package bench

// raceEnabled is false in normal (non-race) builds; throughput-floor tests run.
const raceEnabled = false
