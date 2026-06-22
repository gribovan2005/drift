//go:build !race

package nexmark

// raceEnabled is false in normal (non-race) builds; throughput runs proceed.
const raceEnabled = false
