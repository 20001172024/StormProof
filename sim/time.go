package sim

import "fmt"

// Time is virtual time in microseconds since the start of a simulation.
//
// It is just an integer that only the event loop moves forward. Nothing in
// the simulator reads the wall clock, so a run takes the same virtual time
// however fast or slow the machine running it is.
type Time int64

// Units of virtual time.
const (
	Microsecond Time = 1
	Millisecond      = 1000 * Microsecond
	Second           = 1000 * Millisecond
)

// String formats t as seconds with microsecond precision, e.g. "1.250000s".
func (t Time) String() string {
	return fmt.Sprintf("%d.%06ds", t/Second, t%Second)
}
