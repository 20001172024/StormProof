package sim

import "testing"

func draws(r *Rand, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = r.Intn(1_000_000)
	}
	return out
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRandSameSeedSameStream(t *testing.T) {
	a := draws(NewRand(42, "network"), 100)
	b := draws(NewRand(42, "network"), 100)
	if !equal(a, b) {
		t.Fatal("same seed and stream produced different sequences")
	}
}

func TestRandStreamsDiffer(t *testing.T) {
	cases := []struct {
		name    string
		seedA   uint64
		streamA string
		seedB   uint64
		streamB string
	}{
		{"different stream", 42, "network", 42, "faults"},
		{"different seed", 42, "network", 43, "network"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := draws(NewRand(c.seedA, c.streamA), 100)
			b := draws(NewRand(c.seedB, c.streamB), 100)
			if equal(a, b) {
				t.Fatal("expected different sequences")
			}
		})
	}
}

// Drawing from one stream must not affect another stream.
func TestRandStreamsIndependent(t *testing.T) {
	want := draws(NewRand(7, "faults"), 50)

	net := NewRand(7, "network")
	faults := NewRand(7, "faults")
	got := make([]int, 50)
	for i := range got {
		net.Intn(10) // interleave draws from another stream
		got[i] = faults.Intn(1_000_000)
	}
	if !equal(got, want) {
		t.Fatal("draws from one stream changed another stream's sequence")
	}
}

func TestRandChanceExtremes(t *testing.T) {
	r := NewRand(1, "x")
	for i := 0; i < 1000; i++ {
		if r.Chance(0) {
			t.Fatal("Chance(0) returned true")
		}
		if !r.Chance(1) {
			t.Fatal("Chance(1) returned false")
		}
	}
}

func TestRandBetween(t *testing.T) {
	r := NewRand(1, "x")
	for i := 0; i < 10_000; i++ {
		v := r.Between(-5*Millisecond, 5*Millisecond)
		if v < -5*Millisecond || v >= 5*Millisecond {
			t.Fatalf("Between returned %v, outside [-5ms, 5ms)", v)
		}
	}
	if got := r.Between(3, 3); got != 3 {
		t.Fatalf("Between(3, 3) = %v, want 3", got)
	}
}

func TestTimeString(t *testing.T) {
	if got := (1*Second + 250*Millisecond).String(); got != "1.250000s" {
		t.Fatalf("got %q", got)
	}
}
