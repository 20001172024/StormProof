package main

import "testing"

func TestParseCluster(t *testing.T) {
	m, err := parseCluster("1=a:1, 2=b:2,3=c:3")
	if err != nil || len(m) != 3 || m[2] != "b:2" {
		t.Fatalf("%v %v", m, err)
	}
	for _, bad := range []string{"", "1", "x=a:1", "0=a:1", "1=a:1,1=b:2", "1="} {
		if _, err := parseCluster(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestIDFromHostname(t *testing.T) {
	good := map[string]uint64{"stormproof-0": 1, "stormproof-2": 3, "a-b-c-10": 11}
	for host, want := range good {
		if got, err := idFromHostname(host); err != nil || got != want {
			t.Errorf("%q: got %d, %v; want %d", host, got, err, want)
		}
	}
	for _, host := range []string{"stormproof", "stormproof-", "stormproof-x", "node-1a"} {
		if _, err := idFromHostname(host); err == nil {
			t.Errorf("%q accepted", host)
		}
	}
}
