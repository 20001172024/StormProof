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
