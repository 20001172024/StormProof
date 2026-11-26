package check

import (
	"testing"

	"github.com/20001172024/stormproof/kv"
)

func get(client int, key string, call, ret int64, found bool, value string) Op {
	r := kv.Result{Found: found}
	if found {
		r.Value = []byte(value)
	}
	return Op{ClientID: client, Cmd: kv.Command{Op: kv.OpGet, Key: key}, Call: call, Return: ret, Done: true, Result: r}
}

func put(client int, key, value string, call, ret int64) Op {
	return Op{ClientID: client, Cmd: kv.Command{Op: kv.OpPut, Key: key, Value: []byte(value)}, Call: call, Return: ret, Done: true}
}

func cas(client int, key, expected, value string, call, ret int64, swapped bool) Op {
	return Op{ClientID: client, Cmd: kv.Command{Op: kv.OpCAS, Key: key, Expected: []byte(expected), Value: []byte(value)},
		Call: call, Return: ret, Done: true, Result: kv.Result{Swapped: swapped}}
}

func TestLinearizable(t *testing.T) {
	cases := []struct {
		name string
		ops  []Op
		want bool
	}{
		{"sequential put then get", []Op{put(0, "x", "1", 0, 10), get(1, "x", 20, 30, true, "1")}, true},
		{"stale read after a completed write", []Op{put(0, "x", "1", 0, 10), get(1, "x", 20, 30, false, "")}, false},
		{"concurrent read may see either", []Op{put(0, "x", "1", 0, 30), get(1, "x", 10, 20, false, "")}, true},
		{"concurrent read may see either (new)", []Op{put(0, "x", "1", 0, 30), get(1, "x", 10, 20, true, "1")}, true},
		{"read goes back in time", []Op{
			put(0, "x", "1", 0, 10), put(0, "x", "2", 20, 30),
			get(1, "x", 40, 50, true, "2"), get(2, "x", 60, 70, true, "1"),
		}, false},
		{"cas reports failure though it swapped", []Op{
			put(0, "x", "a", 0, 10), cas(1, "x", "a", "b", 20, 30, false), get(2, "x", 40, 50, true, "b"),
		}, false},
		{"cas succeeds", []Op{put(0, "x", "a", 0, 10), cas(1, "x", "a", "b", 20, 30, true), get(2, "x", 40, 50, true, "b")}, true},
		{"keys are independent", []Op{put(0, "x", "1", 0, 10), get(1, "y", 20, 30, false, "")}, true},
		{"unanswered write may have happened", []Op{
			{ClientID: 0, Cmd: kv.Command{Op: kv.OpPut, Key: "x", Value: []byte("1")}, Call: 0},
			get(1, "x", 20, 30, true, "1"),
		}, true},
		{"unanswered write may not have happened", []Op{
			{ClientID: 0, Cmd: kv.Command{Op: kv.OpPut, Key: "x", Value: []byte("1")}, Call: 0},
			get(1, "x", 20, 30, false, ""),
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Linearizable(c.ops); got != c.want {
				t.Fatalf("Linearizable = %v, want %v", got, c.want)
			}
		})
	}
}
