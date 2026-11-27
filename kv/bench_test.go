package kv

import (
	"fmt"
	"testing"
)

// BenchmarkApply measures the state machine alone: decoding a command and
// applying it, with client sessions, for Puts and Gets over 1,000 keys.
func BenchmarkApply(b *testing.B) {
	value := make([]byte, 100)
	for _, op := range []Op{OpPut, OpGet} {
		b.Run(op.String(), func(b *testing.B) {
			s := NewStore()
			cmds := make([][]byte, 1000)
			for i := range cmds {
				cmds[i] = Command{ClientID: 1, Op: op, Key: fmt.Sprintf("k%d", i), Value: value}.Encode()
			}
			b.ResetTimer()
			for i := range b.N {
				c, err := Decode(cmds[i%len(cmds)])
				if err != nil {
					b.Fatal(err)
				}
				c.RequestID = uint64(i + 1)
				s.Apply(c)
			}
		})
	}
}
