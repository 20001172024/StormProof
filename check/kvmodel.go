package check

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"

	"github.com/20001172024/stormproof/kv"
)

// Op is one client operation in a history: what the client asked for, when
// it sent the request, and, if it got an answer, what and when.
//
// Times are whatever clock the client used; in the simulator, virtual
// microseconds. An operation that never got an answer (Done false) may or
// may not have taken effect, and the checker considers both.
type Op struct {
	ClientID int
	Cmd      kv.Command
	Call     int64
	Return   int64 // ignored unless Done
	Done     bool
	Result   kv.Result
}

// keyState is the model's state for one key.
type keyState struct {
	present bool
	value   string
}

type output struct {
	done   bool
	result kv.Result
}

// kvModel is the sequential specification of the store, one key at a time.
// Operations on different keys never affect each other, so the history is
// split by key and each part checked on its own. That keeps checking fast:
// Porcupine's search is exponential in the number of concurrent operations,
// and splitting divides them up.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(kv.Command).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		sort.Strings(keys)
		parts := make([][]porcupine.Operation, len(keys))
		for i, k := range keys {
			parts[i] = byKey[k]
		}
		return parts
	},
	Init: func() interface{} { return keyState{} },
	Step: func(state, input, out interface{}) (bool, interface{}) {
		st, cmd, o := state.(keyState), input.(kv.Command), out.(output)
		switch cmd.Op {
		case kv.OpGet:
			if !o.done {
				return true, st
			}
			return o.result.Found == st.present && string(o.result.Value) == st.value, st
		case kv.OpPut:
			return true, keyState{present: true, value: string(cmd.Value)}
		case kv.OpDelete:
			if o.done && o.result.Existed != st.present {
				return false, st
			}
			return true, keyState{}
		case kv.OpCAS:
			match := !st.present
			if !cmd.ExpectAbsent {
				match = st.present && st.value == string(cmd.Expected)
			}
			if o.done && o.result.Swapped != match {
				return false, st
			}
			if match {
				return true, keyState{present: true, value: string(cmd.Value)}
			}
			return true, st
		}
		return false, st
	},
	DescribeOperation: func(input, out interface{}) string {
		cmd, o := input.(kv.Command), out.(output)
		res := "?"
		if o.done {
			res = fmt.Sprintf("%+v", o.result)
		}
		return fmt.Sprintf("%s(%q) -> %s", cmd.Op, cmd.Key, res)
	},
}

// Linearizable reports whether the history is linearizable with respect to
// the key-value model.
//
// Operations without an answer get a return time after every other
// operation. Porcupine must still place them somewhere, but placing a write
// last is the same as it never having happened, so this covers both "it
// took effect" and "it didn't". Reads without an answer tell us nothing and
// are dropped.
func Linearizable(ops []Op) bool {
	var end int64
	for _, op := range ops {
		end = max(end, op.Call, op.Return)
	}
	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		if !op.Done && op.Cmd.Op == kv.OpGet {
			continue
		}
		ret := op.Return
		if !op.Done {
			ret = end + 1
		}
		history = append(history, porcupine.Operation{
			ClientId: op.ClientID,
			Input:    op.Cmd,
			Call:     op.Call,
			Output:   output{done: op.Done, result: op.Result},
			Return:   ret,
		})
	}
	return porcupine.CheckOperations(kvModel, history)
}
