package sim

// NodeID identifies a node. IDs start at 1, so 0 means "no node".
type NodeID int

// Message is a message between two nodes. The simulator routes it but never
// looks inside Body, except to write it to the trace.
type Message struct {
	From NodeID
	To   NodeID
	Body Body
}

// Body is the content of a message. It must be a struct (not a map) so it
// encodes to the same JSON every time.
type Body interface {
	// Kind is a short name for the message type, e.g. "RequestVote".
	Kind() string
}
