package server

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/20001172024/stormproof/kv"
	kvv1 "github.com/20001172024/stormproof/proto/stormproof/kv/v1"
	"github.com/20001172024/stormproof/server/host"
)

// Trailer keys a server sets when it is not the leader, so the client can
// go straight to the one it knows of.
const (
	LeaderAddrTrailer = "stormproof-leader-addr"
	LeaderIDTrailer   = "stormproof-leader-id"
)

// kvService implements the client API by passing each request to the event
// loop and waiting for its response.
type kvService struct {
	kvv1.UnimplementedKVServiceServer
	s *Server
}

// do runs one KV command through the loop and turns the outcome into a gRPC
// result. Not the leader: FailedPrecondition, with the leader's address in
// the trailer if known. Outcome unknown or not ready: Unavailable, so the
// client retries with the same request ID.
func (k *kvService) do(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	c := call{req: host.Request{Cmd: cmd}, resp: make(chan host.Response, 1)}
	select {
	case k.s.calls <- c:
	case <-k.s.stop:
		return kv.Result{}, status.Error(codes.Unavailable, "server stopping")
	case <-ctx.Done():
		return kv.Result{}, status.FromContextError(ctx.Err()).Err()
	}
	var r host.Response
	select {
	case r = <-c.resp:
	case <-k.s.done:
		return kv.Result{}, status.Error(codes.Unavailable, "server stopped")
	case <-ctx.Done():
		// The request may still commit; the client retries with the same
		// request ID, and dedup makes that safe.
		return kv.Result{}, status.FromContextError(ctx.Err()).Err()
	}
	switch r.Status {
	case host.StatusOK:
		return r.Result, nil
	case host.StatusNotLeader:
		if addr, ok := k.s.cfg.Cluster[r.Leader]; ok && r.Leader != k.s.cfg.ID {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderAddrTrailer, addr, LeaderIDTrailer, strconv.FormatUint(uint64(r.Leader), 10)))
		}
		return kv.Result{}, status.Error(codes.FailedPrecondition, "not the leader")
	default:
		return kv.Result{}, status.Error(codes.Unavailable, "retry")
	}
}

func command(h *kvv1.RequestHeader, op kv.Op, key []byte) kv.Command {
	return kv.Command{ClientID: h.GetClientId(), RequestID: h.GetRequestId(), Op: op, Key: string(key)}
}

func (k *kvService) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	r, err := k.do(ctx, kv.Command{Op: kv.OpGet, Key: string(req.GetKey())})
	if err != nil {
		return nil, err
	}
	return &kvv1.GetResponse{Found: r.Found, Value: r.Value}, nil
}

func (k *kvService) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	cmd := command(req.GetHeader(), kv.OpPut, req.GetKey())
	cmd.Value = req.GetValue()
	if _, err := k.do(ctx, cmd); err != nil {
		return nil, err
	}
	return &kvv1.PutResponse{}, nil
}

func (k *kvService) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	r, err := k.do(ctx, command(req.GetHeader(), kv.OpDelete, req.GetKey()))
	if err != nil {
		return nil, err
	}
	return &kvv1.DeleteResponse{Existed: r.Existed}, nil
}

func (k *kvService) CompareAndSwap(ctx context.Context, req *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	cmd := command(req.GetHeader(), kv.OpCAS, req.GetKey())
	cmd.Expected, cmd.ExpectAbsent, cmd.Value = req.GetExpectedValue(), req.GetExpectAbsent(), req.GetNewValue()
	r, err := k.do(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &kvv1.CompareAndSwapResponse{Swapped: r.Swapped, Found: r.Found, CurrentValue: r.Value}, nil
}
