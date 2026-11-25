package kvv1_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	kvv1 "github.com/20001172024/stormproof/proto/stormproof/kv/v1"
)

// stubServer answers every call with fixed values. It exists only to prove
// the generated code compiles and that requests and responses survive a
// real gRPC round trip; the actual KV logic arrives in M5.
type stubServer struct {
	kvv1.UnimplementedKVServiceServer
}

func (stubServer) Get(_ context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	return &kvv1.GetResponse{Found: true, Value: append([]byte("value-of-"), req.GetKey()...)}, nil
}

func (stubServer) CompareAndSwap(_ context.Context, req *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	return &kvv1.CompareAndSwapResponse{Swapped: false, Found: true, CurrentValue: []byte("other")}, nil
}

// dial starts the stub server on an in-memory listener (bufconn) and
// returns a client connected to it. No real network port is used.
func dial(t *testing.T) kvv1.KVServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	kvv1.RegisterKVServiceServer(srv, stubServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return kvv1.NewKVServiceClient(conn)
}

func TestRoundTrip(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	got, err := c.Get(ctx, &kvv1.GetRequest{Key: []byte("foo")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || string(got.GetValue()) != "value-of-foo" {
		t.Errorf("Get = %v, want found value-of-foo", got)
	}

	cas, err := c.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
		Header:        &kvv1.RequestHeader{ClientId: 7, RequestId: 1},
		Key:           []byte("foo"),
		ExpectedValue: []byte("old"),
		NewValue:      []byte("new"),
	})
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if cas.GetSwapped() || string(cas.GetCurrentValue()) != "other" {
		t.Errorf("CompareAndSwap = %v, want not swapped, current value other", cas)
	}
}

// An RPC the server hasn't implemented must fail cleanly, not hang or panic.
func TestUnimplemented(t *testing.T) {
	c := dial(t)
	if _, err := c.Put(context.Background(), &kvv1.PutRequest{Key: []byte("k")}); err == nil {
		t.Fatal("Put on stub server succeeded, want Unimplemented error")
	}
}
