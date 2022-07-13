package conncache

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"inet.af/netaddr"
	"log"
	"net"
	"testing"
	"time"
)

func TestGrpcClientCache(t *testing.T) {
	var addresses []string
	for i := 10000; i < 10100; i++ {
		addresses = append(addresses, fmt.Sprintf("127.0.0.1:%d", i))
	}

	var servers []*grpc.Server

	for _, address := range addresses {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			log.Fatalf("failed to listen: %s", err)
		}
		var opts []grpc.ServerOption
		opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Second, // Ample time for iterations
		}))
		grpcServer := grpc.NewServer(opts...)
		servers = append(servers, grpcServer)
		go func() {
			err := grpcServer.Serve(lis)
			if err != nil {
				t.Errorf("unable to bind: %s", err)
				return
			}
		}()
	}

	c := DefaultClientConnCache

	c.RLock()
	clength := len(c.conns)
	c.RUnlock()
	if clength != 0 {
		t.Fatalf("connections should be 0, got %d", clength)
	}

	for _, address := range addresses {
		ad, _ := netaddr.ParseIPPort(address)

		for i := 0; i < 50; i++ {
			ctx, _ := context.WithTimeout(context.Background(), 20*time.Millisecond)
			conn, err := c.Get(ctx, ad)
			if err != nil {
				t.Fatalf("failed to get a connection: %s", err)
			}

			if conn == nil {
				t.Fatalf("connection is nil")
			}
		}
	}

	c.RLock()
	clength = len(c.conns)
	c.RUnlock()
	if clength != 100 {
		t.Fatalf("connections should be 100, got %d", clength)
	}

	time.Sleep(6 * time.Second) // Let server send GOAWAY

	c.RLock()
	clength = len(c.conns)
	c.RUnlock()
	if clength != 0 {
		t.Fatalf("connections should be 0, got %d", clength)
	}

	for _, address := range addresses {
		ad, _ := netaddr.ParseIPPort(address)

		for i := 0; i < 50; i++ {
			ctx, _ := context.WithTimeout(context.Background(), 20*time.Millisecond)
			conn, err := c.Get(ctx, ad)
			if err != nil {
				t.Fatalf("failed to get a connection: %s", err)
			}

			if conn == nil {
				t.Fatalf("connection is nil")
			}
		}
	}

	c.RLock()
	clength = len(c.conns)
	c.RUnlock()
	if clength != 100 {
		t.Fatalf("connections should be 100, got %d", clength)
	}

	c.Close() // Clean up the cache and all connections

	c.RLock()
	clength = len(c.conns)
	c.RUnlock()
	if clength != 0 {
		t.Fatalf("connections should be 0, got %d", clength)
	}

	for _, server := range servers {
		server.Stop()
	}
}
