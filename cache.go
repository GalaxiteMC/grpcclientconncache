package conncache

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"inet.af/netaddr"
	"sync"
	"time"
)

type grpcClientConnCache struct {
	sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	ttl time.Duration

	conns        map[netaddr.IPPort]*grpc.ClientConn
	pendingConns map[netaddr.IPPort]chan struct{}

	defaultDialOptions []grpc.DialOption
}

func NewGrpcClientConnCache(defaultDialOptions []grpc.DialOption) *grpcClientConnCache {
	ctx, cancel := context.WithCancel(context.Background())
	gcc := &grpcClientConnCache{
		ctx:    ctx,
		cancel: cancel,

		conns:        map[netaddr.IPPort]*grpc.ClientConn{},
		pendingConns: map[netaddr.IPPort]chan struct{}{},

		defaultDialOptions: defaultDialOptions,
	}
	return gcc
}

func (gcc *grpcClientConnCache) Get(ctx context.Context, addr netaddr.IPPort) (grpc.ClientConnInterface, error) {
	conn, err := gcc.checkPending(ctx, addr)
	if conn != nil || err != nil {
		return conn, err
	}

	if c := gcc.get(addr); c != nil {
		return c, nil
	}

	// Not in the cache

	c := make(chan struct{})
	gcc.Lock()
	gcc.pendingConns[addr] = c
	gcc.Unlock()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	opts = append(opts, gcc.defaultDialOptions...)

	conn, err = grpc.DialContext(ctx, addr.String(),
		opts...,
	)
	if err != nil {
		gcc.removePending(addr, c)
		return nil, err
	}

	go func() {
		if conn.(*grpc.ClientConn).WaitForStateChange(gcc.ctx, connectivity.Ready) {
			gcc.Lock()
			delete(gcc.conns, addr)
			gcc.Unlock()
			_ = conn.(*grpc.ClientConn).Close()
		}
	}()

	gcc.addToCache(addr, conn.(*grpc.ClientConn))
	gcc.removePending(addr, c)

	return conn, nil
}

func (gcc *grpcClientConnCache) removePending(addr netaddr.IPPort, c chan struct{}) {
	gcc.Lock()
	// Remove *after* connection is added to cache, to avoid misses
	delete(gcc.pendingConns, addr)
	close(c)
	gcc.Unlock()
}

func (gcc *grpcClientConnCache) addToCache(addr netaddr.IPPort, conn *grpc.ClientConn) {
	gcc.Lock()
	defer gcc.Unlock()

	gcc.conns[addr] = conn
}

func (gcc *grpcClientConnCache) checkPending(ctx context.Context, addr netaddr.IPPort) (grpc.ClientConnInterface, error) {
	gcc.RLock()
	if c, ok := gcc.pendingConns[addr]; ok {
		gcc.RUnlock()
		select {
		case _, _ = <-c:
			if c := gcc.get(addr); c != nil {
				return c, nil
			} else {
				return nil, &DialFailedError{}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	gcc.RUnlock()
	return nil, nil
}

func (gcc *grpcClientConnCache) get(addr netaddr.IPPort) grpc.ClientConnInterface {
	gcc.RLock()
	defer gcc.RUnlock()
	if c, ok := gcc.conns[addr]; ok {
		return c
	}
	return nil
}

func (gcc *grpcClientConnCache) Close() {
	gcc.cancel()

	gcc.Lock()
	defer gcc.Unlock()

	for k, v := range gcc.conns {
		delete(gcc.conns, k)

		v := v
		go func() {
			_ = v.Close()
		}()
	}
}
