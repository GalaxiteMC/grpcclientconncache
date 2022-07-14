package conncache

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"inet.af/netaddr"
	"sync"
	"time"
)

// DefaultClientConnCache provides a useful global of grpcClientConnCache
var DefaultClientConnCache = NewGrpcClientConnCache([]grpc.DialOption{
	grpc.WithTransportCredentials(insecure.NewCredentials()),
})

// grpcClientConnCache is the container for client connections
type grpcClientConnCache struct {
	l sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	ttl time.Duration

	conns        map[netaddr.IPPort]*grpc.ClientConn
	pendingConns map[netaddr.IPPort]chan struct{}

	defaultDialOptions []grpc.DialOption
}

// NewGrpcClientConnCache returns a new grpcClientConnCache
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

// Get takes a context and a target address. You should make effort to provide a deadline for
// the context; where none exists, we set this to 500 milliseconds in an effort to keep
// pendingConns quick. A strong effort is made to queue requests where an existing request
// is in flight. Where a connection is already established it will be pulled from the connection
// map. Where there is no in flight requests, nor existing connections, a new connection is established
func (gcc *grpcClientConnCache) Get(ctx context.Context, addr netaddr.IPPort) (grpc.ClientConnInterface, error) {
	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
	}

	conn, err := gcc.checkPending(ctx, addr)
	if conn != nil || err != nil {
		return conn, err
	}

	if c := gcc.get(addr); c != nil {
		return c, nil
	}

	// Not in the cache

	c := make(chan struct{})
	gcc.l.Lock()
	gcc.pendingConns[addr] = c
	gcc.l.Unlock()

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

	go gcc.watch(conn, addr)

	gcc.addToCache(addr, conn.(*grpc.ClientConn))
	gcc.removePending(addr, c)

	return conn, nil
}

// watch will start after a connection is set to ready, it will then monitor for changes
// of the connectivity state. None of the states in the connectivity api are desirable
// and indicate either inactivity, connectivity problems or something else.
func (gcc *grpcClientConnCache) watch(conn grpc.ClientConnInterface, addr netaddr.IPPort) {
	if conn.(*grpc.ClientConn).WaitForStateChange(gcc.ctx, connectivity.Ready) {
		gcc.l.Lock()
		delete(gcc.conns, addr)
		gcc.l.Unlock()
		_ = conn.(*grpc.ClientConn).Close()
	}
}

// removePending will remove the pending connection and then close the channel for
// any listener. This acts as a broadcast for all listeners to then pull from the cache
func (gcc *grpcClientConnCache) removePending(addr netaddr.IPPort, c chan struct{}) {
	gcc.l.Lock()
	// Remove *after* connection is added to cache, to avoid misses
	delete(gcc.pendingConns, addr)
	close(c)
	gcc.l.Unlock()
}

// addToCache will add a connection to the connection map to be cached
func (gcc *grpcClientConnCache) addToCache(addr netaddr.IPPort, conn *grpc.ClientConn) {
	gcc.l.Lock()
	defer gcc.l.Unlock()

	gcc.conns[addr] = conn
}

// checkPending will check if there is a pending connection in flight, if there is it
// will wait for the broadcast, either getting the requested connection or in the event
// of connection failure it will return `DialFailedError`. If no broadcast is received before
// the context times out the timeout error is returned
func (gcc *grpcClientConnCache) checkPending(ctx context.Context, addr netaddr.IPPort) (grpc.ClientConnInterface, error) {
	gcc.l.RLock()
	if c, ok := gcc.pendingConns[addr]; ok {
		gcc.l.RUnlock()
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
	gcc.l.RUnlock()
	return nil, nil
}

// get returns any connection if it exists
func (gcc *grpcClientConnCache) get(addr netaddr.IPPort) grpc.ClientConnInterface {
	gcc.l.RLock()
	defer gcc.l.RUnlock()
	if c, ok := gcc.conns[addr]; ok {
		return c
	}
	return nil
}

// Close will cancel the internal context which triggers the `WaitForStateChange` to
// exit their goroutines. It then deletes all connections from the map and closes the
// connections, leaving no hanging connections
func (gcc *grpcClientConnCache) Close() {
	gcc.cancel()

	gcc.l.Lock()
	defer gcc.l.Unlock()

	for k, v := range gcc.conns {
		delete(gcc.conns, k)

		v := v
		go func() {
			_ = v.Close()
		}()
	}
}
