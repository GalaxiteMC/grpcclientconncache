# gRPC Client Connection Cache

This library is designed to provide a central
cache of [ClientConn](https://pkg.go.dev/google.golang.org/grpc#ClientConn)'s
to make better use of the built in multiplexing
 and reduce the amount of TCP handshakes that 
need to take place.

The lifetime of a connection is managed through
the connectivity API. The go gRPC implementation
does not currently implement idle_timeout to
automatically change its state to idle, we can
force this from the server side using the
GOAWAY mechanism which is implemented easily
with this `ServerOption`:

```go
grpc.KeepaliveParams(keepalive.ServerParameters{
    MaxConnectionIdle: 60 * time.Second,
})
```

This way if the client connection does not have
an open stream, or if it hasn't done an RPC call
in `MaxConnectionIdle` duration then it will
automatically remove the connection from the
cache and perform a graceful closure.

The connectivity API will also change state
when there's a problem with the connection etc.
But do be aware there is a small period of time
where broken connections may be served up
before a connection issue is detected. You
can read more in the [connectivity semantics
and api](https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md) doc.