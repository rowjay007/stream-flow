package raft

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case *[]byte:
		return *b, nil
	default:
		return nil, fmt.Errorf("rawCodec: unsupported marshal type %T", v)
	}
}

func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	switch p := v.(type) {
	case *[]byte:
		*p = append((*p)[:0], data...)
		return nil
	default:
		return fmt.Errorf("rawCodec: unsupported unmarshal type %T", v)
	}
}

func (rawCodec) Name() string { return "raw" }

var raftStreamDesc = &grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true}

// GRPCTransport implements the Transport interface using gRPC. It exposes a
// simple unary RPC "Send" that carries raw bytes; the server side registers
// a handler callback invoked for incoming payloads.
type GRPCTransport struct {
	mu      sync.Mutex
	ln      net.Listener
	srv     *grpc.Server
	conns   map[string]*grpc.ClientConn
	handler func([]byte) error
	// per-peer persistent client streams reuse
	streams    map[string]*clientStream
	tls        bool
	caPool     *x509.CertPool
	clientCert *tls.Certificate
}

type clientStream struct {
	mu     sync.Mutex
	stream grpc.ClientStream
}

// NewGRPCTransport starts a gRPC server listening on the provided address.
// Use address ":0" to auto-assign a free port; call Addr() to retrieve the
// bound address.
func NewGRPCTransport(listenAddr, serverCertFile, serverKeyFile, caFile, clientCertFile, clientKeyFile string) (*GRPCTransport, error) {
	encoding.RegisterCodec(rawCodec{})
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	g := &GRPCTransport{ln: ln, conns: make(map[string]*grpc.ClientConn), streams: make(map[string]*clientStream)}

	var opts []grpc.ServerOption
	if serverCertFile != "" && serverKeyFile != "" {
		// Load server cert
		serverCert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{serverCert}}
		if caFile != "" {
			// require and verify client certs signed by CA
			caBytes, err := os.ReadFile(caFile)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caBytes) {
				return nil, fmt.Errorf("failed to append CA cert")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		g.tls = true
	}
	// If client cert is provided, load it for use when dialing peers
	if clientCertFile != "" && clientKeyFile != "" {
		c, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, err
		}
		g.clientCert = &c
	}
	if caFile != "" {
		caBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		g.caPool = pool
	}
	g.srv = grpc.NewServer(opts...)
	// Register a streaming service "raft.Raft" with method "Stream" for
	// bidirectional raw byte streaming. The server handler reads incoming
	// []byte messages and invokes the registered handler callback.
	sd := &grpc.ServiceDesc{
		ServiceName: "raft.Raft",
		HandlerType: (*interface{})(nil),
		Streams: []grpc.StreamDesc{{
			StreamName: "Stream",
			Handler: func(srv interface{}, stream grpc.ServerStream) error {
				for {
					var payload []byte
					if err := stream.RecvMsg(&payload); err != nil {
						if err == io.EOF {
							return nil
						}
						return err
					}
					if g.handler != nil {
						if err := g.handler(payload); err != nil {
							return err
						}
					}
				}
			},
			ServerStreams: true,
			ClientStreams: true,
		}},
	}
	g.srv.RegisterService(sd, nil)
	go g.srv.Serve(ln)
	return g, nil
}

func (g *GRPCTransport) Addr() string {
	if g == nil || g.ln == nil {
		return ""
	}
	return g.ln.Addr().String()
}

// RegisterHandler sets the callback invoked for incoming payloads.
func (g *GRPCTransport) RegisterHandler(h func([]byte) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handler = h
}

func (g *GRPCTransport) getConn(addr string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[addr]; ok {
		return c, nil
	}
	var conn *grpc.ClientConn
	var err error
	// Configure TLS for client dialing based on available CA / client certs
	if g.tls {
		// If CA is available, use it to verify server certs; otherwise use insecure
		// credentials (fallback).
		if g.caPool != nil {
			tlsCfg := &tls.Config{RootCAs: g.caPool}
			if g.clientCert != nil {
				tlsCfg.Certificates = []tls.Certificate{*g.clientCert}
			}
			conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
		} else {
			conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	} else {
		conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if err != nil {
		return nil, err
	}
	g.conns[addr] = conn
	return conn, nil
}

func (g *GRPCTransport) Send(ctx context.Context, to string, payload []byte) error {
	conn, err := g.getConn(to)
	if err != nil {
		return err
	}
	// reuse or create a persistent client stream for this peer
	g.mu.Lock()
	cs := g.streams[to]
	if cs == nil {
		// create new stream
		stream, err := conn.NewStream(ctx, raftStreamDesc, "/raft.Raft/Stream", grpc.ForceCodec(rawCodec{}))
		if err != nil {
			g.mu.Unlock()
			return err
		}
		cs = &clientStream{stream: stream}
		g.streams[to] = cs
	}
	g.mu.Unlock()

	cs.mu.Lock()
	defer cs.mu.Unlock()
	if err := cs.stream.SendMsg(payload); err != nil {
		// on error, try to recreate the stream once
		_ = cs.stream.CloseSend()
		stream, err2 := conn.NewStream(ctx, raftStreamDesc, "/raft.Raft/Stream", grpc.ForceCodec(rawCodec{}))
		if err2 != nil {
			return err
		}
		cs.stream = stream
		if err := cs.stream.SendMsg(payload); err != nil {
			return err
		}
	}
	return nil
}

func (g *GRPCTransport) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.srv != nil {
		g.srv.Stop()
	}
	if g.ln != nil {
		g.ln.Close()
	}
	for k, c := range g.conns {
		c.Close()
		delete(g.conns, k)
	}
	return nil
}
