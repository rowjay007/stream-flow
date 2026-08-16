package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	raftlib "go.etcd.io/etcd/raft/v3"
)

func TestThreeNodeInProcClusterReplication(t *testing.T) {
	// Use gRPC transports listening on ephemeral ports for each node.
	transports := make([]*GRPCTransport, 3)
	defer func() {
		for _, tr := range transports {
			if tr != nil {
				tr.Close()
			}
		}
	}()

	// Create three nodes with WAL-backed storage directories
	nodes := make([]*Node, 3)
	// Prepare peers list for the cluster
	peers := []raftlib.Peer{}
	for i := 0; i < 3; i++ {
		peers = append(peers, raftlib.Peer{ID: uint64(i + 1)})
	}

	// Generate CA and per-node certs for mutual TLS
	caCertPEM, caKey, err := generateCA()
	if err != nil {
		t.Fatalf("gen ca: %v", err)
	}

	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", "raftnode")
		if err != nil {
			t.Fatalf("mktemp: %v", err)
		}
		defer os.RemoveAll(dir)
		store, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
		if err != nil {
			t.Fatalf("wal store: %v", err)
		}

		// Generate node cert signed by CA
		certPEM, keyPEM, err := generateCertForHost(caCertPEM, caKey, "127.0.0.1")
		if err != nil {
			t.Fatalf("gen cert: %v", err)
		}
		certFile := filepath.Join(dir, "node.crt")
		keyFile := filepath.Join(dir, "node.key")
		caFile := filepath.Join(dir, "ca.crt")
		if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
			t.Fatalf("write cert: %v", err)
		}
		if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
			t.Fatalf("write key: %v", err)
		}
		if err := os.WriteFile(caFile, caCertPEM, 0o644); err != nil {
			t.Fatalf("write ca: %v", err)
		}

		// Start a gRPC transport for this node (mutual TLS configured)
		tr, err := NewGRPCTransport(":0", certFile, keyFile, caFile, certFile, keyFile)
		if err != nil {
			t.Fatalf("start transport: %v", err)
		}
		transports[i] = tr

		nodes[i] = NewNode(uint64(i+1), store, tr)
		// Set initial peers for bootstrapping the cluster using numeric IDs
		nodes[i].peers = make([]raftlib.Peer, len(peers))
		for j := range peers {
			nodes[i].peers[j] = peers[j]
		}
		if err := nodes[i].Start(context.Background()); err != nil {
			t.Fatalf("start node: %v", err)
		}
		defer nodes[i].Stop()
	}

	// Populate peer address maps so nodes can resolve raft peer IDs to
	// gRPC addresses when sending messages.
	for i := 0; i < 3; i++ {
		nodes[i].peerAddrs = make(map[uint64]string)
		for j := 0; j < 3; j++ {
			nodes[i].peerAddrs[uint64(j+1)] = transports[j].Addr()
		}
	}

	// Propose a value to node 1
	if err := nodes[0].Propose(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Retry loop up to 2s to allow leader election and replication
	deadline := time.Now().Add(2 * time.Second)
	for {
		allOk := true
		for i := 0; i < 3; i++ {
			data, err := nodes[i].store.ReadWAL(0)
			if err != nil || len(data) == 0 {
				allOk = false
				break
			}
		}
		if allOk {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replication did not complete in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
