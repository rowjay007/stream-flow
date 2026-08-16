package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	raftlib "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
)

// Node wraps the etcd/raft Node and provides lifecycle methods. This is a
// minimal implementation to start an in-memory raft node for local testing.
type Node struct {
	id        uint64
	store     Storage
	tr        Transport
	node      raftlib.Node
	stopc     chan struct{}
	peers     []raftlib.Peer
	peerAddrs map[uint64]string
}

// NewNode constructs a Node. It uses the provided Storage and Transport
// but currently initializes an in-memory raft storage for demo purposes.
func NewNode(id uint64, store Storage, tr Transport) *Node {
	log.Printf("raft: create node id=%d", id)
	return &Node{id: id, store: store, tr: tr, stopc: make(chan struct{})}
}

func (n *Node) Start(ctx context.Context) error {
	// Create an in-memory storage for the raft state machine. In Phase 2
	// we will wire this to durable WAL + snapshot storage backed by
	// broker segment files.
	mem := raftlib.NewMemoryStorage()
	var storage raftlib.Storage = mem
	var appendable interface {
		Append([]raftpb.Entry) error
		ApplySnapshot(raftpb.Snapshot) error
	} = mem

	// If we have a persistent store, load snapshot (if present) and wrap
	// the memory storage so Append persists entries to the WAL as raft produces them.
	if n.store != nil {
		// Load snapshot and apply to memory storage before replaying WAL
		if rc, err := n.store.LoadSnapshot(); err == nil && rc != nil {
			// Read entire snapshot and apply
			var snapBytes []byte
			snapBytes, _ = io.ReadAll(rc)
			rc.Close()
			if len(snapBytes) > 0 {
				// create a dummy Snapshot with Data set to snapBytes
				var s raftpb.Snapshot
				s.Data = snapBytes
				_ = mem.ApplySnapshot(s)
			}
		}
		if data, err := n.store.ReadWAL(0); err == nil && len(data) > 0 {
			if ents, err := unmarshalEntries(data); err == nil && len(ents) > 0 {
				// Create persistent memory wrapper and populate with loaded ents
				pm := newPersistentMemory(mem, n.store)
				storage = pm
				appendable = pm
				_ = pm.Append(ents)
			}
		}
	}

	cfg := raftlib.Config{
		ID:              n.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	// Start a single-node cluster for now.
	peers := n.peers
	if len(peers) == 0 {
		peers = []raftlib.Peer{{ID: n.id}}
	}
	n.node = raftlib.StartNode(&cfg, peers)

	// If using an in-process transport, register a receive channel so
	// other nodes can send messages directly to this node.
	if ipt, ok := n.tr.(*InProcTransport); ok {
		addr := fmt.Sprintf("%d", n.id)
		ch, err := ipt.Register(addr)
		if err == nil {
			go func() {
				for b := range ch {
					var m raftpb.Message
					if err := proto.Unmarshal(b, &m); err != nil {
						continue
					}
					_ = n.node.Step(ctx, m)
				}
			}()
		}
	}

	// If using a gRPC transport, register a handler that steps incoming
	// raft messages into this node.
	if gt, ok := n.tr.(*GRPCTransport); ok {
		gt.RegisterHandler(func(b []byte) error {
			var m raftpb.Message
			if err := proto.Unmarshal(b, &m); err != nil {
				return err
			}
			return n.node.Step(ctx, m)
		})
	}

	go func() {
		for {
			select {
			case <-n.stopc:
				return
			case rd := <-n.node.Ready():
				// Apply snapshot if present.
				if !raftlib.IsEmptySnap(rd.Snapshot) {
					_ = appendable.ApplySnapshot(rd.Snapshot)
				}
				// Persist entries to WAL (if configured) and append to
				// in-memory storage so raft can proceed.
				if len(rd.Entries) > 0 {
					// Persist entries to WAL (if configured) and append to
					// in-memory storage so raft can proceed.
					if n.store != nil {
						if b, err := marshalEntries(rd.Entries); err == nil {
							_ = n.store.AppendWAL(b)
						}
					}
					_ = appendable.Append(rd.Entries)
				}
				// Send raft messages over the transport to peers. Use
				// peerAddrs mapping to resolve raft node IDs to addresses.
				for _, m := range rd.Messages {
					if n.tr == nil {
						continue
					}
					if b, err := proto.Marshal(&m); err == nil {
						addr := fmt.Sprintf("%d", m.To)
						if n.peerAddrs != nil {
							if a, ok := n.peerAddrs[uint64(m.To)]; ok {
								addr = a
							}
						}
						// attach node id metadata
						ctxWithMeta := metadata.AppendToOutgoingContext(ctx, "node", fmt.Sprintf("%d", n.id))
						_ = n.tr.Send(ctxWithMeta, addr, b)
					}
				}

				// Snapshot policy: create a simple snapshot periodically.
				// For demo purposes, create a snapshot after a modest number of entries.
				// Track entries via Ready's Entries length; use a simple static threshold.
				const snapshotThreshold = 50
				// Use a small, per-node counter stored in the Node (via closure variable).
				// NOTE: entriesSinceSnapshot is a closure var; initialize via a map keyed by node id could be used
				// but for simplicity we keep a local static counter variable using time check as well.
				// Here we create a snapshot whenever there are entries >= snapshotThreshold.
				if n.store != nil && len(rd.Entries) >= snapshotThreshold {
					snap := map[string]interface{}{
						"node_id":   n.id,
						"timestamp": time.Now().UnixNano(),
					}
					var buf bytes.Buffer
					if err := json.NewEncoder(&buf).Encode(snap); err == nil {
						if id, err := n.store.SaveSnapshot(&buf); err == nil {
							log.Printf("raft: saved snapshot %s for node %d", id, n.id)
							// After saving snapshot, request compaction which rotates WAL and purges old snapshots.
							_ = n.Compact(0)
						}
					}
				}

				// Advance the node to acknowledge the Ready has been processed.
				n.node.Advance()
			}
		}
	}()

	log.Printf("raft: started node %d (in-memory)", n.id)
	return nil
}

func (n *Node) Stop() error {
	close(n.stopc)
	if n.node != nil {
		n.node.Stop()
	}
	log.Printf("raft: stopped node %d", n.id)
	return nil
}

// Propose is a convenience to propose a command to the raft node.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	if n.node == nil {
		return nil
	}
	n.node.Propose(ctx, data)
	return nil
}

// Compact the in-memory storage up to the given index (demo helper).
func (n *Node) Compact(index uint64) error {
	// If the underlying Storage supports WAL compaction and snapshot purging, call them.
	if ws, ok := n.store.(*walStorage); ok {
		if err := ws.Compact(index); err != nil {
			return err
		}
		// rotate WAL and purge old snapshots, keep last 2
		if err := ws.RotateWAL(); err != nil {
			return err
		}
		if err := ws.PurgeOldSnapshots(2); err != nil {
			return err
		}
	}
	return nil
}

func marshalEntries(ents []raftpb.Entry) ([]byte, error) {
	var out []byte
	for _, e := range ents {
		b, err := proto.Marshal(&e)
		if err != nil {
			return nil, err
		}
		l := uint32(len(b))
		hdr := []byte{byte(l >> 24), byte(l >> 16), byte(l >> 8), byte(l)}
		out = append(out, hdr...)
		out = append(out, b...)
	}
	return out, nil
}

func unmarshalEntries(data []byte) ([]raftpb.Entry, error) {
	var ents []raftpb.Entry
	i := 0
	for i < len(data) {
		if i+4 > len(data) {
			return nil, nil
		}
		l := (uint32(data[i]) << 24) | (uint32(data[i+1]) << 16) | (uint32(data[i+2]) << 8) | uint32(data[i+3])
		i += 4
		if i+int(l) > len(data) {
			return nil, nil
		}
		var e raftpb.Entry
		if err := proto.Unmarshal(data[i:i+int(l)], &e); err != nil {
			return nil, err
		}
		ents = append(ents, e)
		i += int(l)
	}
	return ents, nil
}
