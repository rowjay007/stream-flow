package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	raftlib "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"sync"
	"time"
)

type Node struct {
	id        uint64
	store     Storage
	tr        Transport
	node      raftlib.Node
	stopc     chan struct{}
	stopOnce  sync.Once
	peers     []raftlib.Peer
	peerAddrs map[uint64]string
}

func NewNode(id uint64, store Storage, tr Transport) *Node {
	log.Printf("raft: create node id=%d", id)
	return &Node{id: id, store: store, tr: tr, stopc: make(chan struct{})}
}

func (n *Node) Start(ctx context.Context) error {

	mem := raftlib.NewMemoryStorage()
	var storage raftlib.Storage = mem
	var appendable interface {
		Append([]raftpb.Entry) error
		ApplySnapshot(raftpb.Snapshot) error
	} = mem

	if n.store != nil {

		if rc, err := n.store.LoadSnapshot(); err == nil && rc != nil {

			var snapBytes []byte
			snapBytes, _ = io.ReadAll(rc)
			rc.Close()
			if len(snapBytes) > 0 {

				var s raftpb.Snapshot
				s.Data = snapBytes
				_ = mem.ApplySnapshot(s)
			}
		}
		if data, err := n.store.ReadWAL(0); err == nil && len(data) > 0 {
			if ents, err := unmarshalEntries(data); err == nil && len(ents) > 0 {

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

	peers := n.peers
	if len(peers) == 0 {
		peers = []raftlib.Peer{{ID: n.id}}
	}
	n.node = raftlib.StartNode(&cfg, peers)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-n.stopc:
				return
			case <-ticker.C:
				n.node.Tick()
			}
		}
	}()

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

				if !raftlib.IsEmptySnap(rd.Snapshot) {
					_ = appendable.ApplySnapshot(rd.Snapshot)
				}

				if len(rd.Entries) > 0 {

					if n.store != nil {
						if b, err := marshalEntries(rd.Entries); err == nil {
							_ = n.store.AppendWAL(b)
						}
					}
					_ = appendable.Append(rd.Entries)
				}

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

						ctxWithMeta := metadata.AppendToOutgoingContext(ctx, "node", fmt.Sprintf("%d", n.id))
						_ = n.tr.Send(ctxWithMeta, addr, b)
					}
				}

				const snapshotThreshold = 50

				if n.store != nil && len(rd.Entries) >= snapshotThreshold {
					snap := map[string]interface{}{
						"node_id":   n.id,
						"timestamp": time.Now().UnixNano(),
					}
					var buf bytes.Buffer
					if err := json.NewEncoder(&buf).Encode(snap); err == nil {
						if id, err := n.store.SaveSnapshot(&buf); err == nil {
							log.Printf("raft: saved snapshot %s for node %d", id, n.id)

							_ = n.Compact(0)
						}
					}
				}

				n.node.Advance()
			}
		}
	}()

	log.Printf("raft: started node %d (in-memory)", n.id)
	return nil
}

func (n *Node) Stop() error {
	n.stopOnce.Do(func() {
		close(n.stopc)
		if n.node != nil {
			n.node.Stop()
		}
		log.Printf("raft: stopped node %d", n.id)
	})
	return nil
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	if n.node == nil {
		return nil
	}
	n.node.Propose(ctx, data)
	return nil
}

func (n *Node) Compact(index uint64) error {

	if ws, ok := n.store.(*walStorage); ok {
		if err := ws.Compact(index); err != nil {
			return err
		}

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
