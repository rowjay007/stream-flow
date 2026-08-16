Cross-region replication — design notes

Goal: allow StreamFlow clusters in separate regions to replicate topics and snapshots with configurable leader affinity and read-local preferences.

Key components:
- Region-aware node metadata: each node advertises `region` tag and `priority`.
- Inter-region replication streams: use secure gRPC streams between region gateways.
- Leader affinity: prefer promoting local leader for writes when possible; use raft joint-consensus across regions for global consistency where required.
- Snapshot shipping: snapshots are periodically offloaded to a global object store (S3), and remote regions can fetch snapshots to catch up.
- WAN tuning: batch replication, compression, and rate-limiting.

Prototype plan:
1. Add node `region` metadata in `Node` and allow setting via env or config.
2. Implement a RegionGateway service that forwards raft messages across regions using a persistent gRPC stream.
3. Provide a CLI flag to start a gateway in `cmd/gateway`.
4. Add tests simulating two-region cluster with delayed links.

Notes:
- This design favors eventual consistency for cross-region reads but can be configured for stronger consistency with cross-region raft groups.
