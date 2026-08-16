Operator Guide (draft)

This document outlines operator responsibilities and deploy-time steps for StreamFlow.

- Upgrades: Use rolling upgrades with `preStop` hooks calling `/drain`.
- Backups: Schedule `cmd/backup` to offload snapshots to S3.
- Scaling: Configure HPA in `deploy/hpa.yaml` with custom metrics.
- Secrets: Use Kubernetes `Secret` for TLS certs and integrate with KMS.

Operator runbook:
1. Drain node: invoke `/drain` endpoint or call `Broker.Drain` via management API.
2. Wait for in-flight requests to finish, then upgrade image.
3. Verify rejoin and replication using Raft health metrics.
