Phase 5 — Production Hardening and Global Features

Goal
- Prepare StreamFlow for production: geo-replication, backups, rolling upgrades, autoscaling, security hardening, resilience testing, performance tuning, CI/CD and operational docs.

Acceptance criteria
- End-to-end streaming queries pass across multi-node, multi-region deployments.
- Snapshots can be offloaded to S3 and restored to rebuild state.
- Nodes support rolling upgrades with zero data loss.
- Automatic certificate rotation works with minimal downtime.
- Autoscaling adjusts cluster capacity based on resource metrics.
- Chaos tests (network partitions, disk corruption) show graceful recovery.
- Performance benchmarks report latency and throughput targets.

Planned work items
1. Define precise metrics & SLOs for production (latency, throughput, recovery time).
2. Implement snapshot offload/restore to S3-compatible storage.
3. Add cross-datacenter replication and leader affinity policies.
4. Implement rolling upgrade orchestration (graceful drain + rejoin).
5. Add autoscaling integration (HPA with custom metrics exporter).
6. Automate mTLS rotation and secret management integration (KMS/HashiVault).
7. Add chaos-testing harness and resilience test suite.
8. Run large-scale benchmarks and optimize hot paths.
9. Create CI/CD pipeline steps to build and publish artifacts.
10. Author runbooks, operator guide, and example k8s deployments.

Immediate next step
- I'll start by drafting the Phase 5 scope/ticket: create backup/restore design and an initial S3 offload implementation (server-side and CLI).