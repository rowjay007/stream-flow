Phase 6 — Scale, Ecosystem & GA Readiness

Goals
- Prepare StreamFlow to operate at production scale across regions and tenants.
- Provide SDKs, integrations, and performance testing necessary for a GA candidate.

Key deliverables
- Autoscaling: HPA + custom metrics, resource-aware scheduling, and operator support.
- Cross-region replication: geo-replication, leader affinity, WAN tuning.
- Multi-tenancy: namespaces, quotas, and tenant isolation.
- Client SDK improvements and official connectors.
- Performance & scale testing harness and CI perf gates.
- Security hardening for production (RBAC, audit logging, secrets integration).

Immediate next tasks
1. Add HPA manifests and custom metrics exporter.
2. Implement graceful node drain and rejoin (rolling upgrades).
3. Design geo-replication protocol sketch and tests.
4. Start SDK stabilization and example integrations.
