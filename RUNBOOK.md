Runbook — StreamFlow (draft)

1. Incident response
 - Pager: on-call rotation and escalation
 - Triage: gather cluster state, leader status, recent snapshots

2. Common recovery steps
 - Check raft leader with /metrics; force leader re-election if necessary
 - Restore from latest snapshot: use `cmd/backup --download` to fetch snapshot
 - Rejoin nodes by restarting with proper raft config and snapshots

3. Rolling upgrade
 - Mark node draining: call Broker.Drain(ctx, 30s)
 - Upgrade container image
 - Start node and ensure rejoin
