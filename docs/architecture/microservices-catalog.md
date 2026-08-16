# StreamFlow Microservices Catalog

This document is the source of truth for runnable services in this repository.

## Core Data Plane

1. Broker API service
   - Entry point: apps/broker/main.go
   - Legacy compatibility: cmd/broker/main.go
   - Default port: 9092
   - Protocol: HTTP
   - Purpose: produce, consume, raw fetch path

2. Processor service
   - Entry point: apps/processor/main.go
   - Legacy compatibility: processor/cmd/processor/main.go
   - Default port: 50070
   - Protocol: HTTP
   - Purpose: stream processor runtime health endpoint and service host

3. Processor gRPC server
   - Entry point: apps/processor-server/main.go
   - Legacy compatibility: cmd/processor-server/main.go
   - Default listen: 127.0.0.1:9090
   - Protocol: gRPC
   - Purpose: processor transport server for remote processing clients

## Control Plane

4. Management API service
   - Entry point: apps/management-api/main.go
   - Legacy compatibility: management/cmd/management/main.go
   - Default port: 8094
   - Protocol: HTTP (+ GraphQL endpoint)
   - Purpose: administration, offsets, auth-protected operations, tx controls, metrics

5. Schema Registry service
   - Entry point: apps/schema-registry/main.go
   - Legacy compatibility: schema/cmd/schema-registry/main.go
   - Default port: 8081
   - Protocol: HTTP
   - Purpose: schema registry service surface

## Integration and Edge

6. Connector Framework service
   - Entry point: apps/connector/main.go
   - Legacy compatibility: connector/cmd/connector/main.go
   - Default port: 50072
   - Protocol: HTTP
   - Purpose: connector runtime host for external system integrations

7. Region Gateway service
   - Entry point: apps/region-gateway/main.go
   - Legacy compatibility: cmd/gateway/main.go
   - Default addr flag: gateway-1
   - Protocol: internal gateway abstraction
   - Purpose: regional forwarding/gateway prototype

## Operational and Tooling Jobs

8. Backup worker
   - Entry point: jobs/backup/main.go
   - Legacy compatibility: cmd/backup/main.go
   - Mode: CLI batch job
   - Purpose: snapshot upload/download to local or S3 offload storage

9. StreamQL runner
   - Entry point: jobs/streamql-runner/main.go
   - Legacy compatibility: cmd/streamql-runner/main.go
   - Mode: CLI batch job
   - Purpose: local query parse-plan-run workflow for StreamQL

## Notes

1. Services 1 through 7 are long-running processes and should be treated as deployable microservices.
2. Services 8 and 9 are operational tooling workloads and should be deployed as jobs, not as always-on services.
3. If a new main package is added, update this file in the same pull request.
