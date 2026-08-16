# stream-flow

StreamFlow is a Go-based streaming platform prototype that includes:

- A single-node log broker with append-only segments and consumer offsets
- Raft-backed replication internals and durability primitives
- A management API with metrics, API-key auth, and drain controls
- Transactional and idempotent produce APIs (epoch/sequence aware)
- Stream processing and StreamQL foundations
- Benchmarks, backup tooling, and Kubernetes deployment manifests

## Repository layout

- `broker/`: core broker logic, segment/index storage, sendfile/raw fetch path
- `broker/internal/raft/`: Raft node, transport, WAL, snapshotting, restart/compaction tests
- `management/api/`: management HTTP API, auth middleware, GraphQL endpoint, audit logging, Prometheus + OTel hooks
- `processor/`, `planner/`, `streamql/`: processing runtime and query/parsing execution layers
- `schema/`: schema registry baseline
- `bench/`: benchmark entry points and throughput/load test scaffolding
- `cmd/`: runnable binaries (`broker`, `gateway`, `backup`, `processor-server`, `streamql-runner`)
- `deploy/`, `k8s/`: deployment manifests

## Prerequisites

- Go 1.23+
- GNU make
- Docker (optional, for image builds)

## Quickstart

### Build

```bash
make build
```

### Test

```bash
make test
```

If your environment has temporary directory importcfg instability, run:

```bash
GOTMPDIR="$PWD/.tmp/go-build" go test ./... -count=1
```

### Lint

```bash
make lint
```

### Docker image

```bash
make docker
```

## Run locally

### Broker API

Start broker:

```bash
go run ./cmd/broker
```

Default address: `:9092`

Endpoints:

- `GET /health`
- `POST /produce`
- `GET /consume`
- `GET /fetchraw`

Produce:

```bash
curl -X POST http://localhost:9092/produce \
	-H 'Content-Type: application/json' \
	-d '{"topic":"orders","key":"k","value":"v"}'
```

Consume:

```bash
curl 'http://localhost:9092/consume?topic=orders&offset=0&max=10'
```

Raw fetch (zero-copy on Linux when possible):

```bash
curl 'http://localhost:9092/fetchraw?topic=orders&offset=0' --output - > record.bin
```

### Management API

Start management API:

```bash
go run ./management/cmd/management
```

Default address: `:8094`

Optional auth:

```bash
export STREAMFLOW_MANAGEMENT_API_KEY='change-me'
```

When `STREAMFLOW_MANAGEMENT_API_KEY` is set, all endpoints except `/health` and `/metrics` require header:

```text
X-API-Key: <value>
```

Endpoints:

- `GET /health`
- `GET /metrics`
- `GET /topics`
- `POST /topics`
- `POST /produce`
- `POST /produce/idempotent`
- `GET /consume`
- `POST /offset/commit`
- `GET /offset`
- `POST /admin/drain`
- `POST /tx/begin`
- `POST /tx/produce`
- `POST /tx/commit`
- `POST /tx/abort`
- `POST /graphql`
- `POST /admin/window-close`

Drain example:

```bash
curl -X POST http://localhost:8094/admin/drain \
	-H 'Content-Type: application/json' \
	-H 'X-API-Key: change-me' \
	-d '{"duration_seconds":30}'
```

While draining, produce requests return HTTP `409 Conflict`.

GraphQL example:

```bash
curl -X POST http://localhost:8094/graphql \
	-H 'Content-Type: application/json' \
	-H 'X-API-Key: change-me' \
	-d '{"query":"{ topics }"}'
```

Transactional flow example:

```bash
TX_ID=$(curl -s -X POST http://localhost:8094/tx/begin \
	-H 'Content-Type: application/json' \
	-H 'X-API-Key: change-me' \
	-d '{"producer_id":"p1","epoch":1}' | jq -r '.tx_id')

curl -X POST http://localhost:8094/tx/produce \
	-H 'Content-Type: application/json' \
	-H 'X-API-Key: change-me' \
	-d '{"tx_id":"'"$TX_ID"'","topic":"orders","key":"k1","value":"v1"}'

curl -X POST http://localhost:8094/tx/commit \
	-H 'Content-Type: application/json' \
	-H 'X-API-Key: change-me' \
	-d '{"tx_id":"'"$TX_ID"'"}'
```

## Benchmarks

Run benchmark suite:

```bash
make bench
```

For realistic sendfile and throughput characteristics, benchmark on Linux.

Direct benchmark command:

```bash
go test ./bench -bench . -run ^$ -benchmem -benchtime=2s
```

## Backup tooling

Backup CLI entry point:

```bash
go run ./cmd/backup
```

Use this command path for snapshot upload/download workflows.

## Additional docs

- Architecture decision: `ADR/0001-zero-copy-sendfile.md`
- Additional ADRs: `ADR/0002-raft-isr-minisr.md` ... `ADR/0008-kubernetes-packaging.md`
- Operator notes: `docs/operator.md`
- SDK notes: `sdk/README.md`
- Helm chart: `deploy/helm/streamflow`
- Grafana dashboard: `observability/grafana/streamflow-dashboard.json`

## Current status

Implemented and stable in mainline:

- Core broker produce/consume/fetch-raw flow
- Idempotent producer sequencing and transaction staging/commit APIs
- Segment index loading on startup
- Sparse mmap index primitive for offset indexing
- Durable offset commit/load
- Management API with REST + GraphQL, auth, audit logs, Prometheus metrics, and OTel instrumentation
- Consumer group coordinator with deterministic rebalancing
- Stream processing utilities: tumbling/sliding/session windows, watermarking, CEP FSM, time-bounded stream join
- Schema registry extensions for JSONSchema/Avro/Protobuf versioning and backward-compat checks
- Built-in connector surfaces for Kafka bridge, PostgreSQL CDC, S3, and HTTP
- Helm/Grafana/ADR artifacts for production operations
- Raft tick lifecycle and WAL compaction/read-path stabilization
- Repository-wide passing test suite
