# stream-flow

Minimal stream broker prototype.

Quick commands:

Build binary:

```
make build
```

Run tests:

```
make test
```

Run lint (installs golangci-lint if missing):

```
make lint
```

Build Docker image:

```
make docker
```

Phase 1 — Run locally
---------------------

Run the single-node broker (HTTP endpoints for quick testing):

```bash
go run ./cmd/broker
```

Produce a record:

```bash
curl -X POST http://localhost:9092/produce -d '{"topic":"orders","key":"k","value":"v"}' -H 'Content-Type: application/json'
```

Consume records:

```bash
curl 'http://localhost:9092/consume?topic=orders&offset=0&max=10'
```

Fetch raw record (uses zero-copy on Linux when possible):

```bash
curl 'http://localhost:9092/fetchraw?topic=orders&offset=0' --output - > record.bin
```

Bench (run on a tuned Linux machine for best results):

```bash
make bench
```


