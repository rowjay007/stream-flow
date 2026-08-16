.PHONY: all test build lint docker bench

all: build

build:
	go build -v -o bin/broker ./apps/broker
	go build -v -o bin/processor ./apps/processor
	go build -v -o bin/processor-server ./apps/processor-server
	go build -v -o bin/streamql ./streamql/cmd/streamql
	go build -v -o bin/streamql-runner ./jobs/streamql-runner
	go build -v -o bin/schema-registry ./apps/schema-registry
	go build -v -o bin/connector ./apps/connector
	go build -v -o bin/gateway ./apps/region-gateway
	go build -v -o bin/backup ./jobs/backup
	go build -v -o bin/management ./apps/management-api


test:
	go test ./... -v

lint:
	@which golangci-lint >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.0
	~/go/bin/golangci-lint run ./...

docker:
	docker build -t streamflow/broker:latest .

bench:
	go test ./bench -bench . -run ^$$ -benchmem -benchtime=2s
