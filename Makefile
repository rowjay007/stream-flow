.PHONY: all test build lint docker

all: build

build:
	go build -v -o bin/broker ./cmd/broker
	go build -v -o bin/processor ./processor/cmd/processor
	go build -v -o bin/streamql ./streamql/cmd/streamql
	go build -v -o bin/schema-registry ./schema/cmd/schema-registry
	go build -v -o bin/connector ./connector/cmd/connector
	go build -v -o bin/management ./management/cmd/management


test:
	go test ./... -v

lint:
	@which golangci-lint >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.0
	~/go/bin/golangci-lint run ./...

docker:
	docker build -t streamflow/broker:latest .

bench:
	go test ./bench -bench BenchmarkBrokerProduce -run ^$$ -benchmem -benchtime=1s
