# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN apk add --no-cache git
RUN go mod download
COPY . .
RUN go build -o /app/cmd-broker ./cmd/broker

FROM alpine:3.18
RUN addgroup -S app && adduser -S app -G app
COPY --from=builder /app/cmd-broker /usr/local/bin/cmd-broker
USER app
ENTRYPOINT ["/usr/local/bin/cmd-broker"]
