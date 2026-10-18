MODULE  := github.com/mhamillwork/unicast-to-multicast-relay
BIN     := bin/mcast-relay
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test lint integration loadtest loadtest-nfr docker docker-multiarch fmt clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/mcast-relay
	CGO_ENABLED=0 go build -trimpath -o bin/pkt ./cmd/pkt

test:
	@ip link show lo 2>/dev/null | grep -q MULTICAST || ip link set lo multicast on 2>/dev/null || sudo -n ip link set lo multicast on 2>/dev/null || true
	go test ./...

lint:
	@test -z "$$(gofmt -l cmd internal test)" || { gofmt -l cmd internal test; exit 1; }
	go vet ./...
	go vet -tags integration ./...

fmt:
	gofmt -w cmd internal test

integration: build
	go test -tags integration -count=1 -timeout 180s ./test/integration/

loadtest: build
	go run ./test/load -duration 3s -payload 64

loadtest-nfr: build
	go run ./test/load -duration 5s -payload 64 -strict

docker:
	docker build -t mcast-relay:dev .

docker-multiarch:
	docker buildx build --platform linux/amd64,linux/arm64 -t mcast-relay:dev .

clean:
	rm -rf bin
