.PHONY: all proto tidy lint test it-test propagation-smoke propagation-full docker run-server clean help

GOBIN ?= $(shell go env GOPATH)/bin
PROTOC ?= protoc
GO ?= go
PKG := github.com/SAY-5/configmesh

all: lint test ## Lint and run unit tests.

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

proto: ## Regenerate protobuf Go code from proto/config.proto.
	mkdir -p proto/configmeshv1
	$(PROTOC) \
		--go_out=. \
		--go_opt=Mconfig.proto=$(PKG)/proto/configmeshv1 \
		--go-grpc_out=. \
		--go-grpc_opt=Mconfig.proto=$(PKG)/proto/configmeshv1 \
		--proto_path=proto \
		config.proto
	mv github.com/SAY-5/configmesh/proto/configmeshv1/*.go proto/configmeshv1/
	rm -rf github.com

tidy: ## go mod tidy.
	$(GO) mod tidy

lint: ## Run golangci-lint.
	$(GOBIN)/golangci-lint run ./...

test: ## Run unit tests (no docker required).
	$(GO) test ./internal/... -short -race -count=1

it-test: ## Run full integration tests (requires docker for Redis testcontainer).
	$(GO) test ./internal/... -race -count=1 -tags=integration

propagation-smoke: ## Run the 50-instance propagation harness at smoke scale and assert SLOs.
	$(GO) test ./test/propagation -run TestPropagation_50Clients_Smoke -race -count=1 -tags=integration -v

propagation-full: ## Run the propagation harness at full scale and write report JSON.
	$(GO) run ./test/propagation/cmd/run -clients=50 -writes=100 -out=propagation-result.json

docker: ## Build the multi-stage docker image.
	docker build -t configmesh:dev .

run-server: ## Run the server locally on :9090 (requires Redis on :6379).
	$(GO) run ./cmd/configmesh-server

clean: ## Clean build artifacts.
	rm -f configmesh-server propagation-result.json
