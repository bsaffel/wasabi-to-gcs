.DEFAULT_GOAL := help

.PHONY: help build run test test-race test-integration test-coverage lint tidy clean

help: ## Show this help message
	@printf "Usage: make <target>\n\n"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

build: ## Build the wasabi-to-gcs binary
	go build -o wasabi-to-gcs .

run: ## Build and run the migration tool
	go run .

test: ## Run tests (short mode)
	go test -v -short ./...

test-race: ## Run tests with race detector
	go test -v -race ./...

test-integration: ## Run integration tests
	go test -v -tags=integration ./...

test-coverage: ## Generate test coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## Run golangci-lint
	golangci-lint run ./...

tidy: ## Tidy go module dependencies
	go mod tidy

clean: ## Remove build artifacts
	rm -f wasabi-to-gcs coverage.out coverage.html
