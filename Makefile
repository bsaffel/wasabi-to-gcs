.DEFAULT_GOAL := help

VM_NAME  ?= wasabi-migrator
VM_ZONE  ?= us-east1-b
VM_TYPE  ?= e2-standard-2   # cheap option for 8 workers
# VM_TYPE  ?= c4d-standard-4  # fast option for up to 32 workers

.PHONY: help build build-linux run test test-race test-integration test-coverage vet lint tidy clean check ci auth deploy redeploy ssh stop start teardown

help: ## Show this help message
	@printf "Usage: make <target>\n\n"
	@printf "\033[1mPre-deploy Workflows\033[0m\n"
	@printf "  \033[36m%-20s\033[0m %s\n" "check" "Dev: tidy → vet → test → build-linux  (fast, run before every redeploy)"
	@printf "  \033[36m%-20s\033[0m %s\n" "ci" "Full: tidy → lint → test-race → test-coverage → build-linux  (complete CI gate)"
	@printf "\n\033[1mBuild\033[0m\n"
	@grep -E '^(build|build-linux|run|clean):.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@printf "\n\033[1mTest & Lint\033[0m\n"
	@grep -E '^(test|test-race|test-integration|test-coverage|vet|lint|tidy):.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@printf "\n\033[1mDeploy (GCP VM)\033[0m\n"
	@grep -E '^(auth|deploy|redeploy|ssh|stop|start|teardown):.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@printf "\n\033[1mVM Settings\033[0m (override with make VAR=value)\n"
	@printf "  \033[33mVM_NAME\033[0m  = $(VM_NAME)\n"
	@printf "  \033[33mVM_ZONE\033[0m  = $(VM_ZONE)\n"
	@printf "  \033[33mVM_TYPE\033[0m  = $(VM_TYPE)\n"
	@echo

##@ Build

build: ## Compile the wasabi-to-gcs binary for the current platform
	go build -o wasabi-to-gcs .

build-linux: ## Cross-compile a Linux amd64 binary for GCP deployment
	GOOS=linux GOARCH=amd64 go build -o wasabi-to-gcs-linux .

run: ## Build and run the migration tool (passes through CLI args)
	go run .

clean: ## Remove build artifacts (binaries, coverage reports)
	rm -f wasabi-to-gcs wasabi-to-gcs-linux coverage.out coverage.html

##@ Test & Lint

test: ## Run unit tests in short mode
	go test -v -short ./...

test-race: ## Run all tests with the Go race detector enabled
	go test -v -race ./...

test-integration: ## Run integration tests (requires real Wasabi/GCS credentials)
	go test -v -tags=integration ./...

test-coverage: ## Generate an HTML test coverage report → coverage.html
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

vet: ## Run go vet (fast built-in static analysis)
	go vet ./...

lint: ## Run golangci-lint (full static analysis — requires golangci-lint installed)
	golangci-lint run ./...

tidy: ## Tidy go.mod and go.sum, removing unused dependencies
	go mod tidy

check: ## Pre-redeploy (dev): tidy, vet, test
	$(MAKE) tidy
	$(MAKE) vet
	$(MAKE) test

ci: ## Pre-redeploy (CI): tidy, lint, test-race, test-coverage
	$(MAKE) tidy
	$(MAKE) lint
	$(MAKE) test-race
	$(MAKE) test-coverage

##@ Deploy (GCP VM)

auth: ## Authenticate to GCP and set the active project (usage: make auth PROJECT=my-project)
	@if [ -z "$(PROJECT)" ]; then echo "Usage: make auth PROJECT=<gcp-project-id>"; exit 1; fi
	gcloud auth application-default login --project $(PROJECT)
	gcloud config set project $(PROJECT)
	@echo "\nActive project set to: $(PROJECT)"

deploy: build-linux ## Build Linux binary, create GCP VM, and deploy with state
	./deploy.sh $(VM_NAME) $(VM_ZONE) $(VM_TYPE)

redeploy: build-linux ## Push updated binary to existing GCP VM
	gcloud compute scp wasabi-to-gcs-linux $(VM_NAME):~ --zone=$(VM_ZONE)
	gcloud compute ssh $(VM_NAME) --zone=$(VM_ZONE) --command="chmod +x ~/wasabi-to-gcs-linux"

ssh: ## Open an SSH session to the GCP VM
	gcloud compute ssh $(VM_NAME) --zone=$(VM_ZONE)

stop: ## Stop the GCP VM (preserves disk, no compute charges)
	gcloud compute instances stop $(VM_NAME) --zone=$(VM_ZONE)

start: ## Start a previously stopped GCP VM
	gcloud compute instances start $(VM_NAME) --zone=$(VM_ZONE)

teardown: ## Delete the GCP VM and its resources
	gcloud compute instances delete $(VM_NAME) --zone=$(VM_ZONE) --quiet
