.PHONY: build test test-race test-integration test-coverage lint tidy clean

build:
	go build -o migrate .

test:
	go test -v -short ./...

test-race:
	go test -v -race ./...

test-integration:
	go test -v -tags=integration ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

clean:
	rm -f migrate coverage.out coverage.html
