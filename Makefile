.PHONY: cover
cover:
	go tool cover -html=cover.out

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -coverprofile=cover.out -shuffle on -short ./...

.PHONY: test-integration
test-integration:
	go test -coverprofile=cover.out -shuffle on ./...
