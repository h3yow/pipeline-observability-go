.PHONY: build vet test race cover fmt

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

cover:
	go test -cover ./...

fmt:
	gofmt -l -w .
