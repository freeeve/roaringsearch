.PHONY: setup test fmt lint

setup:
	git config core.hooksPath .githooks

test:
	go test -v -race ./...

fmt:
	gofmt -s -w .

lint:
	gofmt -s -l .
