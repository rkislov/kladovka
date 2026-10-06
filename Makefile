# Copyright 2026 Роман Сергеевич Кислов
# Licensed under the Apache License, Version 2.0

APP=kladovka
VERSION?=0.1.0
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT)"

.PHONY: build test run clean

build:
	mkdir -p bin
	go build $(LDFLAGS) -o bin/$(APP) ./cmd/kladovka

test:
	go test ./...

run: build
	./bin/$(APP)

clean:
	rm -rf bin
