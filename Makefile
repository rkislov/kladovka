# Copyright 2026 Роман Сергеевич Кислов
# Licensed under the Apache License, Version 2.0

APP=kladovka
VERSION?=0.4.0
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS=-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)
DIST=dist

.PHONY: build test run clean release checksums ui

ui:
	npm ci --ignore-scripts --cache .cache/npm
	npm run build:css

build:
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/$(APP) ./cmd/kladovka

test:
	go test ./...

run: build
	./bin/$(APP)

clean:
	rm -rf bin $(DIST)

# Cross-builds: Linux x86_64, Raspberry Pi OS 64-bit (arm64), Raspberry Pi 32-bit (armv7)
release: clean
	mkdir -p $(DIST)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(DIST)/$(APP)-$(VERSION)-linux-amd64 ./cmd/kladovka
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(DIST)/$(APP)-$(VERSION)-linux-arm64 ./cmd/kladovka
	GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(DIST)/$(APP)-$(VERSION)-linux-armv7 ./cmd/kladovka
	GOOS=linux GOARCH=arm GOARM=6 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(DIST)/$(APP)-$(VERSION)-linux-armv6 ./cmd/kladovka
	# Convenience names for Raspberry Pi
	cp $(DIST)/$(APP)-$(VERSION)-linux-arm64 $(DIST)/$(APP)-$(VERSION)-raspberrypi-64bit
	cp $(DIST)/$(APP)-$(VERSION)-linux-armv7 $(DIST)/$(APP)-$(VERSION)-raspberrypi-32bit
	tar -czf $(DIST)/$(APP)-$(VERSION)-third-party-notices.tar.gz NOTICE THIRD_PARTY_NOTICES.md third_party/go-licenses
	tar -czf $(DIST)/$(APP)-$(VERSION)-documentation.tar.gz README.md docs/USER.md docs/ADMIN.md docs/CONSENSUS.md docs/STATUS.md docs/S3_CLIENTS.md docs/LOAD_PILOT.md docs/load-results docs/presentation docs/wallpapers/joyful docs/screenshots/ui-light.png docs/screenshots/ui-version-history.png monitoring
	$(MAKE) checksums

checksums:
	cd $(DIST) && shasum -a 256 $(APP)-$(VERSION)-* > SHA256SUMS
	@echo "Artifacts in $(DIST)/:"
	@ls -lh $(DIST)
