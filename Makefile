GO := go
BINDIR := $(CURDIR)/bin

all: bin

.PHONY: bin
bin:
	GOBIN=$(BINDIR) $(GO) install github.com/antoninbas/antrea-k8s-to-ddlog/...

clean:
	rm -rf bin

.PHONY: fmt
fmt:
	$(GO) fmt github.com/antoninbas/antrea-k8s-to-ddlog/...

# Run unit tests only, no integration tests
.PHONY: check-unit
check-unit:
	$(GO) test -v github.com/antoninbas/antrea-k8s-to-ddlog/...

.PHONY: check-bench
check-bench:
	$(GO) test -bench=. github.com/antoninbas/antrea-k8s-to-ddlog/...

.golangci-bin:
	@echo "===> Installing Golangci-lint <==="
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $@ v1.21.0

.PHONY: golangci
golangci: .golangci-bin
	@GOOS=linux .golangci-bin/golangci-lint run -c .golangci.yml pkg/ddlog
