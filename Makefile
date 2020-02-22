GO := go
BINDIR := $(CURDIR)/bin

CGO_LDFLAGS:=-L$(CURDIR)/ddlog/libs -lnetworkpolicy_controller_ddlog
CGO_CPPFLAGS:=-I$(CURDIR)/ddlog
LD_LIBRARY_PATH:=$(CURDIR)/ddlog/libs

export CGO_LDFLAGS
export CGO_CPPFLAGS
export LD_LIBRARY_PATH

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
	$(GO) test -v -bench=. github.com/antoninbas/antrea-k8s-to-ddlog/...

.golangci-bin:
	@echo "===> Installing Golangci-lint <==="
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $@ v1.21.0

.PHONY: golangci
golangci: .golangci-bin
	@GOOS=linux .golangci-bin/golangci-lint run -c .golangci.yml pkg/ddlog
