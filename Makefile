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
