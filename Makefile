BIN := syncup
PREFIX ?= $(HOME)/.local/bin

.PHONY: build install bootstrap test vet clean

build:
	go build -o $(BIN) .

install: build
	mkdir -p $(PREFIX)
	cp $(BIN) $(PREFIX)/$(BIN)
	@echo "installed $(PREFIX)/$(BIN) (ensure $(PREFIX) is on PATH)"

# One-shot onboarding: build, install, and configure from the shared .env.
# Brokers come from SYNCUP_BROKERS in .env; username from SYNCUP_USER or $USER.
bootstrap: install
	@test -f .env || { echo "no .env found — copy .env.example to .env and set SYNCUP_BROKERS"; exit 1; }
	@set -a; . ./.env; set +a; "$(PREFIX)/$(BIN)" init --user "$${SYNCUP_USER:-$$USER}"

vet:
	go vet ./...

# Run the end-to-end test against a real cluster:
#   make test BROKERS=b-1:9092,b-2:9092
test: build
	BROKERS=$(BROKERS) ./test/e2e.sh

clean:
	rm -f $(BIN)
