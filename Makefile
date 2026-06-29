BIN := syncup
PREFIX ?= $(HOME)/.local/bin

.PHONY: build install test vet clean

build:
	go build -o $(BIN) .

install: build
	mkdir -p $(PREFIX)
	cp $(BIN) $(PREFIX)/$(BIN)
	@echo "installed $(PREFIX)/$(BIN) (ensure $(PREFIX) is on PATH)"

vet:
	go vet ./...

# Run the end-to-end test against a real cluster:
#   make test BROKERS=b-1:9092,b-2:9092
test: build
	BROKERS=$(BROKERS) ./test/e2e.sh

clean:
	rm -f $(BIN)
