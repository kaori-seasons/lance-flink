.PHONY: build test clean help

MAVEN := mvn
JAVA := java

help:
	@echo "Lance-Flink Connector Build Commands"
	@echo "====================================="
	@echo "make build           - Build all modules"
	@echo "make test            - Run unit tests"
	@echo "make clean           - Clean build artifacts"
	@echo "make help            - Show this help message"

build:
	$(MAVEN) clean package -DskipTests

test:
	$(MAVEN) test

test-unit:
	$(MAVEN) test

test-integration:
	$(MAVEN) test -P integration-tests

clean:
	$(MAVEN) clean
	rm -rf target/

verify:
	$(MAVEN) verify

install:
	$(MAVEN) clean install -DskipTests

# Phase 1 target
phase1-test:
	$(MAVEN) -pl lance-flink-core test
