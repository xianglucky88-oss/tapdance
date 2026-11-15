SHELL := /bin/sh
GO ?= go
BIN := bin

.PHONY: help build test demo verify clean

help:
	@echo "make build    编译 tapdance 与 walgen"
	@echo "make test     单元测试"
	@echo "make demo     生成 WAL 并处理一次"
	@echo "make verify   断言处理结果"
	@echo "make clean    清理产物"

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/tapdance ./cmd/tapdance
	$(GO) build -o $(BIN)/walgen ./cmd/walgen

test:
	$(GO) test ./... -count=1

demo: build
	sh scripts/demo.sh

verify: build
	sh scripts/verify.sh

clean:
	rm -rf $(BIN) /tmp/tapdance
