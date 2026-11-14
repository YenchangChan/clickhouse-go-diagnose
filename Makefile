# clickhouse-go-diagnose 全仓构建入口。
# 产物统一放 clickhouse-doctor/bin/（已 gitignore），不进 /tmp。

DOCTOR_DIR := clickhouse-doctor
BIN_DIR := $(DOCTOR_DIR)/bin
DOCTOR := $(BIN_DIR)/clickhouse-doctor
DOCTOR_CHAT := $(BIN_DIR)/clickhouse-doctor-chat

DOCTOR_MODULE := github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor
NOVA_IMPORTS := AutumnsGrove/nova

.PHONY: build test race vet fmt assert-deps check clean help

build: $(DOCTOR) $(DOCTOR_CHAT) ## 构建两个入口

$(DOCTOR): $(wildcard $(DOCTOR_DIR)/*.go) $(DOCTOR_DIR)/go.mod
	go build -o $@ ./$(DOCTOR_DIR)

$(DOCTOR_CHAT): $(wildcard $(DOCTOR_DIR)/chat/*.go) $(DOCTOR_DIR)/targets/*.go $(DOCTOR_DIR)/go.mod
	go build -o $@ ./$(DOCTOR_DIR)/chat

test: ## 全部模块单元测试（不含真机 integration，需显式启用）
	go test -count=1 ./...
	go test -count=1 ./$(DOCTOR_DIR)/...

race: ## 核心库与消费端 race 测试
	go test -race -count=1 ./...
	go test -race -count=1 ./$(DOCTOR_DIR)/...

vet: ## 静态检查
	go vet ./...
	go vet ./$(DOCTOR_DIR)/...

fmt: ## 格式化并检查
	gofmt -w . $(DOCTOR_DIR)
	gofmt -l . $(DOCTOR_DIR)

assert-deps: ## 分层断言：轻量 CLI 不含 nova；chat 不含 bubbletea 全屏 TUI 框架
	@n=$$(go list -deps $(DOCTOR_MODULE) | grep -c '$(NOVA_IMPORTS)'); \
	if [ "$$n" -ne 0 ]; then \
		echo "轻量 CLI 依赖闭包混入 nova（$$n 处）"; exit 1; \
	fi; \
	m=$$(go list -deps $(DOCTOR_MODULE)/chat | grep -cE 'charm.land/(bubbletea|bubbles|fang)'); \
	if [ "$$m" -ne 0 ]; then \
		echo "chat 依赖闭包混入全屏 TUI 框架（$$m 处）"; exit 1; \
	fi; \
	echo "分层断言通过：CLI 无 nova / chat 无全屏 TUI（glamour/lipgloss 渲染库为 chat 显式依赖，允许）"

check: fmt vet test assert-deps ## 提交前的完整门槛

clean: ## 清理构建产物
	rm -rf $(BIN_DIR)

help: ## 显示本帮助
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
