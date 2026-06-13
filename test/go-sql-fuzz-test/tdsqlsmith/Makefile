SHELL := /bin/bash

BIN_DIR := bin
FRONTEND_DIR := web/console
BACKEND_BIN := $(BIN_DIR)/tdsqlsmith

PACKAGE_BASENAME ?= tdsqlsmith
PACKAGE_VERSION := $(shell date +%Y%m%d_%H%M%S)
PACKAGE_NAME := $(PACKAGE_BASENAME)-$(PACKAGE_VERSION)
PACKAGE_WORKDIR := $(BIN_DIR)/package/$(PACKAGE_NAME)
PACKAGE_ARCHIVE := $(BIN_DIR)/$(PACKAGE_NAME).tar.gz

.PHONY: init build package

init:
	go mod download
	cd $(FRONTEND_DIR) && npm install

build:
	cd $(FRONTEND_DIR) && npm run build
	mkdir -p $(BIN_DIR)
	go build -o $(BACKEND_BIN) ./cmd/tdsqlsmith

package: build
	rm -rf $(PACKAGE_WORKDIR)
	mkdir -p $(PACKAGE_WORKDIR)
	cp $(BACKEND_BIN) $(PACKAGE_WORKDIR)/
	cp run_parent_child_test.sh $(PACKAGE_WORKDIR)/
	cp run_web_service.sh $(PACKAGE_WORKDIR)/
	chmod +x $(PACKAGE_WORKDIR)/tdsqlsmith $(PACKAGE_WORKDIR)/run_parent_child_test.sh $(PACKAGE_WORKDIR)/run_web_service.sh
	rm -f $(PACKAGE_ARCHIVE)
	tar -C $(BIN_DIR)/package -czf $(abspath $(PACKAGE_ARCHIVE)) $(PACKAGE_NAME)
	@echo "package created: $(PACKAGE_ARCHIVE)"
