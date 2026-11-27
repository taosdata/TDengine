# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
	cd $(FRONTEND_DIR) && npm ci --include=dev

build:
	cd $(FRONTEND_DIR) && npm exec vite -- build --outDir ../../internal/serve/webdist
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
