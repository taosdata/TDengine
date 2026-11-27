# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

CC       ?= gcc
CFLAGS   := -std=c11 -Wall -Wextra -Wno-unused-parameter -D_GNU_SOURCE
LDFLAGS  := -lpthread -lm

# TDengine paths -- auto-detect from common install locations
TDENGINE_DIR ?= /usr/local/taos

# Try installed TDengine client first, then source tree
ifneq ($(wildcard $(TDENGINE_DIR)/include/taos.h),)
    # Installed client (typical: /usr/local/taos/)
    CFLAGS  += -I$(TDENGINE_DIR)/include
    LDFLAGS += -L$(TDENGINE_DIR)/driver -ltaos
    LDFLAGS += -Wl,-rpath,$(TDENGINE_DIR)/driver
else ifneq ($(wildcard /usr/include/taos.h),)
    # System-wide install
    LDFLAGS += -ltaos
else
    # Source tree (when building inside TDengine repo)
    TD_ROOT := $(CURDIR)/../..
    CFLAGS  += -I$(TD_ROOT)/include/client \
               -I$(TD_ROOT)/include/common \
               -I$(TD_ROOT)/include/util   \
               -I$(TD_ROOT)/include/os     \
               -I$(TD_ROOT)/include
    # Try to find the built library
    ifneq ($(wildcard $(TD_ROOT)/build/build/lib/libtaos.so),)
        LDFLAGS += -L$(TD_ROOT)/build/build/lib -ltaos
        LDFLAGS += -Wl,-rpath,$(TD_ROOT)/build/build/lib
    else ifneq ($(wildcard $(TD_ROOT)/debug/build/lib/libtaos.so),)
        LDFLAGS += -L$(TD_ROOT)/debug/build/lib -ltaos
        LDFLAGS += -Wl,-rpath,$(TD_ROOT)/debug/build/lib
    else
        LDFLAGS += -ltaos
    endif
endif

# Debug/Release
ifdef DEBUG
    CFLAGS  += -g -O0 -fsanitize=address -DDEBUG
    LDFLAGS += -fsanitize=address
else
    CFLAGS  += -O2 -DNDEBUG
endif

# Sources & objects
SRCDIR   := src
SOURCES  := $(SRCDIR)/main.c \
            $(SRCDIR)/conn_pool.c \
            $(SRCDIR)/schema_sync.c \
            $(SRCDIR)/data_sync.c \
            $(SRCDIR)/progress.c
OBJECTS  := $(SOURCES:.c=.o)
TARGET   := boosttools

# ── Rules ─────────────────────────────────────────────────────

.PHONY: all clean install info

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $^ $(LDFLAGS)
	@echo ""
	@echo "=== Build complete: ./$(TARGET) ==="
	@echo ""

$(SRCDIR)/%.o: $(SRCDIR)/%.c $(SRCDIR)/boost.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJECTS) $(TARGET)
	rm -rf build/

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

info:
	@echo "CC:          $(CC)"
	@echo "CFLAGS:      $(CFLAGS)"
	@echo "LDFLAGS:     $(LDFLAGS)"
	@echo "TDENGINE_DIR: $(TDENGINE_DIR)"
	@echo "SOURCES:     $(SOURCES)"
