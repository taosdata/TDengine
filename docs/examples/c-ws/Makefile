# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

TARGETS = connect_example       \
          create_db_demo        \
          insert_data_demo      \
          query_data_demo       \
          with_reqid_demo       \
          sml_insert_demo       \
          stmt_insert_demo      \
          tmq_demo

LIBS = -ltaosws -lpthread

CFLAGS = -g

all: $(TARGETS)

$(TARGETS):
	$(CC) $(CFLAGS) -o $@ $(wildcard $(@F).c) $(LIBS)

clean:
	rm -f $(TARGETS)
