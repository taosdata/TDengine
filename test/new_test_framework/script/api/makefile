# Copyright (c) 2017 TAOS Data, Inc. <jhtao@taosdata.com>
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

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver/' -ltaos -lpthread -lm -lrt
CFLAGS = -O0 -g -Wno-deprecated -fPIC -Wno-unused-result -Wconversion \
	-Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX \
	-Wno-unused-function -D_M_X64 -I/usr/local/taos/include -std=gnu99 -Wno-sign-conversion
#CFLAGS +=  -fsanitize=address -fsanitize=undefined -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./batchprepare.c  -o $(ROOT)batchprepare  $(LFLAGS)
	# gcc $(CFLAGS) ./stmt2-test.c  -o $(ROOT)stmt2-test $(LFLAGS)
	gcc $(CFLAGS) ./stopquery.c  -o $(ROOT)stopquery $(LFLAGS)
	gcc $(CFLAGS) ./dbTableRoute.c  -o $(ROOT)dbTableRoute $(LFLAGS)
	gcc $(CFLAGS) ./insertSameTs.c  -o $(ROOT)insertSameTs $(LFLAGS)
	gcc $(CFLAGS) ./passwdTest.c  -o $(ROOT)passwdTest $(LFLAGS)
	gcc $(CFLAGS) ./whiteListTest.c  -o $(ROOT)whiteListTest $(LFLAGS)
	gcc $(CFLAGS) ./insert_stb.c  -o $(ROOT)insert_stb $(LFLAGS)
	gcc $(CFLAGS) ./tmqViewTest.c  -o $(ROOT)tmqViewTest $(LFLAGS)
	gcc $(CFLAGS) ./stmtQuery.c  -o $(ROOT)stmtQuery $(LFLAGS)
	gcc $(CFLAGS) ./asyncQuery.c  -o $(ROOT)asyncQuery $(LFLAGS)
	# gcc $(CFLAGS) ./stmt.c  -o $(ROOT)stmt $(LFLAGS)
	gcc $(CFLAGS) ./stmt2.c  -o $(ROOT)stmt2 $(LFLAGS)
	gcc $(CFLAGS) ./stmt2-example.c  -o $(ROOT)stmt2-example $(LFLAGS)
	# gcc $(CFLAGS) ./stmt2-get-fields.c  -o $(ROOT)stmt2-get-fields $(LFLAGS)
	# gcc $(CFLAGS) ./stmt2-nohole.c  -o $(ROOT)stmt2-nohole $(LFLAGS)
	gcc $(CFLAGS) ./stmt-crash.c  -o $(ROOT)stmt-crash $(LFLAGS)
	gcc $(CFLAGS) ./stmt-insert-dupkeys.c  -o $(ROOT)stmt-insert-dupkeys $(LFLAGS)
	gcc $(CFLAGS) ./stmt2-insert-dupkeys.c  -o $(ROOT)stmt2-insert-dupkeys $(LFLAGS)
	gcc $(CFLAGS) ./insertBlob.c  -o $(ROOT)insertBlob $(LFLAGS)
	gcc $(CFLAGS) ./stmt2-performance.c  -o $(ROOT)stmt2-performance $(LFLAGS)
	gcc $(CFLAGS) ./stmt2-blob-performance.c  -o $(ROOT)stmt2-blob-performance $(LFLAGS)

clean:
	rm $(ROOT)batchprepare
	rm $(ROOT)stopquery
	rm $(ROOT)dbTableRoute
	rm $(ROOT)insertSameTs
	rm $(ROOT)passwdTest
	rm $(ROOT)whiteListTest
	rm $(ROOT)insert_stb
	rm $(ROOT)tmqViewTest
	rm $(ROOT)stmtQuery
	rm $(ROOT)stmt2
	rm $(ROOT)stmt2-example
	rm $(ROOT)stmt-crash
	rm $(ROOT)stmt-insert-dupkeys
	rm $(ROOT)stmt2-insert-dupkeys
	rm $(ROOT)insertBlob
	rm $(ROOT)stmt2-performance
	rm $(root)stmt2-blob-performance

