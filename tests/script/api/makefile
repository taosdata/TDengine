# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver/' -ltaos -lpthread -lm -lrt
CFLAGS = -O0 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion \
	-Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX \
	-Wno-unused-function -D_M_X64 -I/usr/local/taos/include -std=gnu99 \
	-I../../../deps/cJson/inc\
	-I../../../src/os/inc/ -I../../../src/inc -I../../../src/util/inc \
	-I../../../src/common/inc -I../../../deps/cJson/inc \
	-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./batchprepare.c  -o $(ROOT)batchprepare  $(LFLAGS)
	gcc $(CFLAGS) ./stmtBatchTest.c -o $(ROOT)stmtBatchTest $(LFLAGS)
	gcc $(CFLAGS) ./stmtTest.c -o $(ROOT)stmtTest $(LFLAGS)
	gcc $(CFLAGS) ./stmt.c -o $(ROOT)stmt $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest.c -o $(ROOT)clientcfgtest $(LFLAGS)
	gcc $(CFLAGS) ./openTSDBTest.c -o $(ROOT)openTSDBTest $(LFLAGS)
	gcc $(CFLAGS) ./resultBlock.c -o $(ROOT)resultBlock $(LFLAGS)


clean:
	rm $(ROOT)batchprepare
	rm $(ROOT)stmtBatchTest
	rm $(ROOT)stmtTest
	rm $(ROOT)stmt
	rm $(ROOT)stmt_function
	rm $(ROOT)clientcfgtest
	rm $(ROOT)openTSDBTest
	rm $(ROOT)resultBlock

