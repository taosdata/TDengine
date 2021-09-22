# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver/' -ltaos -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion \
	-Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX \
	-Wno-unused-function -D_M_X64 -I/usr/local/taos/include -std=gnu99 \
	-I../../../src/os/inc -I../../../src/inc \
	-I../../../src/util/inc -I../../../src/common/inc
all: $(TARGET)

exe:
	gcc $(CFLAGS) ./asyncdemo.c -o $(ROOT)asyncdemo $(LFLAGS)
	gcc $(CFLAGS) ./demo.c -o $(ROOT)demo $(LFLAGS)
	gcc $(CFLAGS) ./prepare.c -o $(ROOT)prepare $(LFLAGS)
	gcc $(CFLAGS) ./stream.c -o $(ROOT)stream $(LFLAGS)
	gcc $(CFLAGS) ./subscribe.c -o $(ROOT)subscribe $(LFLAGS)
	gcc $(CFLAGS) ./apitest.c -o $(ROOT)apitest $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest.c -o $(ROOT)clientcfgtest $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest-wrongtype.c -o $(ROOT)clientcfgtest-wrongtype $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest-wrongjson.c -o $(ROOT)clientcfgtest-wrongjson $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest-wrongvalue.c -o $(ROOT)clientcfgtest-wrongvalue $(LFLAGS)
	gcc $(CFLAGS) ./clientcfgtest-taosd.c -o $(ROOT)clientcfgtest-taosd $(LFLAGS)


clean:
	rm $(ROOT)asyncdemo
	rm $(ROOT)demo
	rm $(ROOT)prepare
	rm $(ROOT)batchprepare
	rm $(ROOT)stream
	rm $(ROOT)subscribe
	rm $(ROOT)apitest
	rm $(ROOT)clientcfgtest
	rm $(ROOT)clientcfgtest-wrongtype
	rm $(ROOT)clientcfgtest-wrongjson
	rm $(ROOT)clientcfgtest-wrongvalue
	rm $(ROOT)clientcfgtest-taosd

