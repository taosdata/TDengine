# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver/' -ltaos -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion \
	-Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX \
	-Wno-unused-function -D_M_X64 -I/usr/local/taos/include -std=gnu99 \
	-I/usr/local/include/cjson
all: $(TARGET)

exe:
	gcc $(CFLAGS) ./asyncdemo.c -o $(ROOT)asyncdemo $(LFLAGS)
	gcc $(CFLAGS) ./demo.c -o $(ROOT)demo $(LFLAGS)
	gcc $(CFLAGS) ./prepare.c -o $(ROOT)prepare $(LFLAGS)
	gcc $(CFLAGS) ./stream_demo.c -o $(ROOT)stream_demo $(LFLAGS)
	gcc $(CFLAGS) ./tmq.c -o $(ROOT)tmq $(LFLAGS)
	gcc $(CFLAGS) ./schemaless.c -o $(ROOT)schemaless $(LFLAGS)

clean:
	rm $(ROOT)asyncdemo
	rm $(ROOT)demo
	rm $(ROOT)prepare
	rm $(ROOT)stream_demo
	rm $(ROOT)tmq
	rm $(ROOT)schemaless
