# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver/' -ltaos -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion \
	-Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX \
	-Wno-unused-function -D_M_X64 -I/usr/local/taos/include -std=gnu99

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./asyncdemo.c -o $(ROOT)asyncdemo $(LFLAGS)
	gcc $(CFLAGS) ./demo.c -o $(ROOT)demo $(LFLAGS)
	gcc $(CFLAGS) ./prepare.c -o $(ROOT)prepare $(LFLAGS)
	gcc $(CFLAGS) ./stream.c -o $(ROOT)stream $(LFLAGS)
	gcc $(CFLAGS) ./subscribe.c -o $(ROOT)subscribe $(LFLAGS)
	gcc $(CFLAGS) ./apitest.c -o $(ROOT)apitest $(LFLAGS)

clean:
	rm $(ROOT)asyncdemo
	rm $(ROOT)demo
	rm $(ROOT)prepare
	rm $(ROOT)stream
	rm $(ROOT)subscribe
	rm $(ROOT)apitest
