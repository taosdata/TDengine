# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver' -ltaos -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 -std=gnu99

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./asyncdemo.c -o $(ROOT)/asyncdemo $(LFLAGS)
	gcc $(CFLAGS) ./demo.c -o $(ROOT)/demo $(LFLAGS)
	gcc $(CFLAGS) ./stream.c -o $(ROOT)/stream $(LFLAGS)
	gcc $(CFLAGS) ./subscribe.c -o $(ROOT)/subscribe $(LFLAGS)

clean:
	rm $(ROOT)asyncdemo
	rm $(ROOT)demo
	rm $(ROOT)stream
	rm $(ROOT)subscribe
	
	