# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/lib/taos/' -ltaos -lpthread -lm -lrt 
# LFLAGS = -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 \
	-I../../inc -I../../client/inc -std=gnu99

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./async_query.c -o $(ROOT)/async_query $(LFLAGS)
	gcc $(CFLAGS) ./batch_import.c -o $(ROOT)/batch_import $(LFLAGS)
	gcc $(CFLAGS) ./column256.c -o $(ROOT)/column256 $(LFLAGS)
	#gcc $(CFLAGS) ./async_query.c ../../../build/lib/libtaos.a ../../thirdparty/libiconv/debian/libiconv.a $(LFLAGS) -o $(ROOT)/async_query
	#gcc $(CFLAGS) ./batch_import.c ../../../build/lib/libtaos.a  ../../thirdparty/libiconv/debian/libiconv.a $(LFLAGS) -o $(ROOT)/batch_import
	#gcc $(CFLAGS) ./column256.c ../../../build/lib/libtaos.a  ../../thirdparty/libiconv/debian/libiconv.a $(LFLAGS) -o $(ROOT)/column256

clean:
	rm $(ROOT)/async_query
	rm $(ROOT)/batch_import
	rm $(ROOT)/column256
	
	
