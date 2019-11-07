# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao
# todo: library dependency, header file dependency

ROOT=../../../build
TARGET=exe
LIB=$(ROOT)/lib/libtaos.a $(ROOT)/lib/libtutil.a $(ROOT)/lib/libtrpc.a -L$(ROOT)/lib -lpthread -lm -lrt
CFLAGS = -g -Wall -fPIC -Wno-char-subscripts -malign-double -malign-stringops -D_REENTRANT -DLINUX -I../inc -I../../inc -I../../util/inc -I../../taos/inc -I../../client/inc -I../perftest/inc -DLZ4_DISABLE_DEPRECATE_WARNINGS -I../../thirdparty/inc

OS = $(shell echo `uname -a | grep Ubuntu | wc -l`)
ifeq ($(OS), 0)
CFLAGS += -std=gnu99
endif #centos

all: $(TARGET)

exe:
	#gcc $(CFLAGS) ./async_query.c -o $(ROOT)/bin/async_query $(LIB)
	#gcc $(CFLAGS) ./sync_query.c -o $(ROOT)/bin/sync_query $(LIB)	
	#gcc $(CFLAGS) ./conn_repeat.c -o $(ROOT)/bin/conn_repeat $(LIB)	
	#gcc $(CFLAGS) ./column256.c -o $(ROOT)/bin/column256 $(LIB)	
	#gcc $(CFLAGS) ./processlist.c -o $(ROOT)/bin/processlist $(LIB)	
	#gcc $(CFLAGS) ./sync_insert.c -o $(ROOT)/bin/sync_insert $(LIB)
	#gcc $(CFLAGS) ./insert_test.c -o $(ROOT)/bin/insert_test $(LIB)
	#gcc $(CFLAGS) ./sync_import.c -o $(ROOT)/bin/sync_import $(LIB)
	#gcc $(CFLAGS) ./batch_import.c -o $(ROOT)/bin/batch_import $(LIB)
	gcc $(CFLAGS) ./ahx_query_from_stables.c -o $(ROOT)/bin/ahx_query_from_stables $(LIB)

clean:
	#rm $(ROOT)/bin/async_query
	#rm $(ROOT)/bin/sync_query
	#rm $(ROOT)/bin/conn_repeat
	#rm $(ROOT)/bin/column256
	#rm $(ROOT)/bin/grant
	#rm $(ROOT)/bin/processlist
	#rm $(ROOT)/bin/sync_insert
	#rm $(ROOT)/bin/sync_import
	rm $(ROOT)/bin/batch_import

	
