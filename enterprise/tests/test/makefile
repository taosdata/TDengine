# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao
# todo: library dependency, header file dependency

ROOT=../../build
TARGET=exe
LIB=$(ROOT)/lib/libtaos.a -lpthread -lm -lrt ../thirdparty/libiconv/debian/libiconv.a
CFLAGS = -g -Wall -fPIC -Wno-char-subscripts -malign-double -malign-stringops -D_REENTRANT -DLINUX -Iinc -I../inc -I../util/inc -I../taos/inc -I../client/inc -Iperftest/inc -DLZ4_DISABLE_DEPRECATE_WARNINGS 

OS = $(shell echo `uname -a | grep Ubuntu | wc -l`)
ifeq ($(OS), 0)
CFLAGS += -std=gnu99
endif #centos

all: $(TARGET)

exe:
	gcc $(CFLAGS) cdriver/case_power.c -o $(ROOT)/bin/case_power $(LIB)
	#gcc -o $(ROOT)/bin/simulate_test cdriver/simulate_test.c -lpthread -ltaos -lrt -lm -I../inc  -L$(ROOT)/lib
	gcc $(CFLAGS) cdriver/synctest.c -o $(ROOT)/bin/synctest $(LIB)
	gcc $(CFLAGS) cdriver/asynctest.c -o $(ROOT)/bin/asynctest $(LIB)
	#gcc $(CFLAGS) cdriver/aquery.c -o $(ROOT)/bin/aquery $(LIB)
	gcc $(CFLAGS) cdriver/tquery.c -o $(ROOT)/bin/tquery $(LIB)
	gcc $(CFLAGS) cdriver/syncinsert.c -o $(ROOT)/bin/syncinsert $(LIB)
	gcc $(CFLAGS) cdriver/subtest.c -o $(ROOT)/bin/subtest $(LIB)
	gcc $(CFLAGS) cdriver/stream.c -o $(ROOT)/bin/stream $(LIB)
	gcc $(CFLAGS) perftest/queryTest.c perftest/testCommon.c -o $(ROOT)/bin/queryTest $(LIB)
	#gcc $(CFLAGS) perftest/taosperf.c perftest/perftestcommon.c -o $(ROOT)/bin/taosperf $(LIB)
	#gcc $(CFLAGS) cdriver/syncloadjson.c -o $(ROOT)/bin/syncloadjson $(LIB)
	# gcc $(CFLAGS) cdriver/simulate_test.c -o $(ROOT)/bin/simulate_test $(LIB)

clean:
#	rm $(ROOT)/bin/case_power
	rm $(ROOT)/bin/synctest
	rm $(ROOT)/bin/asynctest
	rm $(ROOT)/bin/query
#	rm $(ROOT)/bin/aquery
	rm $(ROOT)/bin/syncinsert
	rm $(ROOT)/bin/subtest
	rm $(ROOT)/bin/sqltest

	
