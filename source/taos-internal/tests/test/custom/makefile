# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao
# todo: library dependency, header file dependency

ROOT=../../../build
TARGET=exe
LIB=$(ROOT)/lib/libtaos.a -lpthread -lm -lrt
CFLAGS = -g -Wall -fPIC -Wno-char-subscripts -malign-double -malign-stringops -D_REENTRANT -DLINUX -I../inc -I../../inc -I../../util/inc -I../../taos/inc -I../../client/inc -I../perftest/inc -DLZ4_DISABLE_DEPRECATE_WARNINGS 

OS = $(shell echo `uname -a | grep Ubuntu | wc -l`)
ifeq ($(OS), 0)
CFLAGS += -std=gnu99
endif #centos

all: $(TARGET)

exe:
	#gcc -o $(ROOT)/bin/hntl_test            hntl_test.c -lpthread -ltaos -lrt -lm -I../../inc  -L$(ROOT)/lib 
	#gcc -o $(ROOT)/bin/hntl_create_table    hntl_create_table.c -lpthread -ltaos -lrt -lm -I../../inc  -L$(ROOT)/lib $(ROOT)/lib/libmodules.a
	#g++ -o $(ROOT)/bin/hntl_insert_data     hntl_insert_data.cpp -lpthread -ltaos -lrt -lm -I../../inc  -L$(ROOT)/lib
	#g++ -o $(ROOT)/bin/hntl_parse_table     hntl_parse_table.cpp -lrt -lm -I../../inc  -L$(ROOT)/lib
	#g++ -o $(ROOT)/bin/hntl_parse_data      hntl_parse_data.cpp -lrt -lm -I../../inc  -L$(ROOT)/lib
	#g++  -o $(ROOT)/bin/taosGoldWindGenData  taosGoldWindGenData.cpp -lrt -lm -I../../inc  -L$(ROOT)/lib
	#gcc  -o $(ROOT)/bin/test_many_tables     test_many_tables.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules $(ROOT)/lib/libtaos.a -lutil -lpthread -lm -lrt
	gcc  -o $(ROOT)/bin/taosH3C              taosH3C.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosTest             taosTest.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules $(ROOT)/lib/libtaos.a -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosMVB             taosMVB.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules $(ROOT)/lib/libtaos.a -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtCreateTable1Thread    taosZddtCreateTable1Thread.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtCreateTablenThread    taosZddtCreateTablenThread.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtCreateTableViaInsertNThread    taosZddtCreateTableViaInsertNThread.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtInsertNThread    taosZddtInsertNThread.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtInsertNThreadBack    taosZddtInsertNThreadBack.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt
	#gcc  -o $(ROOT)/bin/taosZddtOdbc    taosZddtOdbc.c -O3 -g -I../../inc -L$(ROOT)/lib -lsdb -ltrpc -lmodules -ltaos -lutil -lpthread -lm -lrt -lodbc 
	

clean:
	#rm $(ROOT)/bin/hntl*
	#rm -rf $(ROOT)/bin/taosGoldWindGenData
	#rm -rf $(ROOT)/bin/taosGoldWind
	#rm -rf $(ROOT)/bin/taosTest
	rm -rf $(ROOT)/bin/test_many_tables
