# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Shengliang Guan

TARGET_DIR = ../src/

TARGET = $(TARGET_DIR)/machine.o

CFLAGS =  -Werror -c -g -std=gnu99 -Wall -fPIC -malign-double -D_REENTRANT -DLINUX -D_TD_LINUX_64 \
    -I../../../../../community/include/os/            \
    -I../../../../../community/include/util/          \
    -I../inc
    
all : $(TARGET)
	@mv ../src/machine.o ../src/machine_linux64.o

$(TARGET_DIR)/%.o : ./%.c
	@mkdir -p $(@D)
	gcc -o $@ $< $(CFLAGS)

clean:
	rm -rf $(TARGET)
