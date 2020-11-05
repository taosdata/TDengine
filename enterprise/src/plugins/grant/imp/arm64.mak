# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Shengliang Guan

TARGET_DIR = ../src/

TARGET = $(TARGET_DIR)/machine.o

CFLAGS = -c -std=gnu99 -Wall -fPIC -g -fsigned-char -fpack-struct=8  \
    -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DLINUX -D_TD_ARM_64        \
    -I../../../../../community/src/os/linux/inc/      \
    -I../../../../../community/src/client/inc/        \
    -I../../../../../community/src/inc/               \
    -I../../../../../community/src/util/inc/          \
    -I../../../../../community/src/mnode/inc/         \
    -I../../../../../community/src/dnode/inc/         \
    -I../../../../../community/src/common/inc/        \
    -I../../../../../community/src/os/inc/            \
    -I../../../inc                                    \
    -I../inc
    
all : $(TARGET)

$(TARGET_DIR)/%.o : ./%.c
	@mkdir -p $(@D)
	gcc -o $@ $< $(CFLAGS)
	@mv ../src/machine.o ../src/machine_arm64.o

clean:
	rm -rf $(TARGET)
