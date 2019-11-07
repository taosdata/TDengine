# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao

ROOT = ../../../build
TARGET = $(ROOT)/lib/libjansson.a
OBJ = $(ROOT)/obj/thirdparty/jansson

OBJFILES = \
	$(OBJ)/dump.o\
	$(OBJ)/error.o \
	$(OBJ)/hashtable.o \
	$(OBJ)/hashtable_seed.o \
	$(OBJ)/load.o \
	$(OBJ)/memory.o \
	$(OBJ)/pack_unpack.o \
	$(OBJ)/strbuffer.o \
	$(OBJ)/strconv.o \
	$(OBJ)/utf.o \
	$(OBJ)/value.o

CC = gcc
CFLAGS = -c -DHAVE_CONFIG_H -g -Wall -Wno-char-subscripts -D_REENTRANT -DLINUX -I./src -I../inc

OS = $(shell echo `uname -a | grep Ubuntu | wc -l`)
ifeq ($(OS), 0)
CFLAGS += -std=gnu99
endif #centos

#-MT 
all: $(TARGET)

$(TARGET):$(OBJFILES)
	@mkdir -p $(@D)
	ar -rc $@ $^

$(OBJ)/%.o:src/%.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c $^ -o $@

clean :
	$(RM) -r $(OBJ) $(TARGET)

