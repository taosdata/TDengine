# Copyright (c) 2017 by TAOS Technologies, Inc.
# Created by Jianhui Tao

ROOT = ../../../build
TARGET = $(ROOT)/lib/libjwt.a
OBJ = $(ROOT)/obj/thirdparty/libjwt

OBJFILES = \
	$(OBJ)/jwt.o\
	$(OBJ)/base64.o \
	$(OBJ)/jwt-openssl.o \
#	$(OBJ)/jwt-gnutls.o \

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
	ar -rc $@ $^ $(ROOT)/lib/libjansson.a

$(OBJ)/%.o:src/%.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c $^ -o $@

clean :
	$(RM) -r $(OBJ) $(TARGET)

