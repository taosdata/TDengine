build_dir := ../../../build/bin
obj_dir := ../../../build/obj
target := $(build_dir)/testCRC32C
OBJS   := $(obj_dir)/util/crc32c.o
CFLAGS := -g -I../../inc
LFLAGS := -L../../../build/obj/util
CC := gcc

$(target): testCRC32C.c $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS) $(LFLAGS)

clean:
	rm $(target)
