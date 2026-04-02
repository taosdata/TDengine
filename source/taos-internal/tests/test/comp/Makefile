CC     := g++
COPTS   := -std=c++11 -g -Iinc -Isrc -I../../vnode/inc -I../../inc
COBJDIR := ../../../build/obj/vnode
LOPTS   := -L$(COBJDIR)
OBJDIR := ./obj
OBJS   := $(OBJDIR)/test.o\
	$(OBJDIR)/TestTimestamp.o\
	$(OBJDIR)/TestBool.o\
	$(OBJDIR)/TestString.o\
	$(OBJDIR)/TestDouble.o\
	$(OBJDIR)/TestFloat.o\
	$(OBJDIR)/TestInt.o

COBJS   := $(COBJDIR)/tsCompression.o\
	$(COBJDIR)/lz4.o


.PHONY: compile link
link: compile
	$(CC) $(OBJS) $(COBJS) -o test

compile: $(OBJS)

$(OBJDIR)/%.o : src/%.c
	$(CC) -c $^ -o $@ $(COPTS)

$(OBJDIR)/%.o : src/%.cpp
	$(CC) -c $^ -o $@ $(COPTS)


run: compile link
	./test


.PHONY: clean

clean:
	rm obj/*.o
