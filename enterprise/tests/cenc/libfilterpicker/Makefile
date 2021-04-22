####### Compiler, tools and options

CC            = gcc
CXX           = g++
CFLAGS        = -fPIC -g -Wall
CXXFLAGS      = -fPIC -g -Wall
INCPATH       = -I. -I/usr/local/include
LINK          = gcc
LFLAGS        = -shared -L/usr/local/lib
TFLAGS        = -L. -L/usr/local/lib -lmseed -ltaos -lcurl -lm
COPY          = cp -f
SED           = sed
DEL_FILE      = rm -f

####### Output directory

####### Files

SOURCES       = PickData.c \
		FilterPicker5.c \
		FilterPicker5_Memory.c \
		callback_udf_func.c
SUBSRCS       = subscribe.c
OBJECTS       = PickData.o \
		FilterPicker5.o \
		FilterPicker5_Memory.o \
		callback_udf_func.o
SUBOBJS       = subscribe.o
DYNLIB        = libfilterpicker.so
TARGET        = subscribe


####### Implicit rules

.SUFFIXES: .o .c .cpp .cc .cxx .C

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"

.cc.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"

.cxx.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"

.C.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"

.c.o:
	$(CC) -c $(CFLAGS) $(INCPATH) -o "$@" "$<"

####### Build rules

all: $(DYNLIB) $(TARGET)

$(DYNLIB): $(OBJECTS)  
	$(LINK) -o $(DYNLIB) $(OBJECTS) $(LFLAGS) 

$(TARGET): $(SUBOBJS)
	$(LINK) -o $(TARGET) $(SUBOBJS) -lfilterpicker $(TFLAGS)

clean:
	-$(DEL_FILE) $(DYNLIB) $(TARGET) $(OBJECTS) $(SUBOBJS)


####### Sub-libraries

distclean: clean
	-$(DEL_FILE) $(TARGET)


####### Compile

PickData.o: PickData.c ew_bridge.h \
		PickData.h
	$(CC) -c $(CFLAGS) $(INCPATH) -o PickData.o PickData.c

FilterPicker5.o: FilterPicker5.c ew_bridge.h \
		PickData.h \
		FilterPicker5_Memory.h \
		FilterPicker5.h
	$(CC) -c $(CFLAGS) $(INCPATH) -o FilterPicker5.o FilterPicker5.c

FilterPicker5_Memory.o: FilterPicker5_Memory.c ew_bridge.h \
		PickData.h \
		FilterPicker5_Memory.h
	$(CC) -c $(CFLAGS) $(INCPATH) -o FilterPicker5_Memory.o FilterPicker5_Memory.c

callback_udf_func.o: callback_udf_func.c \
                callback_udf_func.h
	$(CC) -c $(CFLAGS) $(INCPATH) -o callback_udf_func.o callback_udf_func.c

subscribe.o: subscribe.c
	$(CC) -c $(CFLAGS) $(INCPATH) -o subscribe.o subscribe.c

####### Install

install:
	test -d "/usr/local/lib" || mkdir -p "/usr/local/lib"
	test ! -f "/usr/local/lib/$(DYNLIB)" \
		|| mv "/usr/local/lib/$(DYNLIB)" "/usr/local/lib/$(DYNLIB).old"
	cp $(DYNLIB) "/usr/local/lib"

uninstall:
	test -d "/usr/local/lib" && rm -rf "/usr/local/lib/$(DYNLIB)"
