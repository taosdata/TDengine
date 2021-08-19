####### Compiler, tools and options

CC            = gcc
CXX           = g++
CFLAGS        = -O0 -pipe -g -Wall# -DDELAY_DEBUG
CXXFLAGS      = -O0 -pipe -g -Wall
INCPATH       = -I. -I/usr/local/include
LINK          = gcc
LFLAGS        = -L/usr/local/lib -lmseed -ltaos -lcurl -lm -lpthread
COPY          = cp -f
SED           = sed
DEL_FILE      = rm -f

####### Output directory

####### Files

SOURCES       = packetDelay.c base64.c
OBJECTS       = packetDelay.o base64.o
TARGET        = packetDelay


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

all: $(TARGET)

$(TARGET): $(OBJECTS)  
	$(LINK) -o $(TARGET) $(OBJECTS) $(LFLAGS) 

clean:
	-$(DEL_FILE) $(TARGET) $(OBJECTS)


####### Sub-libraries

distclean: clean
	-$(DEL_FILE) $(TARGET) 


####### Compile

base64.o: base64.c
	$(CC) -c $(CFLAGS) $(INCPATH) -o base64.o base64.c

packetDelay.o: packetDelay.c
	$(CC) -c $(CFLAGS) $(INCPATH) -o packetDelay.o packetDelay.c

####### Install

install: FORCE

uninstall: FORCE

FORCE:

