# Makefile.mak for win64

TARGET = passwdTest.exe
CC = cl
CFLAGS = /W4 /EHsc /I"C:\TDengine\include" /D_WINDOWS
LDFLAGS = /link /LIBPATH:"C:\TDengine\driver" taos.lib

SRCS = passwdTest.c
OBJS = $(SRCS:.c=.obj)

all: $(TARGET)

$(TARGET): $(OBJS)
    $(CC) $(OBJS) $(LDFLAGS)

.c.obj:
    $(CC) $(CFLAGS) /c $<

clean:
    del $(OBJS) $(TARGET)