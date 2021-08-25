CFLAGS += -g -Wall# -DHTTP_IMPORT_DEBUG
INCOPTS = -I/usr/include -I/usr/local/include -I.
LIBOPTS = -L/usr/local/lib -lmseed -ltaos -lcurl

SRCS = base64.c httpImport.c
OBJS = $(SRCS:.c=.o)

TARGET = httpImport

all: $(TARGET)

clean:
	rm -rf *.o $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBOPTS)

.c.o:
	$(CC) $(CFLAGS) $(INCOPTS) -c $< -o $@ $(LIBOPTS)
