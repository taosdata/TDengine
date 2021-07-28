CFLAGS += -g -O0 -Wall# -DIN_DATA_DEBUG -DHTTP_IMPORT_DEBUG
INCOPTS = -I/usr/include -I/usr/local/include -I.
LIBOPTS = -L/usr/local/lib -lmseed -ltaos -lcurl -lpthread

SRCS = urlencode.c base64.c httpImport.c
OBJS = $(SRCS:.c=.o)

TARGET = httpImport

all: $(TARGET)

clean:
	rm -rf *.o $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBOPTS)

.c.o:
	$(CC) $(CFLAGS) $(INCOPTS) -c $< -o $@ $(LIBOPTS)
