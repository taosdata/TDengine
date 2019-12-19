target := jinfo
CFLAGS := -g -O3
CC := gcc

$(target): main.cpp parser.cpp
	$(CC) -o $@ $^ $(CFLAGS) $(LFLAGS)

clean:
	rm $(target)
