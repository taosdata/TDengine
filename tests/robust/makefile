NAME=robust
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 -std=gnu99

all:
	gcc $(CFLAGS) ./$(NAME).c -o $(NAME) -ltaos -lpthread

clean:
	rm $(NAME)