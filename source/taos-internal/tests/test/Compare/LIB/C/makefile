target = libcomp.a

SRC_DIR = src
OBJ_DIR = obj

src = $(wildcard src/*.c)
obj = $(src:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

CFLAGS = -g -c -Iinc
LFLAGS = -g

$(target) : $(obj)
	ar -rc $@ $^

$(OBJ_DIR)/%.o : $(SRC_DIR)/%.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(CFLAGS)

.PHONY : clean

clean:
	rm -rf $(OBJ_DIR) $(target)
