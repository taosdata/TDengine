target = insertOpentsdb

SRC_DIR = src
OBJ_DIR = obj
LIB_DIR = ../../../LIB/C

src = $(wildcard src/*.c)
obj = $(src:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

CFLAGS = -g -c -Iinc -I$(LIB_DIR)/inc
LFLAGS = -g -lpthread -lcurl -ljson

$(target):$(obj) $(LIB_DIR)/libcomp.a
	gcc -o $@ $^ $(LFLAGS)

$(OBJ_DIR)/%.o : $(SRC_DIR)/%.c
	@mkdir -p $(@D)
	gcc -o $@ $^ $(CFLAGS)

$(LIB_DIR)/libcomp.a:
	make -C $(LIB_DIR)

.PHONY : clean

clean:
	rm -rf $(OBJ_DIR) $(target)
