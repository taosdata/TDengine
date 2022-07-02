all:
	# cargo build
	gcc -g show-databases.c -I../ -I./ -L../../target/debug/ -L./ --static -ltaos_ws_sys -o show-databases -lm

.phony: all
