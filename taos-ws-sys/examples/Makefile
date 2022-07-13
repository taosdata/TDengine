WS_FLAGS=-I. -I../../target/debug/ -L. -L../../target/debug/ -ltaosws -lm

all:
	# cargo build
	gcc -g show-databases.c ${WS_FLAGS} -o show-databases
	gcc -g ws-run-sql.c ${WS_FLAGS} -o ws-run-sql

.phony: all
