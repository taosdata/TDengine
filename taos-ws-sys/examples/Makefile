all:
	# cargo build
	gcc -g show-databases.c -I../../target/debug/ -L../../target/debug/ -L./ -ltaosws -o show-databases -lm
	gcc -g ws-run-sql.c -I../../target/debug/ -L../../target/debug/ -L./ -ltaosws -o ws-run-sql -lm

.phony: all
