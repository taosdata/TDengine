.DEFAULT:
	cargo make --version || cargo install --locked cargo-make
	cargo make $@
