# Compile C library libtaosws with Rust

```sh
cargo build --release -p taos-ws-sys
./taos-ws-sys/ci/package.sh
# Check the C header file taosws.h and library files:
#   libtaosws.so/libtaosws.a in target/libtaosws/ directory.
ls target/libtaosws*
```
