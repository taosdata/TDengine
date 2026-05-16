# Cross-Compiling taosX for RISC-V

This guide explains how to build taosX for `riscv64gc-unknown-linux-gnu` on an x86_64 Linux host.

## Prerequisites

### 1. Rust target

```bash
rustup target add riscv64gc-unknown-linux-gnu
```

### 2. GCC cross-compiler and sysroot (Ubuntu/Debian)

```bash
sudo apt-get install \
  gcc-riscv64-linux-gnu \
  g++-riscv64-linux-gnu \
  libc6-dev-riscv64-cross
```

This installs `riscv64-linux-gnu-gcc` and the matching sysroot under
`/usr/riscv64-linux-gnu/`.

### 3. cargo-zigbuild (alternative, recommended for musl / older glibc)

If you need to target a specific minimum glibc version, use
[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) instead of the
GCC cross-compiler:

```bash
cargo install cargo-zigbuild
# Install Zig (required by cargo-zigbuild)
pip install ziglang
```

## Building

### With the GCC cross-compiler

```bash
cargo build --release --target riscv64gc-unknown-linux-gnu
```

The `build.rs` scripts for vendored C dependencies (e.g. `sasl2-sys`,
`krb5-src`) automatically detect `riscv64-linux-gnu-gcc` from `$PATH` and
normalize the Rust triple (`riscv64gc-unknown-linux-gnu` → `riscv64-linux-gnu`)
when passing `--host` to autotools' `configure`, so no manual `CC`/`AR`
environment variables are needed.

### With cargo-zigbuild (pinned glibc)

```bash
cargo zigbuild --release --target riscv64gc-unknown-linux-gnu.2.17
```

Replace `2.17` with the minimum glibc version required by the target system.

### Linking against a system-provided libsasl2

If you are **not** using the `vendored` feature of `sasl2-sys` and the
RISC-V sysroot contains `libsasl2`, point the linker at it:

```bash
export SASL2_LIB_DIR=/usr/riscv64-linux-gnu/lib
export SASL2_INCLUDE_DIR=/usr/riscv64-linux-gnu/include
cargo build --release --target riscv64gc-unknown-linux-gnu
```

## Cargo configuration

Add a `[target.riscv64gc-unknown-linux-gnu]` section to `.cargo/config.toml`
(create it in the repo root if it does not exist yet) to set the linker
permanently:

```toml
[target.riscv64gc-unknown-linux-gnu]
linker = "riscv64-linux-gnu-gcc"
```

## Background: Why the Rust triple must be normalized

Rust uses `riscv64gc-unknown-linux-gnu` as the target triple, where `gc` is a
RISC-V ISA extension group (general-purpose + compressed). Autotools'
`config.sub` and GCC cross-compiler binaries use the plain architecture name
`riscv64`, so:

| Context | Triple used |
|---------|-------------|
| Rust / `cargo` | `riscv64gc-unknown-linux-gnu` |
| GCC cross-compiler prefix | `riscv64-linux-gnu-` |
| autotools `--host` | `riscv64-linux-gnu` |

The `build.rs` scripts in `sasl2-sys` and `krb5-src` handle this normalization
automatically since the riscv cross-compile fix was applied.

## Troubleshooting

**`configure: error: cannot guess build type`**  
autotools received the raw Rust triple. This can happen if you are using a
version of the crate that does not include the RISC-V normalization fix. You
can work around this by manually specifying the cross-compiler tools for Cargo:

```bash
export CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER=riscv64-linux-gnu-gcc
export CC_riscv64gc_unknown_linux_gnu=riscv64-linux-gnu-gcc
export AR_riscv64gc_unknown_linux_gnu=riscv64-linux-gnu-ar
```

**`error: linker 'cc' not found`**  
The `.cargo/config.toml` linker override is missing. Add the section shown
above, or set the environment variable:

```bash
export CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER=riscv64-linux-gnu-gcc
```

**`cannot find -lsasl2`**  
The vendored build failed or system sasl2 is not in the sysroot. Either enable
the `vendored` feature or set `SASL2_LIB_DIR` / `SASL2_INCLUDE_DIR` as shown
above.
