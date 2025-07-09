FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

ARG ZIG_VERSION=0.14.1
COPY ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources
RUN apt update && apt install -y wget gcc make cmake libssl-dev pkg-config perl g++ gcc-aarch64-linux-gnu curl xz-utils ca-certificates

COPY zig-x86_64-linux-${ZIG_VERSION}.tar.xz .
RUN tar -xf zig-x86_64-linux-${ZIG_VERSION}.tar.xz --strip-components 1 -C /usr/bin/

ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
COPY config.toml /root/.cargo/config.toml
ENV PATH=/root/.cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.87.0 --component clippy,rustfmt \
  && . /root/.cargo/env && rustup target add aarch64-unknown-linux-gnu && cargo install cargo-zigbuild && rm -rf /tmp/tmp*

ENV ac_cv_printf_positional=yes
ENV ac_cv_func_regcomp=yes
ENV krb5_cv_attr_constructor_destructor=yes
