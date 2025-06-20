FROM ubuntu:24.04

ENV PATH=/zig-linux-x86_64-0.13.0:$PATH
ENV ac_cv_printf_positional=yes
ENV ac_cv_func_regcomp=yes
ENV krb5_cv_attr_constructor_destructor=yes

COPY zig-linux-x86_64-0.13.0.tar.xz .
COPY config.toml /root/.cargo/config.toml
# RUN wget https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz && tar -xf zig-linux-x86_64-0.13.0.tar.xz -C /
RUN apt update && apt install -y wget gcc make cmake libssl-dev pkg-config perl g++ gcc-aarch64-linux-gnu curl xz-utils && \
    tar -xf zig-linux-x86_64-0.13.0.tar.xz -C /

ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.87.0
ENV PATH=/root/.cargo/bin:$PATH
RUN . "$HOME/.cargo/env" && rustup target add aarch64-unknown-linux-gnu && cargo install cargo-zigbuild


