# NOTE: build image on arm64 machine

FROM rust:latest

ENV DATABASE_URL=sqlite:/app/target/taosx.dev.db
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"

RUN apt-get update && apt-get install -y llvm gcc make cmake libssl-dev pkg-config perl g++

COPY config.toml /usr/local/cargo/
