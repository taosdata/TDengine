# NOTE: build image on arm64 machine

FROM rust:latest

ENV DATABASE_URL=sqlite:/app/target/taosx.dev.db

RUN apt-get update && apt-get install -y llvm gcc make cmake libssl-dev pkg-config perl g++
