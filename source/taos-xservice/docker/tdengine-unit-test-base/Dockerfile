# Base image for unit-test CI: Rust toolchain + cargo coverage tools.
# Built once per Rust version; does NOT contain TDengine (version-independent).
# Tagged as: tdengine-unit-test-base:rust-{RUST_VERSION}
ARG RUST_VERSION=1.90.0
FROM rust:${RUST_VERSION}-bookworm
LABEL maintainer="TaosX CI Team"

# Install all components required by rust-toolchain.toml so rustup does not
# re-download them at test time when the project source is bind-mounted.
RUN rustup component add llvm-tools-preview clippy rustfmt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget curl netcat-openbsd ca-certificates lcov \
    bison flex cmake pkg-config libtool libtool-bin \
    protobuf-compiler \
    locales tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/ /var/cache/apt/

# Configure locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

# Compile and install Cargo tools (slowest step — cached by Rust version)
RUN cargo install cargo-make --locked && \
    cargo install cargo-nextest@0.9.128 --locked && \
    cargo install cargo-llvm-cov --locked && \
    rm -rf /root/.cargo/registry /root/.cargo/git
