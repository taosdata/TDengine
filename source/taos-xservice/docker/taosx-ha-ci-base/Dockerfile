ARG RUST_VERSION=1.90.0
FROM rust:${RUST_VERSION}-bookworm
LABEL maintainer="TaosX CI Team"
LABEL description="Base image with dependencies for taosx-ha-ci"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    wget curl jq sqlite3 openjdk-17-jre-headless ca-certificates \
    locales tzdata netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/ /var/cache/apt/

# Configure locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8

# Install llvm-tools-preview component
RUN rustup component add llvm-tools-preview

# Set locale
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

