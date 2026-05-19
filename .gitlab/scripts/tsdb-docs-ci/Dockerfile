FROM node:24-bookworm-slim

ARG TYPOS_VERSION=v1.46.1
ARG AUTOCORRECT_VERSION=2.14.0
ARG MARKDOWNLINT_VERSION=0.22.1

# Use internal Nexus apt proxy for faster package downloads.
# Nexus uses a self-signed cert; tell apt to skip peer verification for this host only.
RUN sed -i \
      's|URIs: http://deb.debian.org/debian$|URIs: https://nexus.tdengine.net/repository/debian12|; \
       s|URIs: http://deb.debian.org/debian-security|URIs: https://nexus.tdengine.net/repository/debian12-security|' \
      /etc/apt/sources.list.d/debian.sources \
    && echo 'Acquire::https::nexus.tdengine.net::Verify-Peer "false";' \
       > /etc/apt/apt.conf.d/99nexus-ssl

# System tools: git for workspace ops; curl/ca-certificates for downloads;
# python3/make/g++ as fallback build deps for node-gyp native addons during yarn install.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git \
       bash \
       ca-certificates \
       curl \
       openssh-client \
       python3 \
       make \
       g++ \
    && rm -rf /var/lib/apt/lists/*

# Node.js-based linting tools: markdownlint-cli2 and autocorrect (NAPI.RS binary via npm).
RUN npm install -g \
       markdownlint-cli2@${MARKDOWNLINT_VERSION} \
       autocorrect-node@${AUTOCORRECT_VERSION} \
    && npm cache clean --force

# typos: pre-built static binary from GitHub Releases, no compiler needed.
RUN arch=$(uname -m) \
    && case "$arch" in \
         x86_64)  TYPOS_ARCH="x86_64-unknown-linux-musl"  ;; \
         aarch64) TYPOS_ARCH="aarch64-unknown-linux-musl" ;; \
         *)       echo "Unsupported arch: $arch" && exit 1 ;; \
       esac \
    && curl -fsSL \
         "https://github.com/crate-ci/typos/releases/download/${TYPOS_VERSION}/typos-${TYPOS_VERSION}-${TYPOS_ARCH}.tar.gz" \
       | tar -xz -C /tmp \
    && install -m 755 /tmp/typos /usr/local/bin/typos \
    && rm -f /tmp/typos

WORKDIR /root/gitlab_doc_ci_work
