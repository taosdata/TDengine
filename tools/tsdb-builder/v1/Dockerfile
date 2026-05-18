# ============================================================================
# TDengine Build Environment Docker Image (Optimized)
# Base:
#   amd64: manylinux2014 (glibc 2.17) + devtoolset-10 (GCC 10.x) / CentOS 7
#   arm64: manylinux_2_28 (glibc 2.28) + GCC 12 / AlmaLinux 8
#         (Node 22 requires glibc >= 2.25; manylinux_2_28 satisfies this)
# Supports: amd64 and arm64 architectures
#
# Migration from CentOS 7 to manylinux with:
#   - Multi-stage build for architecture handling
#   - mold linker for faster compilation
#   - protoc for Protocol Buffers
#   - tini as init process
#   - Python 3.12 from manylinux pre-installed (no source compilation)
# ============================================================================

# ============================================================================
# Stage 1: Builder - Architecture variable mapping
# ============================================================================
# Use BUILD platform (host architecture) to avoid cross-compilation issues
FROM --platform=$BUILDPLATFORM docker.1ms.run/alpine AS builder
ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
ARG JDK_VERSION_AMD64=8u144
ARG JDK_VERSION_ARM64=8u441

RUN echo "Building for platform: ${TARGETPLATFORM}"

# Map TARGETPLATFORM to architecture-specific variables
RUN case "${TARGETPLATFORM}" in \
  "linux/amd64") \
    echo MOLD_ARCH=x86_64 > /etc/environment; \
    echo TINI_ARCH=amd64 >> /etc/environment; \
    echo PROTOC_ARCH=x86_64 >> /etc/environment; \
    echo GO_ARCH=amd64 >> /etc/environment; \
    echo JDK_ARCH=x64 >> /etc/environment; \
    echo JDK_VERSION=${JDK_VERSION_AMD64} >> /etc/environment; \
    echo CMAKE_ARCH=x86_64 >> /etc/environment; \
    echo DOTNET_ARCH=x64 >> /etc/environment; \
    echo OPENSSL_ARCH=linux-x86_64 >> /etc/environment; \
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_x86_64 >> /etc/environment; \
    ;; \
  "linux/arm64") \
    echo MOLD_ARCH=aarch64 > /etc/environment; \
    echo TINI_ARCH=arm64 >> /etc/environment; \
    echo PROTOC_ARCH=aarch_64 >> /etc/environment; \
    echo GO_ARCH=arm64 >> /etc/environment; \
    echo JDK_ARCH=aarch64 >> /etc/environment; \
    echo JDK_VERSION=${JDK_VERSION_ARM64} >> /etc/environment; \
    echo CMAKE_ARCH=aarch64 >> /etc/environment; \
    echo DOTNET_ARCH=arm64 >> /etc/environment; \
    echo OPENSSL_ARCH=linux-aarch64 >> /etc/environment; \
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux_2_28_aarch64 >> /etc/environment; \
    ;; \
  *) \
    echo "Unsupported platform: ${TARGETPLATFORM}"; \
    exit 1; \
    ;; \
  esac

RUN cat /etc/environment

# ============================================================================
# Stage 2: Main - Build environment
# Uses architecture-specific manylinux image:
#   amd64: quay.io/pypa/manylinux2014_x86_64  (glibc 2.17, CentOS 7)
#   arm64: quay.io/pypa/manylinux_2_28_aarch64 (glibc 2.28, AlmaLinux 8)
#          Node 22 arm64 requires glibc >= 2.25 — manylinux_2_28 satisfies this
# ============================================================================
FROM --platform=linux/amd64 quay.io/pypa/manylinux2014_x86_64 AS stage2-amd64
FROM --platform=linux/arm64 quay.io/pypa/manylinux_2_28_aarch64 AS stage2-arm64

FROM stage2-${TARGETARCH}

WORKDIR /home

# ============================================================================
# Build-time arguments for version management
# ============================================================================

# Development Tools Versions
ARG GO_VERSION=1.23.4
ARG MAVEN_VERSION=3.8.4
ARG CMAKE_VERSION=3.21.5
ARG RUST_VERSION=1.90.0
# Python: use manylinux2014 pre-installed Python (format: 3.12 → cp312-cp312)
ARG PYTHON_VERSION=3.12
ARG DOTNET_VERSION=6.0.100
ARG MOLD_VERSION=2.40.3
ARG PROTOC_VERSION=33.0
ARG TINI_VERSION=v0.19.0

# Python Package Versions
ARG TAOSPY_VERSION=2.8.8
ARG TAOS_WS_PY_VERSION=0.6.5

# Mirror Configuration
ARG PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/
ARG PYPI_TRUSTED_HOST=mirrors.aliyun.com
ARG GO_PROXY=https://goproxy.cn

# Timezone Configuration
ARG TIMEZONE=Asia/Shanghai

# Copy architecture variables from builder stage
COPY --from=builder /etc/environment /etc/

# Copy verification script into the image
COPY verify-image.sh /usr/local/bin/verify-image.sh
RUN chmod +x /usr/local/bin/verify-image.sh

# ============================================================================
# Set environment variables
# ============================================================================
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
    RUSTUP_DIST_SERVER=https://rsproxy.cn


# ============================================================================
# Layer 1: System base - yum repos + base packages
# amd64 (manylinux2014/CentOS 7): replaces repos with aliyun mirror
# arm64 (manylinux_2_28/AlmaLinux 8): uses pre-configured AlmaLinux 8 repos
# ============================================================================
RUN ARCH=$(uname -m) && \
    echo "retries=10" >> /etc/yum.conf && \
    echo "timeout=60" >> /etc/yum.conf && \
    if [ "$ARCH" != "aarch64" ]; then \
      BASE_URL="https://mirrors.aliyun.com/centos/7" && \
      { \
        echo "[base]"; \
        echo "name=CentOS-7 - Base"; \
        echo "baseurl=${BASE_URL}/os/\$basearch/"; \
        echo "gpgcheck=0"; \
        echo "enabled=1"; \
        echo ""; \
        echo "[updates]"; \
        echo "name=CentOS-7 - Updates"; \
        echo "baseurl=${BASE_URL}/updates/\$basearch/"; \
        echo "gpgcheck=0"; \
        echo "enabled=1"; \
        echo ""; \
        echo "[extras]"; \
        echo "name=CentOS-7 - Extras"; \
        echo "baseurl=${BASE_URL}/extras/\$basearch/"; \
        echo "gpgcheck=0"; \
        echo "enabled=1"; \
      } > /etc/yum.repos.d/CentOS-Base.repo; \
    fi && \
    yum makecache && \
    yum install -y \
      git wget curl unzip tar gzip bzip2 \
      libatomic openssl-devel zlib-devel libffi-devel \
      perl perl-IPC-Cmd perl-Time-Piece perl-Digest-SHA && \
    yum clean all && \
    rm -rf /var/cache/yum

# ============================================================================
# Layer 4: Go toolchain (from local installers/)
# ============================================================================
RUN --mount=type=bind,source=installers,target=/mnt/installers \
    . /etc/environment && \
    tar -C /usr/local -xzf /mnt/installers/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz && \
    rm -rf /usr/local/go/test/ /usr/local/go/api/ /usr/local/go/doc/

ENV GOROOT=/usr/local/go
ENV GOPATH=/root/go
ENV GOPROXY=${GO_PROXY}
ENV PATH=${GOROOT}/bin:${GOPATH}/bin:${PATH}

RUN go install github.com/magefile/mage@latest

# ============================================================================
# Layer 5: JDK + Maven (from local installers/)
# ============================================================================
RUN --mount=type=bind,source=installers,target=/mnt/installers \
    . /etc/environment && \
    tar -xzf /mnt/installers/jdk-${JDK_VERSION}-linux-${JDK_ARCH}.tar.gz -C /usr/local && \
    tar -xzf /mnt/installers/apache-maven-${MAVEN_VERSION}-bin.tar.gz -C /usr/local && \
    JDK_DIR=$(ls /usr/local | grep 'jdk1.8' | head -1) && \
    rm -rf /usr/local/${JDK_DIR}/src.zip \
           /usr/local/${JDK_DIR}/javafx-src.zip \
           /usr/local/${JDK_DIR}/man/ && \
    ln -sf /usr/local/${JDK_DIR} /usr/local/jdk

ENV JAVA_HOME=/usr/local/jdk
ENV M2_HOME=/usr/local/apache-maven-${MAVEN_VERSION}
ENV PATH=${JAVA_HOME}/bin:${M2_HOME}/bin:${PATH}

# ============================================================================
# Layer 6: CMake (from local installers/)
# ============================================================================
RUN --mount=type=bind,source=installers,target=/mnt/installers \
    . /etc/environment && \
    tar -xzf /mnt/installers/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz -C /usr/local && \
    rm -rf /usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/doc/ \
           /usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/man/ && \
    ln -sf /usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin/cmake /usr/local/bin/cmake && \
    ln -sf /usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin/ctest /usr/local/bin/ctest && \
    ln -sf /usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin/cpack /usr/local/bin/cpack


# ============================================================================
# Layer 7: Rust toolchain
# ============================================================================
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --verbose --profile minimal --component clippy,rustfmt --default-toolchain ${RUST_VERSION}

ENV PATH=/root/.cargo/bin:${PATH}

# Copy Cargo configuration from installers directory
COPY ./installers/.cargo/config.toml /root/.cargo/config.toml

# ============================================================================
# Layer 8: Python environment
# manylinux2014 ships Python 3.8-3.14 pre-installed at /opt/python/cpXXX-cpXXX/
# This eliminates source compilation (~13 min) and the custom OpenSSL layer.
# Python 3.12 already uses OpenSSL 3.x (built-in to the manylinux image).
# ============================================================================
RUN PY_TAG=$(echo ${PYTHON_VERSION} | tr -d '.') && \
    PYTHON_PATH="/opt/python/cp${PY_TAG}-cp${PY_TAG}" && \
    test -d "${PYTHON_PATH}" || { echo "ERROR: Python ${PYTHON_VERSION} not found at ${PYTHON_PATH}"; exit 1; } && \
    ln -sf ${PYTHON_PATH}/bin/python3   /usr/local/bin/python3 && \
    ln -sf ${PYTHON_PATH}/bin/python3   /usr/local/bin/python && \
    ln -sf ${PYTHON_PATH}/bin/pip3      /usr/local/bin/pip3 && \
    ln -sf ${PYTHON_PATH}/bin/pip3      /usr/local/bin/pip && \
    ln -sf ${PYTHON_PATH}/bin/conan     /usr/local/bin/conan && \
    echo "Python ${PYTHON_VERSION} linked from ${PYTHON_PATH}"

# Configure pip mirror
RUN pip3 config set global.index-url ${PYPI_MIRROR} && \
    pip3 config set global.trusted-host ${PYPI_TRUSTED_HOST}

# Install uv (modern Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python packages (including conan for C++ dependency management)
RUN pip3 install --no-cache-dir \
    taospy==${TAOSPY_VERSION} \
    taos-ws-py==${TAOS_WS_PY_VERSION} \
    conan \
    maturin && \
    conan profile detect --force && \
    sed -i 's/compiler.cppstd=gnu14/compiler.cppstd=gnu17/' /root/.conan2/profiles/default

ENV PATH=/root/.cargo/bin:${PATH}


# ============================================================================
# Layer 9: .NET SDK (using official install script)
# ============================================================================
RUN wget --no-check-certificate https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh && \
    chmod +x /tmp/dotnet-install.sh && \
    /tmp/dotnet-install.sh --version ${DOTNET_VERSION} --install-dir /usr/local/dotnet && \
    rm /tmp/dotnet-install.sh

ENV DOTNET_ROOT=/usr/local/dotnet
ENV PATH=${DOTNET_ROOT}:${PATH}

# ============================================================================
# Layer 10: Node.js + Yarn
# ============================================================================
ARG NODE_VERSION=22.14.0
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
      wget -q https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-arm64.tar.xz -O /tmp/node.tar.xz; \
    else \
      wget -q https://unofficial-builds.nodejs.org/download/release/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64-glibc-217.tar.xz -O /tmp/node.tar.xz; \
    fi && \
    tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 && \
    rm -f /tmp/node.tar.xz && \
    npm install -g yarn pnpm

# ============================================================================
# Layer 12: Modern tools (mold + protoc + tini)
# ============================================================================
RUN . /etc/environment && \
    # Install tini
    wget -O /bin/tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TINI_ARCH} && \
    chmod +x /bin/tini && \
    # Install mold
    wget https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz && \
    tar -xzf mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz && \
    cp mold-${MOLD_VERSION}-${MOLD_ARCH}-linux/bin/mold /usr/bin/mold && \
    rm -rf mold-${MOLD_VERSION}-${MOLD_ARCH}-linux mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz && \
    # Install protoc
    wget https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip && \
    unzip -o protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip -d /usr/ && \
    chmod +x /usr/bin/protoc && \
    rm -f protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip

# ============================================================================
# Layer 13: Environment configuration and final setup
# ============================================================================
RUN echo "export PATH=/usr/local/go/bin:/root/go/bin:/usr/local/jdk/bin:/usr/local/apache-maven-${MAVEN_VERSION}/bin:/root/.cargo/bin:/usr/local/dotnet:${PATH}" >> /etc/profile && \
    echo "export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}" >> /etc/profile && \
    mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    mkdir -p /etc/taos && \
    rm -f /etc/localtime && \
    ln -s /usr/share/zoneinfo/${TIMEZONE} /etc/localtime && \
    echo "${TIMEZONE}" > /etc/timezone && \
    git config --global --add safe.directory /app

# Set working directory
WORKDIR /app

# Set entrypoint to tini
ENTRYPOINT ["/bin/tini", "--"]

# ============================================================================
# Layer 14: Final verification - all tool versions printed during build
# ============================================================================
RUN echo "============================================================" && \
    echo "  Build Environment Verification" && \
    echo "============================================================" && \
    echo "glibc  : $(ldd --version 2>&1 | head -1)" && \
    echo "gcc    : $(gcc --version | head -1)" && \
    echo "g++    : $(g++ --version | head -1)" && \
    echo "python : $(python3 --version)" && \
    echo "pip    : $(pip3 --version)" && \
    echo "ssl    : $(python3 -c 'import ssl; print(ssl.OPENSSL_VERSION)')" && \
    echo "go     : $(go version)" && \
    echo "java   : $(java -version 2>&1 | head -1)" && \
    echo "mvn    : $(mvn --version 2>&1 | head -1)" && \
    echo "cmake  : $(cmake --version | head -1)" && \
    echo "rustc  : $(rustc --version)" && \
    echo "cargo  : $(cargo --version)" && \
    echo "dotnet : $(dotnet --version 2>/dev/null || echo 'N/A')" && \
    echo "mold   : $(mold --version 2>&1 | head -1)" && \
    echo "protoc : $(protoc --version)" && \
    echo "tini   : $(tini --version 2>&1 | head -1)" && \
    echo "arch   : $(uname -m)" && \
    echo "============================================================" && \
    echo "  All verifications passed!" && \
    echo "============================================================"

