FROM centos:7 as base

# 1.0. Use tuna mirrors as base mirror
RUN sed -e 's|^mirrorlist=|#mirrorlist=|g' \
    -e 's|^#baseurl=http://mirror.centos.org/centos|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos|g' \
    -i.bak \
    /etc/yum.repos.d/CentOS-*.repo
# 1.1. Add endpoint repo for git-*
RUN yum install -y https://mirrors.tuna.tsinghua.edu.cn/ius/ius-release-el7.rpm epel-release
# https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm
# 1.2. Add SCLs repo
RUN yum install -y centos-release-scl

# 2. Install
RUN yum install -y \
  autoconf make automake cmake openssl-devel curl wget \
  git236-all \
  perl-IPC-Cmd libatomic \
  devtoolset-11-gcc devtoolset-11-gcc-c++ cmake3

RUN wget -c https://github.com/llvm/llvm-project/releases/download/llvmorg-14.0.6/llvm-project-14.0.6.src.tar.xz \
  && tar -xvf llvm-project-14.0.6.src.tar.xz \
	&& cd llvm-project-14.0.6.src \
	&& mkdir build \
	&& . /opt/rh/devtoolset-11/enable \
	&& cmake3 -S llvm -B build -G 'Unix Makefiles' \
      -DLLVM_ENABLE_PROJECTS='clang;lld' \
      -DCMAKE_BUILD_TYPE=Release \
  && cmake3 --build build -j20 \
	&& cmake3 --install build \
	&& cd .. \
	&& rm -rf llvm-project-14.0.6.src llvm-project-14.0.6.src.tar.xz

# Rust
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUST_VERSION=1.78.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --verbose --profile minimal --default-toolchain nightly
ENV PATH=/root/.cargo/bin:$PATH

ENV TINI_VERSION v0.19.0
RUN wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -O /tini
RUN chmod +x /tini

ENV MOLD_VERSION 2.4.0
RUN wget https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-x86_64-linux.tar.gz \
  && tar -xvf mold-${MOLD_VERSION}-x86_64-linux.tar.gz \
	&& cp mold-${MOLD_VERSION}-x86_64-linux/bin/mold /usr/bin/mold \
	&& rm -rf mold-${MOLD_VERSION}-x86_64-linux.tar.gz mold-${MOLD_VERSION}-x86_64-linux/

ENV SCCACHE_VERSION v0.7.6
RUN wget https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz \
  && tar -xvf sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz \
  && cp sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache /usr/bin/sccache \
  && rm -rf sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl/

ENV RUSTC_WRAPPER=/usr/bin/sccache

WORKDIR /app
RUN git config --global --add safe.directory /app
RUN ln -sf /opt/rh/devtoolset-11/enable /etc/profile.d/devtoolset-11.sh
RUN ln -sf /usr/bin/cmake3 /usr/bin/cmake

ADD docker/dev/entrypoint.sh /sbin/entrypoint.sh
ADD docker/dev/cargo.toml /root/.cargo/config

ENTRYPOINT ["/tini", "--", "/sbin/entrypoint.sh"]
CMD ["cargo", "build", "--release", "--all"]
