FROM centos:7

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
  devtoolset-11-gcc devtoolset-11-gcc-c++

RUN ln -s /opt/rh/devtoolset-11/enable /etc/profile.d/devtoolset-11.sh
WORKDIR /app
RUN git config --global --add safe.directory /app

# Rust
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUST_VERSION=1.75.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --verbose --profile minimal --default-toolchain nightly
ENV PATH=/root/.cargo/bin:$PATH

# Compile llvm/clang
# https://gist.github.com/liweitianux/90f900c9a4565d0135851d8a473bd9bc

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ADD docker/dev/entrypoint.sh /sbin/entrypoint.sh
ADD docker/dev/cargo.toml /root/.cargo/config

ENTRYPOINT ["/tini", "--", "/sbin/entrypoint.sh"]
CMD ["cargo", "build", "--release", "--all"]
